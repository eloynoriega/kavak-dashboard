"""
fetch_dim_str_v2.py
Fetches all 5 STR dimensions × 3 métodos (all / Financing / Cash payment)
Outputs closed weekly data + WTD / LWTD / WoW Δ / Δ
Output: /tmp/rawDimSTR_v2.json

STR = ventas / (ventas + cancelaciones)
ventas       = fecha_venta_declarada IS NOT NULL
cancelaciones= fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1
Cohort       = COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva)

WTD  = Lun semana actual → D-1 (ayer)
LWTD = Lun semana pasada → (D-1 − 7 días)  ← mismo weekday, misma cantidad de días
"""
import sys
sys.path.insert(0, '/Users/choloynoriega/Documents/Kavak Claude V1/felipevanososte/Documents/CLAUDIO/.claude/skills/kavak-analytics')
from query_runner import execute_query
import json
from collections import defaultdict
from datetime import date, timedelta

WEEKS_START  = '2026-02-16'
MONTHS_START = '2026-01-01'

# ─── Date logic ───────────────────────────────────────────────────────────────
today      = date.today()
d1         = today - timedelta(days=1)          # D-1 (ayer)

# WTD = lunes de la semana ACTUAL → D-1
# Usar today.weekday() (no d1.weekday()) para que el lunes dé WTD vacío:
#   Lunes: wtd_start = hoy → wtd_end = ayer (domingo) → wtd_end < wtd_start → 0 días
#   Martes: wtd_start = ayer (lunes) → wtd_end = ayer → 1 día
#   Miércoles+: funciona normal
wtd_start  = today - timedelta(days=today.weekday())   # Lunes de esta semana
wtd_end    = d1                                         # Ayer
lwtd_start = wtd_start - timedelta(days=7)             # Lunes semana pasada
lwtd_end   = wtd_end   - timedelta(days=7)             # Mismo weekday, semana pasada

# Si hoy es lunes, WTD = 0 días → columna vacía
wtd_empty  = wtd_end < wtd_start

WTD_KEY    = str(wtd_start)   # key del dict weekly (semana actual)
WTD_START  = str(wtd_start)
WTD_END    = str(wtd_end)
LWTD_START = str(lwtd_start)
LWTD_END   = str(lwtd_end)

if wtd_empty:
    print(f"WTD  : VACÍO (hoy es lunes — {today})")
    print(f"LWTD : VACÍO (espejo de WTD)")
else:
    n_days = (wtd_end - wtd_start).days + 1
    print(f"WTD  : {WTD_START} → {WTD_END}  ({n_days} días)")
    print(f"LWTD : {LWTD_START} → {LWTD_END}  ({n_days} días)")

# ─── Utils ────────────────────────────────────────────────────────────────────
def df_to_records(df):
    df = df.copy()
    for col in df.columns:
        if hasattr(df[col], 'dt'):
            df[col] = df[col].astype(str)
    return df.to_dict(orient='records')

def str_pct(v, c):
    d = v + c
    return round(v / d * 100, 1) if d else None

def delta_pp(a, b):
    if a is None or b is None:
        return None
    return round(a - b, 1)

def build_lwtd_agg(rows, dim_col):
    """
    Aggregates LWTD query rows (no semana col) →
    {('all'|'Financing'|'Cash payment', dim): [v, c]}
    """
    agg = defaultdict(lambda: [0, 0])
    for r in rows:
        dim = r.get(dim_col)
        if dim is None:
            continue
        method = r.get('metodo_de_pago') or 'all'
        v = int(r.get('ventas') or 0)
        c = int(r.get('cancelaciones') or 0)
        agg[(method, dim)][0] += v
        agg[(method, dim)][1] += c
        if method in ('Financing', 'Cash payment'):
            agg[('all', dim)][0] += v
            agg[('all', dim)][1] += c
    return agg

# ─── Pivot helpers ────────────────────────────────────────────────────────────
def pivot_plain(rows, dim_key, all_weeks, lwtd_agg, dims_override=None):
    """
    Standard pivot (no mix%). Returns (result_dict, closed_weeks).
    closed_weeks = all_weeks minus the current WTD week key.
    """
    agg = defaultdict(lambda: [0, 0])
    dims_seen = []
    for r in rows:
        dim = r.get(dim_key) or r.get('_dim')
        if dim is None:
            continue
        method = r.get('metodo_de_pago') or 'all'
        sem    = str(r['semana'])[:10]
        v      = int(r.get('ventas') or 0)
        c      = int(r.get('cancelaciones') or 0)
        agg[(method, dim, sem)][0] += v
        agg[(method, dim, sem)][1] += c
        if method in ('Financing', 'Cash payment'):
            agg[('all', dim, sem)][0] += v
            agg[('all', dim, sem)][1] += c
        if dim not in dims_seen:
            dims_seen.append(dim)

    if dims_override:
        dims_seen = dims_override

    closed_weeks = [w for w in all_weeks if w != WTD_KEY]

    result = {}
    for target in ['all', 'Financing', 'Cash payment']:
        method_rows = []
        for dim in dims_seen:
            row = {'dim': dim}
            # Closed weekly columns
            for wk in closed_weeks:
                v, c = agg[(target, dim, wk)]
                row[wk] = str_pct(v, c)
            # WoW Δ (last 2 closed weeks)
            row['wow_delta'] = None
            if len(closed_weeks) >= 2:
                row['wow_delta'] = delta_pp(row.get(closed_weeks[-1]),
                                            row.get(closed_weeks[-2]))
            # WTD
            v_w, c_w = agg[(target, dim, WTD_KEY)]
            row['wtd'] = str_pct(v_w, c_w)
            # LWTD
            v_l, c_l = lwtd_agg.get((target, dim), [0, 0])
            row['lwtd'] = str_pct(v_l, c_l)
            # Δ WTD − LWTD
            row['delta'] = delta_pp(row['wtd'], row['lwtd'])
            method_rows.append(row)
        result[target] = method_rows
    return result, closed_weeks


def pivot_with_mix(rows, dim_key, all_weeks, lwtd_agg):
    """
    Pivot with interleaved STR + Mix% rows.
    Returns (result_dict, closed_weeks).
    """
    agg = defaultdict(lambda: [0, 0])
    dims_seen = []
    for r in rows:
        dim = r.get(dim_key) or r.get('_dim')
        if dim is None:
            continue
        method = r.get('metodo_de_pago') or 'all'
        sem    = str(r['semana'])[:10]
        v      = int(r.get('ventas') or 0)
        c      = int(r.get('cancelaciones') or 0)
        agg[(method, dim, sem)][0] += v
        agg[(method, dim, sem)][1] += c
        if method in ('Financing', 'Cash payment'):
            agg[('all', dim, sem)][0] += v
            agg[('all', dim, sem)][1] += c
        if dim not in dims_seen:
            dims_seen.append(dim)

    closed_weeks = [w for w in all_weeks if w != WTD_KEY]

    result = {}
    for target in ['all', 'Financing', 'Cash payment']:
        method_rows = []
        for dim in dims_seen:
            # ── STR row ──────────────────────────────────────────────────
            str_row = {'dim': f'{dim} — STR'}
            for wk in closed_weeks:
                v, c = agg[(target, dim, wk)]
                str_row[wk] = str_pct(v, c)
            str_row['wow_delta'] = None
            if len(closed_weeks) >= 2:
                str_row['wow_delta'] = delta_pp(str_row.get(closed_weeks[-1]),
                                                str_row.get(closed_weeks[-2]))
            v_w, c_w = agg[(target, dim, WTD_KEY)]
            str_row['wtd']   = str_pct(v_w, c_w)
            v_l, c_l = lwtd_agg.get((target, dim), [0, 0])
            str_row['lwtd']  = str_pct(v_l, c_l)
            str_row['delta'] = delta_pp(str_row['wtd'], str_row['lwtd'])
            method_rows.append(str_row)

            # ── Mix% row ─────────────────────────────────────────────────
            mix_row = {'dim': f'{dim} — Mix%', 'wow_delta': None, 'delta': None}
            for wk in closed_weeks:
                v_d, c_d = agg[(target, dim, wk)]
                total = sum(agg[(target, d2, wk)][0] + agg[(target, d2, wk)][1]
                            for d2 in dims_seen)
                mix_row[wk] = round((v_d + c_d) / total * 100, 1) if total else None
            # WTD mix%
            v_w2, c_w2 = agg[(target, dim, WTD_KEY)]
            total_wtd = sum(agg[(target, d2, WTD_KEY)][0] + agg[(target, d2, WTD_KEY)][1]
                            for d2 in dims_seen)
            mix_row['wtd'] = round((v_w2 + c_w2) / total_wtd * 100, 1) if total_wtd else None
            # LWTD mix%
            v_l2, c_l2 = lwtd_agg.get((target, dim), [0, 0])
            total_lwtd = sum(lwtd_agg.get((target, d2), [0, 0])[0] +
                             lwtd_agg.get((target, d2), [0, 0])[1]
                             for d2 in dims_seen)
            mix_row['lwtd'] = round((v_l2 + c_l2) / total_lwtd * 100, 1) if total_lwtd else None
            method_rows.append(mix_row)
        result[target] = method_rows
    return result, closed_weeks


# ─── Monthly pivot helpers ────────────────────────────────────────────────────
def pivot_monthly_with_mix(rows, dim_key, month_keys):
    """
    Same as pivot_with_mix but grouped by month (YYYY-MM) instead of week.
    MoM Δ = last two month_keys comparison.
    WTD / LWTD / delta are set to None (not applicable for monthly).
    """
    agg = defaultdict(lambda: [0, 0])
    dims_seen = []
    for r in rows:
        dim = r.get(dim_key) or r.get('_dim')
        if dim is None:
            continue
        method = r.get('metodo_de_pago') or 'all'
        mes    = str(r['semana'])[:7]   # 'YYYY-MM' from any date-like field
        v      = int(r.get('ventas') or 0)
        c      = int(r.get('cancelaciones') or 0)
        agg[(method, dim, mes)][0] += v
        agg[(method, dim, mes)][1] += c
        if method in ('Financing', 'Cash payment'):
            agg[('all', dim, mes)][0] += v
            agg[('all', dim, mes)][1] += c
        if dim not in dims_seen:
            dims_seen.append(dim)

    result = {}
    for target in ['all', 'Financing', 'Cash payment']:
        method_rows = []
        for dim in dims_seen:
            # ── STR row ──────────────────────────────────────────────────
            str_row = {'dim': f'{dim} — STR'}
            for mk in month_keys:
                v, c = agg[(target, dim, mk)]
                str_row[mk] = str_pct(v, c)
            # MoM Δ (last 2 months)
            str_row['wow_delta'] = None
            if len(month_keys) >= 2:
                str_row['wow_delta'] = delta_pp(str_row.get(month_keys[-1]),
                                                str_row.get(month_keys[-2]))
            str_row['wtd']   = None
            str_row['lwtd']  = None
            str_row['delta'] = None
            method_rows.append(str_row)

            # ── Mix% row ─────────────────────────────────────────────────
            mix_row = {'dim': f'{dim} — Mix%', 'wow_delta': None,
                       'wtd': None, 'lwtd': None, 'delta': None}
            for mk in month_keys:
                v_d, c_d = agg[(target, dim, mk)]
                total = sum(agg[(target, d2, mk)][0] + agg[(target, d2, mk)][1]
                            for d2 in dims_seen)
                mix_row[mk] = round((v_d + c_d) / total * 100, 1) if total else None
            method_rows.append(mix_row)
        result[target] = method_rows
    return result


def pivot_monthly_plain(rows, dim_key, month_keys, dims_override=None):
    """
    Same as pivot_plain but grouped by month (YYYY-MM).
    MoM Δ = last two month_keys comparison.
    WTD / LWTD / delta are set to None.
    """
    agg = defaultdict(lambda: [0, 0])
    dims_seen = []
    for r in rows:
        dim = r.get(dim_key) or r.get('_dim')
        if dim is None:
            continue
        method = r.get('metodo_de_pago') or 'all'
        mes    = str(r['semana'])[:7]
        v      = int(r.get('ventas') or 0)
        c      = int(r.get('cancelaciones') or 0)
        agg[(method, dim, mes)][0] += v
        agg[(method, dim, mes)][1] += c
        if method in ('Financing', 'Cash payment'):
            agg[('all', dim, mes)][0] += v
            agg[('all', dim, mes)][1] += c
        if dim not in dims_seen:
            dims_seen.append(dim)

    if dims_override:
        dims_seen = dims_override

    result = {}
    for target in ['all', 'Financing', 'Cash payment']:
        method_rows = []
        for dim in dims_seen:
            row = {'dim': dim}
            for mk in month_keys:
                v, c = agg[(target, dim, mk)]
                row[mk] = str_pct(v, c)
            # MoM Δ (last 2 months)
            row['wow_delta'] = None
            if len(month_keys) >= 2:
                row['wow_delta'] = delta_pp(row.get(month_keys[-1]),
                                            row.get(month_keys[-2]))
            row['wtd']   = None
            row['lwtd']  = None
            row['delta'] = None
            method_rows.append(row)
        result[target] = method_rows
    return result


# ─── 1. REGIÓN ────────────────────────────────────────────────────────────────
print("\nFetching REGIÓN...")
REGION_CASE = """
  CASE
    WHEN reservation_hub_name ILIKE 'CDMX%'  OR reservation_hub_name ILIKE 'EDOMEX%' THEN 'CDMX'
    WHEN reservation_hub_name ILIKE 'GDL%'   THEN 'GDL'
    WHEN reservation_hub_name ILIKE 'QRO%'   THEN 'QRO'
    WHEN reservation_hub_name ILIKE 'MTY%'   THEN 'MTY'
    WHEN reservation_hub_name ILIKE 'PUE%'   THEN 'PUE'
    WHEN reservation_hub_name ILIKE 'CUE%'   THEN 'CUE'
    ELSE NULL
  END
"""
df_region = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  {REGION_CASE} AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) >= '{WEEKS_START}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2, 3
HAVING region IS NOT NULL
ORDER BY 1, 2, 3
""")

df_mx = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  'MX' AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) >= '{WEEKS_START}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

# LWTD region
df_region_lwtd = execute_query(f"""
SELECT
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  {REGION_CASE} AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2
HAVING region IS NOT NULL
""")

df_mx_lwtd = execute_query(f"""
SELECT
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  'MX' AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2
""")
print(f"  → region={len(df_region)} mx={len(df_mx)} lwtd_region={len(df_region_lwtd)} lwtd_mx={len(df_mx_lwtd)}")


# ─── 2. AGING ─────────────────────────────────────────────────────────────────
print("Fetching AGING...")
AGING_CTE = """
WITH first_inv AS (
  SELECT bk_stock, MIN(item_receipt_date) AS item_receipt_date
  FROM prd_datamx_serving.serving.catalog_inventory_velocity
  WHERE item_receipt_date IS NOT NULL
  GROUP BY bk_stock
)
"""
AGING_CASE = """
  CASE
    WHEN DATEDIFF(day, fi.item_receipt_date, b.fecha_reserva) < 30  THEN '0-30d'
    WHEN DATEDIFF(day, fi.item_receipt_date, b.fecha_reserva) < 60  THEN '30-60d'
    WHEN DATEDIFF(day, fi.item_receipt_date, b.fecha_reserva) < 90  THEN '60-90d'
    ELSE '90+d'
  END
"""
df_aging = execute_query(f"""
{AGING_CTE},
aged AS (
  SELECT
    DATE_TRUNC('week', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {AGING_CASE} AS aging_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN first_inv fi ON CAST(b.stock AS BIGINT) = fi.bk_stock
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND fi.item_receipt_date IS NOT NULL
)
SELECT
  semana, metodo_de_pago, aging_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM aged
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE aging_bucket WHEN '0-30d' THEN 1 WHEN '30-60d' THEN 2 WHEN '60-90d' THEN 3 ELSE 4 END
""")

df_aging_lwtd = execute_query(f"""
{AGING_CTE}
SELECT
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  {AGING_CASE} AS aging_bucket,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN first_inv fi ON CAST(b.stock AS BIGINT) = fi.bk_stock
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
  AND fi.item_receipt_date IS NOT NULL
GROUP BY 1, 2
""")
print(f"  → aging={len(df_aging)} lwtd_aging={len(df_aging_lwtd)}")


# ─── 3. PRECIO ────────────────────────────────────────────────────────────────
print("Fetching PRECIO...")
PRECIO_CASE = """
  CASE
    WHEN h.real_published_price < 250000 THEN '0-250K'
    WHEN h.real_published_price < 350000 THEN '250-350K'
    WHEN h.real_published_price < 500000 THEN '350-500K'
    ELSE '500K+'
  END
"""
df_precio = execute_query(f"""
WITH priced AS (
  SELECT
    DATE_TRUNC('week', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {PRECIO_CASE} AS precio_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
    ON CAST(b.stock AS BIGINT) = h.bk_stock
    AND h.inv_date = b.fecha_reserva
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND h.real_published_price IS NOT NULL
)
SELECT
  semana, metodo_de_pago, precio_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM priced
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE precio_bucket WHEN '0-250K' THEN 1 WHEN '250-350K' THEN 2 WHEN '350-500K' THEN 3 ELSE 4 END
""")

df_precio_lwtd = execute_query(f"""
SELECT
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  {PRECIO_CASE} AS precio_bucket,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
  ON CAST(b.stock AS BIGINT) = h.bk_stock
  AND h.inv_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
  AND h.real_published_price IS NOT NULL
GROUP BY 1, 2
""")
print(f"  → precio={len(df_precio)} lwtd_precio={len(df_precio_lwtd)}")


# ─── 4. COMING SOON ───────────────────────────────────────────────────────────
print("Fetching COMING SOON...")
df_cs = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  CASE WHEN h.flag_coming_soon = 1 THEN 'Coming Soon' ELSE 'Sin Coming Soon' END AS cs_dim,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
  ON CAST(b.stock AS BIGINT) = h.bk_stock
  AND h.inv_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

df_cs_lwtd = execute_query(f"""
SELECT
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  CASE WHEN h.flag_coming_soon = 1 THEN 'Coming Soon' ELSE 'Sin Coming Soon' END AS cs_dim,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
  ON CAST(b.stock AS BIGINT) = h.bk_stock
  AND h.inv_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
GROUP BY 1, 2
""")
print(f"  → cs={len(df_cs)} lwtd_cs={len(df_cs_lwtd)}")


# ─── 5. CON/SIN PAGO ── PENDIENTE ─────────────────────────────────────────────
# TODO: Necesita acceso a tabla de pagos de reserva (aún no identificada en Databricks).
# La lógica correcta es: booking con primer pago >= 1,000 MXN → Con Pago, sino Sin Pago.
# fecha_confirmado IS NOT NULL da STR ~99% (incorrecto — captura llegada al hub, no pago).
# second_payments_aux es de compras (supply), no aplica.
print("CON/SIN PAGO → PENDIENTE (sin acceso a tabla de pagos), usando DataFrame vacío")
import pandas as _pd
_pago_cols = ['semana', 'metodo_de_pago', 'pago_dim', 'ventas', 'cancelaciones']
df_pago      = _pd.DataFrame(columns=_pago_cols)
df_pago_lwtd = _pd.DataFrame(columns=['metodo_de_pago', 'pago_dim', 'ventas', 'cancelaciones'])
print(f"  → pago=0 lwtd_pago=0 (pendiente)")


# ─── 6. PIX ───────────────────────────────────────────────────────────────────
print("Fetching PIX...")
PIX_CASE = """
  CASE
    WHEN CASE b.metodo_de_pago WHEN 'Financing' THEN h.pix_financing WHEN 'Financing Kavak' THEN h.pix_financing WHEN 'Cash payment' THEN h.pix_cash ELSE COALESCE(h.pix_financing, h.pix_cash) END < 0.88  THEN '< 0.88'
    WHEN CASE b.metodo_de_pago WHEN 'Financing' THEN h.pix_financing WHEN 'Financing Kavak' THEN h.pix_financing WHEN 'Cash payment' THEN h.pix_cash ELSE COALESCE(h.pix_financing, h.pix_cash) END < 0.93  THEN '0.88-0.93'
    WHEN CASE b.metodo_de_pago WHEN 'Financing' THEN h.pix_financing WHEN 'Financing Kavak' THEN h.pix_financing WHEN 'Cash payment' THEN h.pix_cash ELSE COALESCE(h.pix_financing, h.pix_cash) END < 0.98  THEN '0.93-0.98'
    ELSE '≥ 0.98'
  END
"""
PIX_ORDER = ['< 0.88', '0.88-0.93', '0.93-0.98', '≥ 0.98']

df_pix = execute_query(f"""
WITH pixed AS (
  SELECT
    DATE_TRUNC('week', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {PIX_CASE} AS pix_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
    ON CAST(b.stock AS BIGINT) = h.bk_stock
    AND h.inv_date = b.fecha_reserva
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND (h.pix_financing IS NOT NULL OR h.pix_cash IS NOT NULL)
)
SELECT
  semana, metodo_de_pago, pix_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM pixed
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE pix_bucket WHEN '< 0.88' THEN 1 WHEN '0.88-0.93' THEN 2 WHEN '0.93-0.98' THEN 3 ELSE 4 END
""")

df_pix_lwtd = execute_query(f"""
SELECT
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  {PIX_CASE} AS pix_bucket,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
  ON CAST(b.stock AS BIGINT) = h.bk_stock
  AND h.inv_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
  AND (h.pix_financing IS NOT NULL OR h.pix_cash IS NOT NULL)
GROUP BY 1, 2
""")
print(f"  → pix={len(df_pix)} lwtd_pix={len(df_pix_lwtd)}")


# ─── 7. REGIÓN DEL STOCK (hub del auto al momento de reserva) ────────────────
print("Fetching REGIÓN DEL STOCK...")
STOCK_REGION_VEL_CTE = """
WITH vel AS (
  SELECT stock_region, inv_date, bk_stock
  FROM prd_datamx_serving.serving.catalog_inventory_velocity
  WHERE stock_region IN ('CDMX','GUADALAJARA','MONTERREY','PUEBLA','QUERETARO','CUERNAVACA')
  GROUP BY 1, 2, 3
),
civ2 AS (
  SELECT stock_region, bk_stock
  FROM (
    SELECT stock_region, bk_stock,
           ROW_NUMBER() OVER(PARTITION BY bk_stock ORDER BY inv_date DESC) AS rn
    FROM vel
  ) WHERE rn = 1
)
"""
STOCK_REGION_CASE = """
  CASE
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'CDMX'        THEN 'CDMX'
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'GUADALAJARA' THEN 'GDL'
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'MONTERREY'   THEN 'MTY'
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'PUEBLA'      THEN 'PUE'
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'QUERETARO'   THEN 'QRO'
    WHEN COALESCE(c0.stock_region, c1.stock_region, c2.stock_region) = 'CUERNAVACA'  THEN 'CUE'
    ELSE NULL
  END
"""

df_stock_region = execute_query(f"""
{STOCK_REGION_VEL_CTE},
base AS (
  SELECT
    DATE_TRUNC('week', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {STOCK_REGION_CASE} AS sr,
    CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 ELSE 0 END AS venta,
    CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1 THEN 1 ELSE 0 END AS cancel
  FROM prd_datamx_serving.serving.bookings_history b
  LEFT JOIN vel c0 ON c0.bk_stock = CAST(b.stock AS BIGINT) AND c0.inv_date = b.fecha_reserva::date
  LEFT JOIN vel c1 ON c1.bk_stock = CAST(b.stock AS BIGINT) AND c1.inv_date = b.fecha_reserva::date - 1
  LEFT JOIN civ2 c2 ON c2.bk_stock = CAST(b.stock AS BIGINT)
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
)
SELECT semana, metodo_de_pago, sr AS stock_region,
       SUM(venta) AS ventas, SUM(cancel) AS cancelaciones
FROM base
WHERE sr IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

df_stock_region_lwtd = execute_query(f"""
{STOCK_REGION_VEL_CTE},
base AS (
  SELECT
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {STOCK_REGION_CASE} AS sr,
    CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 ELSE 0 END AS venta,
    CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1 THEN 1 ELSE 0 END AS cancel
  FROM prd_datamx_serving.serving.bookings_history b
  LEFT JOIN vel c0 ON c0.bk_stock = CAST(b.stock AS BIGINT) AND c0.inv_date = b.fecha_reserva::date
  LEFT JOIN vel c1 ON c1.bk_stock = CAST(b.stock AS BIGINT) AND c1.inv_date = b.fecha_reserva::date - 1
  LEFT JOIN civ2 c2 ON c2.bk_stock = CAST(b.stock AS BIGINT)
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
)
SELECT metodo_de_pago, sr AS stock_region,
       SUM(venta) AS ventas, SUM(cancel) AS cancelaciones
FROM base
WHERE sr IS NOT NULL
GROUP BY 1, 2
""")
print(f"  → stock_region={len(df_stock_region)} lwtd_stock_region={len(df_stock_region_lwtd)}")


# ─── Build weeks list ─────────────────────────────────────────────────────────
all_weeks = sorted(df_mx['semana'].astype(str).unique().tolist())
# Ensure WTD_KEY is present (if current week has data)
if WTD_KEY not in all_weeks:
    print(f"  ⚠️  WTD key {WTD_KEY} not in data – no activity yet this week")
    all_weeks.append(WTD_KEY)
closed_weeks = [w for w in all_weeks if w != WTD_KEY]
print(f"\nAll weeks : {all_weeks}")
print(f"Closed    : {closed_weeks}")
print(f"WTD key   : {WTD_KEY}")


# ─── Build LWTD agg dicts ─────────────────────────────────────────────────────
combined_region_lwtd = (
    [{'metodo_de_pago': r['metodo_de_pago'], 'region': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx_lwtd)]
    + df_to_records(df_region_lwtd)
)

lwtd_region  = build_lwtd_agg(combined_region_lwtd, 'region')
lwtd_aging   = build_lwtd_agg(df_to_records(df_aging_lwtd), 'aging_bucket')
lwtd_precio  = build_lwtd_agg(df_to_records(df_precio_lwtd), 'precio_bucket')
lwtd_cs      = build_lwtd_agg(df_to_records(df_cs_lwtd), 'cs_dim')
lwtd_pago    = build_lwtd_agg(df_to_records(df_pago_lwtd), 'pago_dim')
lwtd_pix          = build_lwtd_agg(df_to_records(df_pix_lwtd), 'pix_bucket')
lwtd_stock_region = build_lwtd_agg(
    [{'metodo_de_pago': r['metodo_de_pago'], 'stock_region': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx_lwtd)]
    + df_to_records(df_stock_region_lwtd),
    'stock_region'
)


# ─── Build pivots ─────────────────────────────────────────────────────────────
print("\nBuilding pivots...")

# Region: combine MX + cities
combined_region_weekly = (
    [{'semana': r['semana'], 'metodo_de_pago': r['metodo_de_pago'], '_dim': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx)]
    + [{**r, '_dim': r['region']} for r in df_to_records(df_region)]
)
region_pivot, cw = pivot_plain(combined_region_weekly, '_dim', all_weeks, lwtd_region,
                               dims_override=['MX','CDMX','CUE','GDL','MTY','PUE','QRO'])

aging_pivot,  _  = pivot_with_mix(df_to_records(df_aging),  'aging_bucket', all_weeks, lwtd_aging)
precio_pivot, _  = pivot_with_mix(df_to_records(df_precio), 'precio_bucket', all_weeks, lwtd_precio)
cs_pivot,     _  = pivot_with_mix(df_to_records(df_cs),  'cs_dim',       all_weeks, lwtd_cs)
pago_pivot,   _  = pivot_with_mix(df_to_records(df_pago),   'pago_dim',     all_weeks, lwtd_pago)
pix_pivot,    _  = pivot_with_mix(df_to_records(df_pix),    'pix_bucket',   all_weeks, lwtd_pix)

combined_stock_region_weekly = (
    [{'semana': r['semana'], 'metodo_de_pago': r['metodo_de_pago'], '_dim': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx)]
    + [{**r, '_dim': r['stock_region']} for r in df_to_records(df_stock_region)]
)
stock_region_pivot, _ = pivot_plain(combined_stock_region_weekly, '_dim', all_weeks, lwtd_stock_region,
                                    dims_override=['MX','CDMX','CUE','GDL','MTY','PUE','QRO'])


# ─── Month keys (last 4 calendar months up to current month) ──────────────────
month_keys = []
y, m = today.year, today.month
for _ in range(4):
    month_keys.append(f"{y}-{m:02d}")
    m -= 1
    if m == 0:
        m = 12
        y -= 1
month_keys.reverse()
print(f"\nMonth keys: {month_keys}")


# ─── Monthly queries ──────────────────────────────────────────────────────────
print("Fetching monthly REGIÓN...")
df_region_monthly = execute_query(f"""
SELECT
  DATE_TRUNC('month', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  {REGION_CASE} AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) >= '{MONTHS_START}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2, 3
HAVING region IS NOT NULL
ORDER BY 1, 2, 3
""")

df_mx_monthly = execute_query(f"""
SELECT
  DATE_TRUNC('month', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE metodo_de_pago END AS metodo_de_pago,
  'MX' AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) >= '{MONTHS_START}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")
print(f"  → region_monthly={len(df_region_monthly)} mx_monthly={len(df_mx_monthly)}")

print("Fetching monthly AGING...")
df_aging_monthly = execute_query(f"""
{AGING_CTE},
aged AS (
  SELECT
    DATE_TRUNC('month', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {AGING_CASE} AS aging_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN first_inv fi ON CAST(b.stock AS BIGINT) = fi.bk_stock
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{MONTHS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND fi.item_receipt_date IS NOT NULL
)
SELECT
  semana, metodo_de_pago, aging_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM aged
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE aging_bucket WHEN '0-30d' THEN 1 WHEN '30-60d' THEN 2 WHEN '60-90d' THEN 3 ELSE 4 END
""")
print(f"  → aging_monthly={len(df_aging_monthly)}")

print("Fetching monthly PRECIO...")
df_precio_monthly = execute_query(f"""
WITH priced AS (
  SELECT
    DATE_TRUNC('month', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {PRECIO_CASE} AS precio_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
    ON CAST(b.stock AS BIGINT) = h.bk_stock
    AND h.inv_date = b.fecha_reserva
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{MONTHS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND h.real_published_price IS NOT NULL
)
SELECT
  semana, metodo_de_pago, precio_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM priced
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE precio_bucket WHEN '0-250K' THEN 1 WHEN '250-350K' THEN 2 WHEN '350-500K' THEN 3 ELSE 4 END
""")
print(f"  → precio_monthly={len(df_precio_monthly)}")

print("Fetching monthly COMING SOON...")
df_cs_monthly = execute_query(f"""
SELECT
  DATE_TRUNC('month', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
  CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
  CASE WHEN h.flag_coming_soon = 1 THEN 'Coming Soon' ELSE 'Sin Coming Soon' END AS cs_dim,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM prd_datamx_serving.serving.bookings_history b
JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
  ON CAST(b.stock AS BIGINT) = h.bk_stock
  AND h.inv_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{MONTHS_START}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")
print(f"  → cs_monthly={len(df_cs_monthly)}")

print("monthly CON/SIN PAGO → PENDIENTE")
df_pago_monthly = _pd.DataFrame(columns=['semana', 'metodo_de_pago', 'pago_dim', 'ventas', 'cancelaciones'])

print("Fetching monthly PIX...")
df_pix_monthly = execute_query(f"""
WITH pixed AS (
  SELECT
    DATE_TRUNC('month', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {PIX_CASE} AS pix_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM prd_datamx_serving.serving.bookings_history b
  JOIN prd_datamx_serving.serving.catalog_inventory_velocity h
    ON CAST(b.stock AS BIGINT) = h.bk_stock
    AND h.inv_date = b.fecha_reserva
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{MONTHS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
    AND (h.pix_financing IS NOT NULL OR h.pix_cash IS NOT NULL)
)
SELECT
  semana, metodo_de_pago, pix_bucket,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)   AS cancelaciones
FROM pixed
GROUP BY 1, 2, 3
ORDER BY 1, 2,
  CASE pix_bucket WHEN '< 0.88' THEN 1 WHEN '0.88-0.93' THEN 2 WHEN '0.93-0.98' THEN 3 ELSE 4 END
""")
print(f"  → pix_monthly={len(df_pix_monthly)}")

print("Fetching monthly REGIÓN DEL STOCK...")
df_stock_region_monthly = execute_query(f"""
{STOCK_REGION_VEL_CTE},
base AS (
  SELECT
    DATE_TRUNC('month', COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva))::date AS semana,
    CASE WHEN b.metodo_de_pago = 'Financing Kavak' THEN 'Financing' ELSE b.metodo_de_pago END AS metodo_de_pago,
    {STOCK_REGION_CASE} AS sr,
    CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 ELSE 0 END AS venta,
    CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1 THEN 1 ELSE 0 END AS cancel
  FROM prd_datamx_serving.serving.bookings_history b
  LEFT JOIN vel c0 ON c0.bk_stock = CAST(b.stock AS BIGINT) AND c0.inv_date = b.fecha_reserva::date
  LEFT JOIN vel c1 ON c1.bk_stock = CAST(b.stock AS BIGINT) AND c1.inv_date = b.fecha_reserva::date - 1
  LEFT JOIN civ2 c2 ON c2.bk_stock = CAST(b.stock AS BIGINT)
  WHERE b.b2b = 0
    AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{MONTHS_START}'
    AND (b.fecha_venta_declarada IS NOT NULL
         OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
)
SELECT semana, metodo_de_pago, sr AS stock_region,
       SUM(venta) AS ventas, SUM(cancel) AS cancelaciones
FROM base
WHERE sr IS NOT NULL
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")
print(f"  → stock_region_monthly={len(df_stock_region_monthly)}")


# ─── Monthly pivots ───────────────────────────────────────────────────────────
print("\nBuilding monthly pivots...")

combined_region_monthly = (
    [{'semana': r['semana'], 'metodo_de_pago': r['metodo_de_pago'], '_dim': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx_monthly)]
    + [{**r, '_dim': r['region']} for r in df_to_records(df_region_monthly)]
)
monthly_region_pivot = pivot_monthly_plain(combined_region_monthly, '_dim', month_keys,
                                           dims_override=['MX','CDMX','CUE','GDL','MTY','PUE','QRO'])
monthly_aging_pivot  = pivot_monthly_with_mix(df_to_records(df_aging_monthly),  'aging_bucket', month_keys)
monthly_precio_pivot = pivot_monthly_with_mix(df_to_records(df_precio_monthly), 'precio_bucket', month_keys)
monthly_cs_pivot     = pivot_monthly_with_mix(df_to_records(df_cs_monthly),     'cs_dim',        month_keys)
monthly_pago_pivot   = pivot_monthly_with_mix(df_to_records(df_pago_monthly),   'pago_dim',      month_keys)
monthly_pix_pivot    = pivot_monthly_with_mix(df_to_records(df_pix_monthly),    'pix_bucket',    month_keys)

combined_stock_region_monthly = (
    [{'semana': r['semana'], 'metodo_de_pago': r['metodo_de_pago'], '_dim': 'MX',
      'ventas': r['ventas'], 'cancelaciones': r['cancelaciones']}
     for r in df_to_records(df_mx_monthly)]
    + [{**r, '_dim': r['stock_region']} for r in df_to_records(df_stock_region_monthly)]
)
monthly_stock_region_pivot = pivot_monthly_plain(combined_stock_region_monthly, '_dim', month_keys,
                                                 dims_override=['MX','CDMX','CUE','GDL','MTY','PUE','QRO'])


# ─── Output ───────────────────────────────────────────────────────────────────
output = {
    'weeks':       closed_weeks,
    'wtd_empty':   wtd_empty,
    'wtd_label':   '—' if wtd_empty else f"{wtd_start.day}/{wtd_start.month}–{wtd_end.day}/{wtd_end.month}",
    'lwtd_label':  '—' if wtd_empty else f"{lwtd_start.day}/{lwtd_start.month}–{lwtd_end.day}/{lwtd_end.month}",
    'region':      region_pivot,
    'aging':       aging_pivot,
    'precio':      precio_pivot,
    'cs_flag':     cs_pivot,
    'sin_pago':    pago_pivot,
    'pix':         pix_pivot,
    'months':               month_keys,
    'monthly_region':       monthly_region_pivot,
    'monthly_aging':        monthly_aging_pivot,
    'monthly_precio':       monthly_precio_pivot,
    'monthly_cs_flag':      monthly_cs_pivot,
    'monthly_sin_pago':     monthly_pago_pivot,
    'monthly_pix':          monthly_pix_pivot,
    'stock_region':         stock_region_pivot,
    'monthly_stock_region': monthly_stock_region_pivot,
}

out_path = '/tmp/rawDimSTR_v2.json'
with open(out_path, 'w') as f:
    json.dump(output, f, separators=(',', ':'), ensure_ascii=False)

size_kb = len(json.dumps(output)) / 1024
print(f"\nSaved to {out_path} ({size_kb:.1f} KB)")

# ─── QA ───────────────────────────────────────────────────────────────────────
print("\n=== QA: MX (all) ===")
mx_row = next(r for r in region_pivot['all'] if r['dim'] == 'MX')
print(f"  Closed weeks (last 3): {[(w[-5:], mx_row.get(w)) for w in closed_weeks[-3:]]}")
print(f"  WoW Δ: {mx_row['wow_delta']}pp")
print(f"  WTD:   {mx_row['wtd']}%   LWTD: {mx_row['lwtd']}%   Δ: {mx_row['delta']}pp")

print("\n=== QA: AGING 0-30d (all) ===")
aging_row = next((r for r in aging_pivot['all'] if '0-30d' in r['dim'] and 'STR' in r['dim']), None)
if aging_row:
    print(f"  WTD: {aging_row['wtd']}%  LWTD: {aging_row['lwtd']}%  Δ: {aging_row['delta']}pp")
