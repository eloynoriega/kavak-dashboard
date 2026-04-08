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

WEEKS_START = '2026-02-16'

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
  metodo_de_pago,
  {REGION_CASE} AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
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
  metodo_de_pago,
  'MX' AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
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
  metodo_de_pago,
  {REGION_CASE} AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2
HAVING region IS NOT NULL
""")

df_mx_lwtd = execute_query(f"""
SELECT
  metodo_de_pago,
  'MX' AS region,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
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
  FROM serving.dl_catalog_inventory_velocity_s01
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
    b.metodo_de_pago,
    {AGING_CASE} AS aging_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM serving.bookings_history b
  JOIN first_inv fi ON CAST(b.stock AS BIGINT)::varchar = fi.bk_stock
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
  b.metodo_de_pago,
  {AGING_CASE} AS aging_bucket,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM serving.bookings_history b
JOIN first_inv fi ON CAST(b.stock AS BIGINT)::varchar = fi.bk_stock
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
    b.metodo_de_pago,
    {PRECIO_CASE} AS precio_bucket,
    b.fecha_venta_declarada,
    b.fecha_cancelacion_reserva,
    b.estimate_flag
  FROM serving.bookings_history b
  JOIN serving.mvp_retail_stock_history h
    ON CAST(b.stock AS BIGINT)::varchar = h.stock_id
    AND h.inventory_date = b.fecha_reserva
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
  b.metodo_de_pago,
  {PRECIO_CASE} AS precio_bucket,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM serving.bookings_history b
JOIN serving.mvp_retail_stock_history h
  ON CAST(b.stock AS BIGINT)::varchar = h.stock_id
  AND h.inventory_date = b.fecha_reserva
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
  b.metodo_de_pago,
  CASE WHEN h.flag_coming_soon = 1 THEN 'Coming Soon' ELSE 'Sin Coming Soon' END AS cs_dim,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM serving.bookings_history b
JOIN serving.mvp_retail_stock_history h
  ON CAST(b.stock AS BIGINT)::varchar = h.stock_id
  AND h.inventory_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) >= '{WEEKS_START}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

df_cs_lwtd = execute_query(f"""
SELECT
  b.metodo_de_pago,
  CASE WHEN h.flag_coming_soon = 1 THEN 'Coming Soon' ELSE 'Sin Coming Soon' END AS cs_dim,
  COUNT(CASE WHEN b.fecha_venta_declarada IS NOT NULL THEN 1 END)                           AS ventas,
  COUNT(CASE WHEN b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag=1 THEN 1 END) AS cancelaciones
FROM serving.bookings_history b
JOIN serving.mvp_retail_stock_history h
  ON CAST(b.stock AS BIGINT)::varchar = h.stock_id
  AND h.inventory_date = b.fecha_reserva
WHERE b.b2b = 0
  AND COALESCE(b.fecha_venta_declarada, b.fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (b.fecha_venta_declarada IS NOT NULL
       OR (b.fecha_cancelacion_reserva IS NOT NULL AND b.estimate_flag = 1))
GROUP BY 1, 2
""")
print(f"  → cs={len(df_cs)} lwtd_cs={len(df_cs_lwtd)}")


# ─── 5. CON/SIN PAGO ──────────────────────────────────────────────────────────
print("Fetching CON/SIN PAGO...")
df_pago = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  metodo_de_pago,
  CASE WHEN reserva_con_pago = 1 THEN 'Con Pago' ELSE 'Sin Pago' END AS pago_dim,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) >= '{WEEKS_START}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

df_pago_lwtd = execute_query(f"""
SELECT
  metodo_de_pago,
  CASE WHEN reserva_con_pago = 1 THEN 'Con Pago' ELSE 'Sin Pago' END AS pago_dim,
  COUNT(CASE WHEN fecha_venta_declarada IS NOT NULL THEN 1 END)                          AS ventas,
  COUNT(CASE WHEN fecha_cancelacion_reserva IS NOT NULL AND estimate_flag=1 THEN 1 END)  AS cancelaciones
FROM serving.bookings_history
WHERE b2b = 0
  AND COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva) BETWEEN '{LWTD_START}' AND '{LWTD_END}'
  AND (fecha_venta_declarada IS NOT NULL
       OR (fecha_cancelacion_reserva IS NOT NULL AND estimate_flag = 1))
GROUP BY 1, 2
""")
print(f"  → pago={len(df_pago)} lwtd_pago={len(df_pago_lwtd)}")


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
