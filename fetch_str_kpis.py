"""
fetch_str_kpis.py
Genera los 4 objetos JS que alimentan la fila de KPIs del STR Dashboard v2:

  rawE_w  → STR semanal por hub × semana, cohort CLOSE DATE
             (ventas_total/fin/cash, cancel_total/fin/cash)
             Últimas 8 semanas completas + WTD

  rawA_w  → Reservas/ventas/cancel por hub × semana, cohort BOOKING DATE
             (reservas_total/fin/cash, ventas_total, cancel_total)

  rawMTD  → MTD actual (día 1 del mes → ayer) por hub, cohort CLOSE DATE
             + reservas_total por booking date en el mismo periodo

  rawLMTD → LMTD (mismo período del mes anterior) por hub, misma lógica

Output: /tmp/rawSTRKPIs.json  con keys { rawE_w, rawA_w, rawMTD, rawLMTD, mtd_label }
"""
import sys
sys.path.insert(0, '/Users/choloynoriega/Documents/Kavak Claude V1/felipevanososte/Documents/CLAUDIO/.claude/skills/kavak-analytics')
from query_runner import execute_query
import json
from collections import defaultdict
from datetime import date, timedelta

today     = date.today()
yesterday = today - timedelta(days=1)

# ── MTD / LMTD date windows ──────────────────────────────────────────────────
mtd_start = today.replace(day=1)
mtd_end   = yesterday

# LMTD: mismo período del mes anterior
# Si estamos en día 5, LMTD = día 1 → día 4 del mes pasado
lmtd_year  = (mtd_start - timedelta(days=1)).year
lmtd_month = (mtd_start - timedelta(days=1)).month
lmtd_start = date(lmtd_year, lmtd_month, 1)
lmtd_end   = date(lmtd_year, lmtd_month, mtd_end.day)  # mismo día del mes

# Weekly start: desde Dic 2025 para tener historial suficiente en los charts
weekly_start = '2025-12-01'

MTD_LABEL  = f"{mtd_start.day}/{mtd_start.month}–{mtd_end.day}/{mtd_end.month}"
LMTD_LABEL = f"{lmtd_start.day}/{lmtd_start.month}–{lmtd_end.day}/{lmtd_end.month}"

print(f"MTD : {mtd_start} → {mtd_end} ({MTD_LABEL})")
print(f"LMTD: {lmtd_start} → {lmtd_end} ({LMTD_LABEL})")

# ── 1. rawE_w — STR semanal (close-date cohort) ──────────────────────────────
print("\nFetching rawE_w (STR semanal, close date)...")

df_e = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(bh.fecha_venta_declarada, bh.fecha_cancelacion_reserva))::date AS semana,
  bh.reservation_hub_name                                                                      AS hub,
  bh.metodo_de_pago,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL THEN 1 END)                             AS ventas_total,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
              AND bh.metodo_de_pago = 'Financing'    THEN 1 END)                               AS ventas_fin,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
              AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)                               AS ventas_cash,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1               THEN 1 END)                               AS cancel_total,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1
              AND bh.metodo_de_pago = 'Financing'    THEN 1 END)                               AS cancel_fin,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1
              AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)                               AS cancel_cash
FROM serving.bookings_history bh
WHERE bh.b2b = 0
  AND (
    bh.fecha_venta_declarada IS NOT NULL
    OR (bh.fecha_cancelacion_reserva IS NOT NULL AND bh.estimate_flag = 1)
  )
  AND COALESCE(bh.fecha_venta_declarada, bh.fecha_cancelacion_reserva) >= '{weekly_start}'
  AND COALESCE(bh.fecha_venta_declarada, bh.fecha_cancelacion_reserva) <= '{yesterday}'
  AND bh.metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
""")

print(f"rawE_w: {len(df_e)} rows")
df_e['semana'] = df_e['semana'].astype(str)

# Aggregate by semana×hub (sum across metodo_de_pago rows for totals already in query)
e_records = []
for _, r in df_e.iterrows():
    e_records.append({
        'semana':       str(r['semana'])[:10],
        'hub':          r['hub'],
        'ventas_total': int(r.get('ventas_total') or 0),
        'ventas_fin':   int(r.get('ventas_fin')   or 0),
        'ventas_cash':  int(r.get('ventas_cash')  or 0),
        'cancel_total': int(r.get('cancel_total') or 0),
        'cancel_fin':   int(r.get('cancel_fin')   or 0),
        'cancel_cash':  int(r.get('cancel_cash')  or 0),
    })

# De-dup: collapse metodo rows (query groups by metodo_de_pago but we already have totals)
# Actually the query doesn't group by metodo correctly for totals — need to re-agg
e_agg = defaultdict(lambda: dict(ventas_total=0, ventas_fin=0, ventas_cash=0,
                                  cancel_total=0, cancel_fin=0, cancel_cash=0))
for r in e_records:
    key = (r['semana'], r['hub'])
    e_agg[key]['ventas_total'] += r['ventas_total']
    e_agg[key]['ventas_fin']   += r['ventas_fin']
    e_agg[key]['ventas_cash']  += r['ventas_cash']
    e_agg[key]['cancel_total'] += r['cancel_total']
    e_agg[key]['cancel_fin']   += r['cancel_fin']
    e_agg[key]['cancel_cash']  += r['cancel_cash']

rawE_w = []
for (semana, hub), vals in sorted(e_agg.items()):
    rawE_w.append({'semana': semana, 'hub': hub, **vals})

# QA: last 3 weeks MX total
wks = sorted(set(r['semana'] for r in rawE_w))[-4:]
print("\n=== rawE_w últimas 3 semanas (MX) ===")
for wk in wks[-3:]:
    rows = [r for r in rawE_w if r['semana'] == wk]
    vt = sum(r['ventas_total'] for r in rows)
    ct = sum(r['cancel_total'] for r in rows)
    str_pct = vt/(vt+ct)*100 if (vt+ct) else 0
    print(f"  {wk}: ventas={vt} cancel={ct} STR={str_pct:.1f}%")

# ── 2. rawA_w — Reservas semanales (fecha_origen cohort) ──────────────────────
# Fórmula validada vs PDF Performance: COUNTUNIQUEIF(id, fecha_origen, b2b=0, estimate_flag=1)
# SIN filtro metodo_de_pago para reservas_netas (el PDF no lo aplica)
print("\nFetching rawA_w (reservas by fecha_origen)...")

df_a = execute_query(f"""
SELECT
  DATE_TRUNC('week', bh.fecha_origen)::date AS semana,
  bh.reservation_hub_name                    AS hub,
  COUNT(*)                                    AS reservas_total,
  COUNT(CASE WHEN bh.estimate_flag = 1 THEN 1 END)                                                    AS reservas_netas,
  COUNT(CASE WHEN bh.metodo_de_pago = 'Financing'    THEN 1 END)                                      AS reservas_fin,
  COUNT(CASE WHEN bh.metodo_de_pago = 'Cash payment' THEN 1 END)                                      AS reservas_cash,
  COUNT(CASE WHEN bh.estimate_flag = 1 AND bh.metodo_de_pago = 'Financing'    THEN 1 END)             AS reservas_netas_fin,
  COUNT(CASE WHEN bh.estimate_flag = 1 AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)             AS reservas_netas_cash,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
              AND bh.metodo_de_pago = 'Financing'    THEN 1 END)                                      AS ventas_fin,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
              AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)                                      AS ventas_cash,
  COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL THEN 1 END)                                    AS ventas_total,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1               THEN 1 END)                                      AS cancel_total,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1
              AND bh.metodo_de_pago = 'Financing'    THEN 1 END)                                      AS cancel_fin,
  COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
              AND bh.estimate_flag = 1
              AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)                                      AS cancel_cash
FROM serving.bookings_history bh
WHERE bh.b2b = 0
  AND bh.fecha_origen >= '{weekly_start}'
  AND bh.fecha_origen <= '{yesterday}'
GROUP BY 1, 2
ORDER BY 1, 2
""")

print(f"rawA_w: {len(df_a)} rows")
df_a['semana'] = df_a['semana'].astype(str)

rawA_w = []
for _, r in df_a.iterrows():
    rawA_w.append({
        'semana':        str(r['semana'])[:10],
        'hub':           r['hub'],
        'reservas_total':      int(r.get('reservas_total')      or 0),
        'reservas_netas':      int(r.get('reservas_netas')      or 0),
        'reservas_fin':        int(r.get('reservas_fin')        or 0),
        'reservas_cash':       int(r.get('reservas_cash')       or 0),
        'reservas_netas_fin':  int(r.get('reservas_netas_fin')  or 0),
        'reservas_netas_cash': int(r.get('reservas_netas_cash') or 0),
        'ventas_total':  int(r.get('ventas_total')   or 0),
        'ventas_fin':    int(r.get('ventas_fin')     or 0),
        'ventas_cash':   int(r.get('ventas_cash')    or 0),
        'cancel_total':  int(r.get('cancel_total')   or 0),
        'cancel_fin':    int(r.get('cancel_fin')     or 0),
        'cancel_cash':   int(r.get('cancel_cash')    or 0),
    })

# QA
wks_a = sorted(set(r['semana'] for r in rawA_w))[-3:]
print("\n=== rawA_w últimas 3 semanas (MX) ===")
for wk in wks_a:
    rows = [r for r in rawA_w if r['semana'] == wk]
    rt = sum(r['reservas_total'] for r in rows)
    vt = sum(r['ventas_total'] for r in rows)
    ct = sum(r['cancel_total'] for r in rows)
    print(f"  {wk}: reservas={rt}, ventas={vt}, cancel={ct}")

# ── 2b. rawB_w — Perfiles mix semanal (booking date cohort) ──────────────────
# Columna: bh.perfil → valores: X, A, B, C, Z_R, NULL (sin perfil)
print("\nFetching rawB_w (perfiles mix semanal)...")

df_b = execute_query(f"""
SELECT
  DATE_TRUNC('week', bh.fecha_origen)::date AS semana,
  bh.reservation_hub_name                    AS hub,
  COUNT(*)                                                                       AS reservas_total,
  COUNT(CASE WHEN bh.perfil IS NOT NULL AND bh.perfil NOT IN ('Revisar') THEN 1 END) AS n_perfilados,
  COUNT(CASE WHEN bh.perfil = 'X'   THEN 1 END)                                 AS n_x,
  COUNT(CASE WHEN bh.perfil = 'A'   THEN 1 END)                                 AS n_a,
  COUNT(CASE WHEN bh.perfil = 'B'   THEN 1 END)                                 AS n_b,
  COUNT(CASE WHEN bh.perfil = 'C'   THEN 1 END)                                 AS n_c,
  COUNT(CASE WHEN bh.perfil = 'Z_R' THEN 1 END)                                 AS n_zr,
  COUNT(CASE WHEN bh.perfil IS NULL OR bh.perfil = 'Revisar' THEN 1 END)        AS n_sin_perfil
FROM serving.bookings_history bh
WHERE bh.b2b = 0
  AND bh.estimate_flag = 1
  AND bh.fecha_origen >= '{weekly_start}'
  AND bh.fecha_origen <= '{yesterday}'
GROUP BY 1, 2
ORDER BY 1, 2
""")

print(f"rawB_w: {len(df_b)} rows")
df_b['semana'] = df_b['semana'].astype(str)

rawB_w = []
for _, r in df_b.iterrows():
    rawB_w.append({
        'semana':       str(r['semana'])[:10],
        'hub':          r['hub'],
        'reservas_total': int(r.get('reservas_total') or 0),
        'n_perfilados': int(r.get('n_perfilados') or 0),
        'n_x':          int(r.get('n_x')          or 0),
        'n_a':          int(r.get('n_a')           or 0),
        'n_b':          int(r.get('n_b')           or 0),
        'n_c':          int(r.get('n_c')           or 0),
        'n_zr':         int(r.get('n_zr')          or 0),
        'n_sin_perfil': int(r.get('n_sin_perfil')  or 0),
    })

wks_b = sorted(set(r['semana'] for r in rawB_w))[-3:]
print("\n=== rawB_w últimas 3 semanas (MX) ===")
for wk in wks_b:
    rows = [r for r in rawB_w if r['semana'] == wk]
    rt  = sum(r['reservas_total'] for r in rows)
    nx  = sum(r['n_x']           for r in rows)
    na  = sum(r['n_a']           for r in rows)
    sp  = sum(r['n_sin_perfil']  for r in rows)
    print(f"  {wk}: reservas={rt}  X={nx}  A={na}  sin_perfil={sp}")

# ── 3. Helper: build MTD/LMTD hub dict ───────────────────────────────────────
def build_period_dict(df, period_label):
    """
    df has columns: hub, metodo_de_pago, ventas, cancelaciones, reservas_fecha
    Returns dict: {hub: {ventas_fin, ventas_cash, ventas_total,
                          cancel_fin, cancel_cash, cancel_total,
                          reservas_fin, reservas_cash, reservas_total}}
    Also adds '__MX__' aggregate.
    """
    agg = defaultdict(lambda: dict(
        ventas_fin=0, ventas_cash=0, ventas_total=0,
        cancel_fin=0, cancel_cash=0, cancel_total=0,
        reservas_fin=0, reservas_cash=0, reservas_total=0,
        reservas_netas=0, reservas_netas_fin=0, reservas_netas_cash=0,
    ))
    for _, r in df.iterrows():
        hub = r['hub']
        m   = r.get('metodo_de_pago', '')
        vt  = int(r.get('ventas_total')        or 0)
        ct  = int(r.get('cancel_total')        or 0)
        rt  = int(r.get('reservas_total')      or 0)
        vf  = int(r.get('ventas_fin')          or 0)
        vc  = int(r.get('ventas_cash')         or 0)
        cf  = int(r.get('cancel_fin')          or 0)
        cc  = int(r.get('cancel_cash')         or 0)
        rf  = int(r.get('reservas_fin')        or 0)
        rc  = int(r.get('reservas_cash')       or 0)
        rn  = int(r.get('reservas_netas')      or 0)
        rnf = int(r.get('reservas_netas_fin')  or 0)
        rnc = int(r.get('reservas_netas_cash') or 0)

        for h in [hub, '__MX__']:
            agg[h]['ventas_total']       += vt
            agg[h]['ventas_fin']         += vf
            agg[h]['ventas_cash']        += vc
            agg[h]['cancel_total']       += ct
            agg[h]['cancel_fin']         += cf
            agg[h]['cancel_cash']        += cc
            agg[h]['reservas_total']     += rt
            agg[h]['reservas_fin']       += rf
            agg[h]['reservas_cash']      += rc
            agg[h]['reservas_netas']     += rn
            agg[h]['reservas_netas_fin'] += rnf
            agg[h]['reservas_netas_cash']+= rnc

    return dict(agg)

# ── 4. rawMTD & rawLMTD ───────────────────────────────────────────────────────
def fetch_period(start, end, label):
    print(f"\nFetching {label}: {start} → {end}...")
    df = execute_query(f"""
    SELECT
      bh.reservation_hub_name                                               AS hub,
      bh.metodo_de_pago,
      -- ventas/cancel by CLOSE DATE
      COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL THEN 1 END)      AS ventas_total,
      COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
                  AND bh.metodo_de_pago = 'Financing'    THEN 1 END)        AS ventas_fin,
      COUNT(CASE WHEN bh.fecha_venta_declarada IS NOT NULL
                  AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)        AS ventas_cash,
      COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
                  AND bh.estimate_flag = 1               THEN 1 END)        AS cancel_total,
      COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
                  AND bh.estimate_flag = 1
                  AND bh.metodo_de_pago = 'Financing'    THEN 1 END)        AS cancel_fin,
      COUNT(CASE WHEN bh.fecha_cancelacion_reserva IS NOT NULL
                  AND bh.estimate_flag = 1
                  AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)        AS cancel_cash,
      -- reservas by BOOKING DATE (para el KPI de reservas brutas)
      0                                                                      AS reservas_total,
      0                                                                      AS reservas_fin,
      0                                                                      AS reservas_cash,
      0                                                                      AS reservas_netas,
      0                                                                      AS reservas_netas_fin,
      0                                                                      AS reservas_netas_cash
    FROM serving.bookings_history bh
    WHERE bh.b2b = 0
      AND bh.metodo_de_pago IN ('Financing', 'Cash payment')
      AND (
        bh.fecha_venta_declarada IS NOT NULL
        OR (bh.fecha_cancelacion_reserva IS NOT NULL AND bh.estimate_flag = 1)
      )
      AND COALESCE(bh.fecha_venta_declarada, bh.fecha_cancelacion_reserva)
          BETWEEN '{start}' AND '{end}'
    GROUP BY 1, 2

    UNION ALL

    SELECT
      bh.reservation_hub_name                                               AS hub,
      bh.metodo_de_pago,
      0 AS ventas_total, 0 AS ventas_fin, 0 AS ventas_cash,
      0 AS cancel_total, 0 AS cancel_fin, 0 AS cancel_cash,
      -- reservas: fecha_origen cohort, SIN filtro metodo_de_pago (fórmula PDF validada)
      COUNT(*)                                                               AS reservas_total,
      COUNT(CASE WHEN bh.metodo_de_pago = 'Financing'    THEN 1 END)        AS reservas_fin,
      COUNT(CASE WHEN bh.metodo_de_pago = 'Cash payment' THEN 1 END)        AS reservas_cash,
      COUNT(CASE WHEN bh.estimate_flag = 1               THEN 1 END)        AS reservas_netas,
      COUNT(CASE WHEN bh.estimate_flag = 1
                  AND bh.metodo_de_pago = 'Financing'    THEN 1 END)        AS reservas_netas_fin,
      COUNT(CASE WHEN bh.estimate_flag = 1
                  AND bh.metodo_de_pago = 'Cash payment' THEN 1 END)        AS reservas_netas_cash
    FROM serving.bookings_history bh
    WHERE bh.b2b = 0
      AND bh.fecha_origen BETWEEN '{start}' AND '{end}'
    GROUP BY 1, 2
    """)
    print(f"  {label}: {len(df)} rows")
    return build_period_dict(df, label)

rawMTD  = fetch_period(mtd_start,  mtd_end,  'rawMTD')
rawLMTD = fetch_period(lmtd_start, lmtd_end, 'rawLMTD')

# ── 5. QA ─────────────────────────────────────────────────────────────────────
print("\n=== QA rawMTD __MX__ ===")
mx = rawMTD.get('__MX__', {})
vt, ct, rt, rn = mx.get('ventas_total',0), mx.get('cancel_total',0), mx.get('reservas_total',0), mx.get('reservas_netas',0)
print(f"  reservas_brutas={rt}, reservas_netas={rn}, ventas={vt}, cancel={ct}, STR={vt/(vt+ct)*100:.1f}%")

print("=== QA rawLMTD __MX__ ===")
mx_l = rawLMTD.get('__MX__', {})
vt_l, ct_l, rt_l, rn_l = mx_l.get('ventas_total',0), mx_l.get('cancel_total',0), mx_l.get('reservas_total',0), mx_l.get('reservas_netas',0)
print(f"  reservas_brutas={rt_l}, reservas_netas={rn_l}, ventas={vt_l}, cancel={ct_l}, STR={vt_l/(vt_l+ct_l)*100:.1f}%")

# ── 6. rawDaily — ventas + entregas por día (últimos 45 días) ────────────────
print("\nFetching rawDaily (últimos 45 días)...")
daily_start = str(today - timedelta(days=45))
lm_start    = str(lmtd_start)                          # 1 de mes anterior
lm_end      = str(mtd_start - timedelta(days=1))       # último día del mes anterior

df_dv = execute_query(f"""
SELECT
  fecha_venta_declarada::date                                              AS fecha,
  reservation_hub_name                                                     AS hub,
  COUNT(*)                                                                 AS ventas_total,
  COUNT(CASE WHEN metodo_de_pago = 'Financing'         THEN 1 END)        AS ventas_fin,
  COUNT(CASE WHEN metodo_de_pago = 'Cash payment'      THEN 1 END)        AS ventas_cash,
  COUNT(CASE WHEN financing_provider = 'Kavak Capital' THEN 1 END)        AS ventas_kuna
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_venta_declarada >= '{daily_start}'
  AND fecha_venta_declarada < '{today}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1, 2 ORDER BY 1, 2
""")

df_de = execute_query(f"""
SELECT
  fecha_entrega::date                                                      AS fecha,
  reservation_hub_name                                                     AS hub,
  COUNT(*)                                                                 AS entregas_brutas,
  COUNT(CASE WHEN metodo_de_pago = 'Financing'         THEN 1 END)        AS entregas_fin,
  COUNT(CASE WHEN metodo_de_pago = 'Cash payment'      THEN 1 END)        AS entregas_cash,
  COUNT(CASE WHEN financing_provider = 'Kavak Capital' THEN 1 END)        AS entregas_kuna
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_entrega >= '{daily_start}'
  AND fecha_entrega < '{today}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1, 2 ORDER BY 1, 2
""")

df_dd = execute_query(f"""
SELECT
  devolucion_date::date AS fecha,
  reservation_hub_name  AS hub,
  COUNT(*)              AS devoluciones
FROM serving.bookings_history
WHERE b2b = 0
  AND devolucion_date IS NOT NULL
  AND devolucion_date::date >= '{daily_start}'
  AND devolucion_date::date < '{today}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1, 2 ORDER BY 1, 2
""")
df_dc = execute_query(f"""
SELECT
  fecha_cancelacion_reserva::date                                          AS fecha,
  reservation_hub_name                                                     AS hub,
  COUNT(*)                                                                 AS cancel_total,
  COUNT(CASE WHEN metodo_de_pago = 'Financing'    THEN 1 END)             AS cancel_fin,
  COUNT(CASE WHEN metodo_de_pago = 'Cash payment' THEN 1 END)             AS cancel_cash
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_cancelacion_reserva IS NOT NULL
  AND estimate_flag = 1
  AND fecha_cancelacion_reserva::date >= '{daily_start}'
  AND fecha_cancelacion_reserva::date < '{today}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1, 2 ORDER BY 1, 2
""")

df_dr = execute_query(f"""
SELECT
  fecha_origen::date                                                       AS fecha,
  reservation_hub_name                                                     AS hub,
  COUNT(CASE WHEN estimate_flag = 1               THEN 1 END)             AS reservas_netas,
  COUNT(CASE WHEN estimate_flag = 1
              AND metodo_de_pago = 'Financing'    THEN 1 END)             AS reservas_netas_fin,
  COUNT(CASE WHEN estimate_flag = 1
              AND metodo_de_pago = 'Cash payment' THEN 1 END)             AS reservas_netas_cash
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_origen >= '{daily_start}'
  AND fecha_origen < '{today}'
GROUP BY 1, 2 ORDER BY 1, 2
""")
print(f"  rawDaily: dv={len(df_dv)} de={len(df_de)} dd={len(df_dd)} dc={len(df_dc)} dr={len(df_dr)}")

# ── 7. rawLastMonth — cierre real del mes anterior ────────────────────────────
print("Fetching rawLastMonth...")
df_lmv = execute_query(f"""
SELECT
  reservation_hub_name                                                     AS hub,
  COUNT(*)                                                                 AS ventas_total,
  COUNT(CASE WHEN metodo_de_pago = 'Financing'         THEN 1 END)        AS ventas_fin,
  COUNT(CASE WHEN metodo_de_pago = 'Cash payment'      THEN 1 END)        AS ventas_cash,
  COUNT(CASE WHEN financing_provider = 'Kavak Capital' THEN 1 END)        AS ventas_kuna
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_venta_declarada BETWEEN '{lm_start}' AND '{lm_end}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1
""")

df_lme = execute_query(f"""
SELECT
  reservation_hub_name                                                     AS hub,
  COUNT(*)                                                                 AS entregas_brutas,
  COUNT(CASE WHEN metodo_de_pago = 'Financing'         THEN 1 END)        AS entregas_fin,
  COUNT(CASE WHEN financing_provider = 'Kavak Capital' THEN 1 END)        AS entregas_kuna
FROM serving.bookings_history
WHERE b2b = 0
  AND fecha_entrega BETWEEN '{lm_start}' AND '{lm_end}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1
""")

df_lmd = execute_query(f"""
SELECT
  reservation_hub_name AS hub,
  COUNT(*)             AS devoluciones
FROM serving.bookings_history
WHERE b2b = 0
  AND devolucion_date IS NOT NULL
  AND devolucion_date::date BETWEEN '{lm_start}' AND '{lm_end}'
  AND metodo_de_pago IN ('Financing', 'Cash payment')
GROUP BY 1
""")
print(f"  rawLastMonth: lmv={len(df_lmv)} lme={len(df_lme)} lmd={len(df_lmd)}")

# ── Aggregate rawDaily ────────────────────────────────────────────────────────
_dv_d = defaultdict(lambda: dict(ventas_total=0, ventas_fin=0, ventas_cash=0, ventas_kuna=0))
_de_d = defaultdict(lambda: dict(entregas_brutas=0, entregas_fin=0, entregas_cash=0, entregas_kuna=0))
_dd_d = defaultdict(int)
_dc_d = defaultdict(lambda: dict(cancel_total=0, cancel_fin=0, cancel_cash=0))
_dr_d = defaultdict(lambda: dict(reservas_netas=0, reservas_netas_fin=0, reservas_netas_cash=0))

for _, r in df_dv.iterrows():
    k = (str(r['fecha']), str(r['hub'] or 'null'))
    for f2 in ('ventas_total', 'ventas_fin', 'ventas_cash', 'ventas_kuna'):
        _dv_d[k][f2] += int(r[f2] or 0)

for _, r in df_de.iterrows():
    k = (str(r['fecha']), str(r['hub'] or 'null'))
    for f2 in ('entregas_brutas', 'entregas_fin', 'entregas_cash', 'entregas_kuna'):
        _de_d[k][f2] += int(r[f2] or 0)

for _, r in df_dd.iterrows():
    k = (str(r['fecha']), str(r['hub'] or 'null'))
    _dd_d[k] += int(r['devoluciones'] or 0)

for _, r in df_dc.iterrows():
    k = (str(r['fecha']), str(r['hub'] or 'null'))
    for f2 in ('cancel_total', 'cancel_fin', 'cancel_cash'):
        _dc_d[k][f2] += int(r[f2] or 0)

for _, r in df_dr.iterrows():
    k = (str(r['fecha']), str(r['hub'] or 'null'))
    for f2 in ('reservas_netas', 'reservas_netas_fin', 'reservas_netas_cash'):
        _dr_d[k][f2] += int(r[f2] or 0)

# MX totals per date
_dv_mx_d = defaultdict(lambda: dict(ventas_total=0, ventas_fin=0, ventas_cash=0, ventas_kuna=0))
_de_mx_d = defaultdict(lambda: dict(entregas_brutas=0, entregas_fin=0, entregas_cash=0, entregas_kuna=0))
_dd_mx_d = defaultdict(int)
_dc_mx_d = defaultdict(lambda: dict(cancel_total=0, cancel_fin=0, cancel_cash=0))
_dr_mx_d = defaultdict(lambda: dict(reservas_netas=0, reservas_netas_fin=0, reservas_netas_cash=0))

for (fd, hd), v in _dv_d.items():
    for f2 in v: _dv_mx_d[fd][f2] += v[f2]
for (fd, hd), v in _de_d.items():
    for f2 in v: _de_mx_d[fd][f2] += v[f2]
for (fd, hd), cnt in _dd_d.items():
    _dd_mx_d[fd] += cnt
for (fd, hd), v in _dc_d.items():
    for f2 in v: _dc_mx_d[fd][f2] += v[f2]
for (fd, hd), v in _dr_d.items():
    for f2 in v: _dr_mx_d[fd][f2] += v[f2]

_hubs_d  = sorted(set(k[1] for k in list(_dv_d.keys()) + list(_de_d.keys())))
_dates_d = sorted(set(
    k[0] for k in list(_dv_d.keys()) + list(_de_d.keys()) +
    list(_dc_d.keys()) + list(_dr_d.keys())
))

rawDaily = []
for fecha in _dates_d:
    # __MX__
    v   = _dv_mx_d.get(fecha, {})
    e   = _de_mx_d.get(fecha, {})
    c   = _dc_mx_d.get(fecha, {})
    res = _dr_mx_d.get(fecha, {})
    dev = _dd_mx_d.get(fecha, 0)
    eb  = e.get('entregas_brutas', 0)
    rawDaily.append({'fecha': fecha, 'hub': '__MX__',
        'ventas_total': v.get('ventas_total', 0), 'ventas_fin': v.get('ventas_fin', 0),
        'ventas_cash':  v.get('ventas_cash',  0), 'ventas_kuna': v.get('ventas_kuna', 0),
        'cancel_total': c.get('cancel_total', 0), 'cancel_fin': c.get('cancel_fin', 0),
        'cancel_cash':  c.get('cancel_cash',  0),
        'reservas_netas': res.get('reservas_netas', 0),
        'reservas_netas_fin':  res.get('reservas_netas_fin',  0),
        'reservas_netas_cash': res.get('reservas_netas_cash', 0),
        'entregas_brutas': eb, 'entregas_netas': max(0, eb - dev),
        'entregas_fin': e.get('entregas_fin', 0), 'entregas_cash': e.get('entregas_cash', 0),
        'entregas_kuna': e.get('entregas_kuna', 0)})
    # Hub-level
    for hd in _hubs_d:
        kk  = (fecha, hd)
        v   = _dv_d.get(kk, {})
        e   = _de_d.get(kk, {})
        c   = _dc_d.get(kk, {})
        res = _dr_d.get(kk, {})
        dev2 = _dd_d.get(kk, 0)
        eb2  = e.get('entregas_brutas', 0)
        if not v and not e and not c and not res:
            continue
        rawDaily.append({'fecha': fecha, 'hub': hd,
            'ventas_total': v.get('ventas_total', 0), 'ventas_fin': v.get('ventas_fin', 0),
            'ventas_cash':  v.get('ventas_cash',  0), 'ventas_kuna': v.get('ventas_kuna', 0),
            'cancel_total': c.get('cancel_total', 0), 'cancel_fin': c.get('cancel_fin', 0),
            'cancel_cash':  c.get('cancel_cash',  0),
            'reservas_netas': res.get('reservas_netas', 0),
            'reservas_netas_fin':  res.get('reservas_netas_fin',  0),
            'reservas_netas_cash': res.get('reservas_netas_cash', 0),
            'entregas_brutas': eb2, 'entregas_netas': max(0, eb2 - dev2),
            'entregas_fin': e.get('entregas_fin', 0), 'entregas_cash': e.get('entregas_cash', 0),
            'entregas_kuna': e.get('entregas_kuna', 0)})

# ── Aggregate rawLastMonth ────────────────────────────────────────────────────
_lmv_h = defaultdict(lambda: dict(ventas_total=0, ventas_fin=0, ventas_cash=0, ventas_kuna=0))
_lme_h = defaultdict(lambda: dict(entregas_brutas=0, entregas_fin=0, entregas_kuna=0))
_lmd_h = defaultdict(int)

for _, r in df_lmv.iterrows():
    h = str(r['hub'] or 'null')
    for f2 in ('ventas_total', 'ventas_fin', 'ventas_cash', 'ventas_kuna'):
        v = int(r[f2] or 0)
        _lmv_h[h][f2]       += v
        _lmv_h['__MX__'][f2] += v

for _, r in df_lme.iterrows():
    h = str(r['hub'] or 'null')
    for f2 in ('entregas_brutas', 'entregas_fin', 'entregas_kuna'):
        v = int(r[f2] or 0)
        _lme_h[h][f2]       += v
        _lme_h['__MX__'][f2] += v

for _, r in df_lmd.iterrows():
    h = str(r['hub'] or 'null')
    v = int(r['devoluciones'] or 0)
    _lmd_h[h]       += v
    _lmd_h['__MX__'] += v

rawLastMonth = {}
for h in set(list(_lmv_h.keys()) + list(_lme_h.keys())):
    v  = _lmv_h.get(h, {})
    e  = _lme_h.get(h, {})
    dev = _lmd_h.get(h, 0)
    eb  = e.get('entregas_brutas', 0)
    rawLastMonth[h] = {
        'ventas_total': v.get('ventas_total', 0), 'ventas_fin': v.get('ventas_fin', 0),
        'ventas_cash':  v.get('ventas_cash',  0), 'ventas_kuna': v.get('ventas_kuna', 0),
        'entregas_brutas': eb, 'entregas_netas': max(0, eb - dev),
        'entregas_fin': e.get('entregas_fin', 0),
        'entregas_kuna': e.get('entregas_kuna', 0),
    }

mx_lm = rawLastMonth.get('__MX__', {})
print(f"  __MX__ last month: ventas={mx_lm.get('ventas_total')}  ent_netas={mx_lm.get('entregas_netas')}  kuna={mx_lm.get('entregas_kuna')}")

# ── 8. Save ───────────────────────────────────────────────────────────────────
output = {
    'rawE_w':       rawE_w,
    'rawA_w':       rawA_w,
    'rawB_w':       rawB_w,
    'rawMTD':       rawMTD,
    'rawLMTD':      rawLMTD,
    'rawDaily':     rawDaily,
    'rawLastMonth': rawLastMonth,
    'mtd_label':    MTD_LABEL,
    'lmtd_label':   LMTD_LABEL,
}

with open('/tmp/rawSTRKPIs.json', 'w') as f:
    json.dump(output, f, separators=(',', ':'), ensure_ascii=False)

total_kb = len(json.dumps(output)) // 1024
print(f"\n✅ Saved /tmp/rawSTRKPIs.json ({total_kb} KB)")
print(f"   rawE_w: {len(rawE_w)} rows | rawA_w: {len(rawA_w)} rows")
print(f"   rawMTD hubs: {len(rawMTD)} | rawLMTD hubs: {len(rawLMTD)}")
