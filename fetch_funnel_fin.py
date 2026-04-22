"""
fetch_funnel_fin.py
Funnel de Financing: Reservas → Handoffs → Aprobadas → Ventas
Cohort: cada stage por su propia fecha de evento (non-cohorted, como STR por close date).

Filtros clave:
  - b2b = 0
  - metodo_de_pago = 'Financing'
  - estimate_flag = 1  (excluye reservas fantasma/prueba — igual que STR)
  - COUNT(DISTINCT booking_id)  (evita duplicados)

Denominador reservas: COUNT DISTINCT booking_id, estimate_flag=1, by fecha_reserva week
Numeradores: COUNT DISTINCT booking_id, estimate_flag=1, by event_date week

Output: /tmp/rawFunnelFin.json
"""
import sys
sys.path.insert(0, '/Users/choloynoriega/Documents/Kavak Claude V1/felipevanososte/Documents/CLAUDIO/.claude/skills/kavak-analytics')
from query_runner import execute_query
import json, pandas as pd
from collections import defaultdict
from datetime import date, timedelta

today        = date.today()
yesterday    = today - timedelta(days=1)
weekly_start = '2025-12-01'
mtd_start    = date(today.year, today.month, 1).isoformat()
lm           = today.month - 1 if today.month > 1 else 12
ly           = today.year if today.month > 1 else today.year - 1
lmtd_start   = date(ly, lm, 1).isoformat()
lmtd_end     = date(ly, lm, today.day).isoformat()

print(f"MTD : {mtd_start} -> {yesterday}")
print(f"LMTD: {lmtd_start} -> {lmtd_end}")

BASE = "b2b=0 AND metodo_de_pago IN ('Financing', 'Financing Kavak') AND estimate_flag=1"

def q_weekly(date_col, alias, d_start, d_end):
    return f"""
SELECT DATE_TRUNC('week', {date_col})::date AS semana,
       reservation_hub_name AS hub,
       COUNT(DISTINCT booking_id) AS {alias}_total,
       COUNT(DISTINCT CASE WHEN trade_in=0 THEN booking_id END) AS {alias}_sales,
       COUNT(DISTINCT CASE WHEN trade_in=1 THEN booking_id END) AS {alias}_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE} AND {date_col} BETWEEN '{d_start}' AND '{d_end}'
GROUP BY 1,2 ORDER BY 1,2"""

def q_period(date_col, alias, d_start, d_end):
    return f"""
SELECT reservation_hub_name AS hub,
       COUNT(DISTINCT booking_id) AS {alias}_total,
       COUNT(DISTINCT CASE WHEN trade_in=0 THEN booking_id END) AS {alias}_sales,
       COUNT(DISTINCT CASE WHEN trade_in=1 THEN booking_id END) AS {alias}_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE} AND {date_col} BETWEEN '{d_start}' AND '{d_end}'
GROUP BY 1 ORDER BY 1"""

# ── 1. Weekly ─────────────────────────────────────────────────────────────────
# Fórmula correcta (non-cohorted, cada etapa por su propia fecha de evento):
#   CR Res→HO  = HO / (HO + cancel_sin_HO)
#   CR Res→App = App / (App + cancel_sin_App)
# Denominador = eventos de esa etapa + los que cancelaron SIN llegar a esa etapa
# (cancel contado por fecha_cancelacion_reserva, no por fecha_reserva)
print("\nQuerying weekly funnel...")

def q_cancel_no_stage(stage_col, alias, d_start, d_end):
    """Cancels grouped by fecha_cancelacion that never reached the given stage."""
    return f"""
SELECT DATE_TRUNC('week', fecha_cancelacion_reserva)::date AS semana,
       reservation_hub_name AS hub,
       COUNT(DISTINCT booking_id) AS {alias}_total,
       COUNT(DISTINCT CASE WHEN trade_in=0 THEN booking_id END) AS {alias}_sales,
       COUNT(DISTINCT CASE WHEN trade_in=1 THEN booking_id END) AS {alias}_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE}
  AND fecha_cancelacion_reserva BETWEEN '{d_start}' AND '{d_end}'
  AND {stage_col} IS NULL
GROUP BY 1,2 ORDER BY 1,2"""

stages_wtd = [
    ('ho',         'fecha_handoff'),
    ('can_no_ho',  None),            # cancel sin HO → query especial
    ('app',        'fecha_aprobacion'),
    ('can_no_app', None),            # cancel sin App → query especial
]

merged = None
for alias, col in stages_wtd:
    if col is not None:
        df = execute_query(q_weekly(col, alias, weekly_start, str(yesterday)))
    elif alias == 'can_no_ho':
        df = execute_query(q_cancel_no_stage('fecha_handoff',   alias, weekly_start, str(yesterday)))
    else:  # can_no_app
        df = execute_query(q_cancel_no_stage('fecha_aprobacion', alias, weekly_start, str(yesterday)))
    df['semana'] = df['semana'].astype(str)
    print(f"  {alias}: {len(df)} rows")
    cols = ['semana','hub',f'{alias}_total',f'{alias}_sales',f'{alias}_ti']
    if merged is None:
        merged = df[cols]
    else:
        merged = merged.merge(df[cols], on=['semana','hub'], how='outer').fillna(0)

for col in merged.columns:
    if col not in ['semana','hub']:
        merged[col] = merged[col].astype(int)
raw_weekly = merged.to_dict(orient='records')

# QA: verify WTD CR against reference (ref: CR_HO=83%, CR_App=78%)
wk_last = sorted(set(r['semana'] for r in raw_weekly))[-1]
mx = [r for r in raw_weekly if r['semana'] == wk_last]
ho = sum(r['ho_total'] for r in mx); cnh = sum(r['can_no_ho_total'] for r in mx)
ap = sum(r['app_total'] for r in mx); cna = sum(r['can_no_app_total'] for r in mx)
print(f"\n=== QA {wk_last} MX ===")
print(f"  HO={ho} can_no_ho={cnh} → CR_HO={ho/(ho+cnh)*100:.1f}%")
print(f"  App={ap} can_no_app={cna} → CR_App={ap/(ap+cna)*100:.1f}%")

# ── 1b. CR App→Venta (STR-style, close date, with credit approval) ────────────
# Formula: vta_app / (vta_app + cancel_app)
# Each stage by its own event date, filtered to fecha_aprobacion IS NOT NULL
print("Querying CR App→Venta (STR with aprobacion)...")
df_vta_str = execute_query(f"""
SELECT
  DATE_TRUNC('week', COALESCE(fecha_venta_declarada, fecha_cancelacion_reserva))::date AS semana,
  reservation_hub_name AS hub,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        THEN booking_id END) AS vta_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS vta_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS vta_app_ti,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        THEN booking_id END) AS cancel_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS cancel_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS cancel_app_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE}
  AND (fecha_venta_declarada BETWEEN '{weekly_start}' AND '{yesterday}'
       OR fecha_cancelacion_reserva BETWEEN '{weekly_start}' AND '{yesterday}')
GROUP BY 1, 2
ORDER BY 1, 2
""")
df_vta_str['semana'] = df_vta_str['semana'].astype(str)
for col in df_vta_str.columns:
    if col not in ['semana','hub']: df_vta_str[col] = df_vta_str[col].astype(int)
raw_vta_str = df_vta_str.to_dict(orient='records')
print(f"  vta_str: {len(df_vta_str)} rows")

# ── 2. MTD ────────────────────────────────────────────────────────────────────
def q_cancel_no_stage_period(stage_col, alias, d_start, d_end):
    return f"""
SELECT reservation_hub_name AS hub,
       COUNT(DISTINCT booking_id) AS {alias}_total,
       COUNT(DISTINCT CASE WHEN trade_in=0 THEN booking_id END) AS {alias}_sales,
       COUNT(DISTINCT CASE WHEN trade_in=1 THEN booking_id END) AS {alias}_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE}
  AND fecha_cancelacion_reserva BETWEEN '{d_start}' AND '{d_end}'
  AND {stage_col} IS NULL
GROUP BY 1 ORDER BY 1"""

print("Querying MTD...")
merged_mtd = None
for alias, col in stages_wtd:
    if col is not None:
        df = execute_query(q_period(col, alias, mtd_start, str(yesterday)))
    elif alias == 'can_no_ho':
        df = execute_query(q_cancel_no_stage_period('fecha_handoff',    alias, mtd_start, str(yesterday)))
    else:
        df = execute_query(q_cancel_no_stage_period('fecha_aprobacion', alias, mtd_start, str(yesterday)))
    df['hub'] = df['hub'].astype(str)
    cols_p = ['hub', f'{alias}_total', f'{alias}_sales', f'{alias}_ti']
    if merged_mtd is None:
        merged_mtd = df[cols_p]
    else:
        merged_mtd = merged_mtd.merge(df[cols_p], on='hub', how='outer').fillna(0)
# MTD vta_str (STR with aprobacion)
df_mtd_vta = execute_query(f"""
SELECT reservation_hub_name AS hub,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        THEN booking_id END) AS vta_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS vta_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS vta_app_ti,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        THEN booking_id END) AS cancel_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS cancel_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS cancel_app_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE}
  AND (fecha_venta_declarada BETWEEN '{mtd_start}' AND '{yesterday}'
       OR fecha_cancelacion_reserva BETWEEN '{mtd_start}' AND '{yesterday}')
GROUP BY 1""")
df_mtd_vta['hub'] = df_mtd_vta['hub'].astype(str)
merged_mtd = merged_mtd.merge(df_mtd_vta, on='hub', how='outer').fillna(0)
for col in merged_mtd.columns:
    if col != 'hub': merged_mtd[col] = merged_mtd[col].astype(int)
raw_mtd = merged_mtd.to_dict(orient='records')

# ── 3. LMTD ───────────────────────────────────────────────────────────────────
print("Querying LMTD...")
merged_lmtd = None
for alias, col in stages_wtd:
    if col is not None:
        df = execute_query(q_period(col, alias, lmtd_start, lmtd_end))
    elif alias == 'can_no_ho':
        df = execute_query(q_cancel_no_stage_period('fecha_handoff',    alias, lmtd_start, lmtd_end))
    else:
        df = execute_query(q_cancel_no_stage_period('fecha_aprobacion', alias, lmtd_start, lmtd_end))
    df['hub'] = df['hub'].astype(str)
    cols_p = ['hub', f'{alias}_total', f'{alias}_sales', f'{alias}_ti']
    if merged_lmtd is None:
        merged_lmtd = df[cols_p]
    else:
        merged_lmtd = merged_lmtd.merge(df[cols_p], on='hub', how='outer').fillna(0)
df_lmtd_vta = execute_query(f"""
SELECT reservation_hub_name AS hub,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        THEN booking_id END) AS vta_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS vta_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_venta_declarada IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS vta_app_ti,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        THEN booking_id END) AS cancel_app_total,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=0 THEN booking_id END) AS cancel_app_sales,
  COUNT(DISTINCT CASE WHEN fecha_aprobacion IS NOT NULL AND fecha_cancelacion_reserva IS NOT NULL
        AND trade_in=1 THEN booking_id END) AS cancel_app_ti
FROM prd_datamx_serving.serving.bookings_history
WHERE {BASE}
  AND (fecha_venta_declarada BETWEEN '{lmtd_start}' AND '{lmtd_end}'
       OR fecha_cancelacion_reserva BETWEEN '{lmtd_start}' AND '{lmtd_end}')
GROUP BY 1""")
df_lmtd_vta['hub'] = df_lmtd_vta['hub'].astype(str)
merged_lmtd = merged_lmtd.merge(df_lmtd_vta, on='hub', how='outer').fillna(0)
for col in merged_lmtd.columns:
    if col != 'hub': merged_lmtd[col] = merged_lmtd[col].astype(int)
raw_lmtd = merged_lmtd.to_dict(orient='records')

# ── 4. Save ───────────────────────────────────────────────────────────────────
output = {
    'rawFunnelFin':     raw_weekly,
    'rawFunnelFinVta':  raw_vta_str,
    'rawFunnelFinMTD':  raw_mtd,
    'rawFunnelFinLMTD': raw_lmtd,
    'mtd_label':  f"{mtd_start[5:]} - {str(yesterday)[5:]}",
    'lmtd_label': f"{lmtd_start[5:]} - {lmtd_end[5:]}",
}
with open('/tmp/rawFunnelFin.json', 'w') as f:
    json.dump(output, f, separators=(',', ':'))
sz = len(json.dumps(output)) // 1024
print(f"\n✅ Saved /tmp/rawFunnelFin.json ({sz} KB)")

# ── QA ────────────────────────────────────────────────────────────────────────
ref = {
    '2026-02-16': (647, 582), '2026-02-23': (583, 543),
    '2026-03-02': (651, 595), '2026-03-09': (655, 588),
    '2026-03-16': (658, 571), '2026-03-23': (665, 596),
    '2026-03-30': (726, 645),
}
weekly_agg = defaultdict(lambda: {k: 0 for k in ['ho_total','can_no_ho_total','app_total','can_no_app_total']})
for r in raw_weekly:
    wk = r['semana']
    for k in weekly_agg[wk]: weekly_agg[wk][k] += r.get(k, 0)

# CR App->Vta from raw_vta_str
vta_agg = defaultdict(lambda: {'v':0,'c':0})
for r in raw_vta_str:
    wk = r['semana']
    vta_agg[wk]['v'] += r['vta_app_total']
    vta_agg[wk]['c'] += r['cancel_app_total']

print(f"\n{'Semana':<12} {'HO':>5} {'cnh':>5}  CR_HO  {'App':>5} {'cna':>5}  CR_App  App->Vta")
print('-' * 75)
for wk in sorted(weekly_agg)[-7:]:
    d = weekly_agg[wk]
    ho=d['ho_total']; cnh=d['can_no_ho_total']
    ap=d['app_total']; cna=d['can_no_app_total']
    v = vta_agg[wk]['v']; c = vta_agg[wk]['c']
    cr_ho  = ho/(ho+cnh)*100  if (ho+cnh)  else 0
    cr_app = ap/(ap+cna)*100  if (ap+cna)  else 0
    cr_vta = v/(v+c)*100      if (v+c)     else 0
    print(f"{wk:<12} {ho:>5} {cnh:>5}  {cr_ho:>5.1f}%  {ap:>5} {cna:>5}  {cr_app:>6.1f}%  {cr_vta:>7.1f}%")
