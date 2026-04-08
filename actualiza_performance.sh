#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  actualiza_performance.sh
#  Uso: bash actualiza_performance.sh
#  Actualiza todos los datos del STR Dashboard y publica en GitHub
# ══════════════════════════════════════════════════════════════

set -e  # para si falla algo
FETCH_ERRORS=0

DIR="/Users/choloynoriega/Documents/Kavak Claude V1"
HTML_SRC="/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html"
HTML_DEST="$DIR/kavak_str_dashboard_v2.html"
LOG="/tmp/actualiza_performance_$(date +%Y%m%d_%H%M).log"

cd "$DIR"

echo "════════════════════════════════════════"
echo "  Performance Sales MX — Actualizando"
echo "  $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════"
echo ""

run() {
  echo "▶ $1..."
  python3 "$DIR/$2" >> "$LOG" 2>&1 && echo "  ✅ Listo" || { echo "  ❌ Error en $2 — ver $LOG"; exit 1; }
}

# ── 1. Fetch datos (paralelo, dim_str al final para evitar cancelación) ──────
echo "── Fetching datos desde Redshift ──"
python3 "$DIR/fetch_backlog_summary.py"  >> "$LOG" 2>&1 & PID1=$!
python3 "$DIR/fetch_sla_drilldown.py"   >> "$LOG" 2>&1 & PID2=$!
python3 "$DIR/fetch_cohort_str.py"      >> "$LOG" 2>&1 & PID3=$!
python3 "$DIR/fetch_cohort_entrega.py"  >> "$LOG" 2>&1 & PID4=$!
python3 "$DIR/fetch_cancel_motivos.py"  >> "$LOG" 2>&1 & PID5=$!
python3 "$DIR/fetch_sla_cierre.py"      >> "$LOG" 2>&1 & PID6=$!
python3 "$DIR/fetch_str_tradein.py"     >> "$LOG" 2>&1 & PID7=$!
python3 "$DIR/fetch_sla_delivery.py"    >> "$LOG" 2>&1 & PID8=$!
python3 "$DIR/fetch_str_kpis.py"        >> "$LOG" 2>&1 & PID9=$!
python3 "$DIR/fetch_funnel_fin.py"      >> "$LOG" 2>&1 & PID10=$!

# NPS con timeout de 90s
( timeout 90 python3 "$DIR/fetch_nps.py" >> "$LOG" 2>&1 ) \
  || echo "  ⚠️  NPS: timeout/error — datos anteriores se mantienen" >> "$LOG"

# Esperar fetches paralelos — fallos individuales no matan el script
for PID in $PID1 $PID2 $PID3 $PID4 $PID5 $PID6 $PID7 $PID8 $PID9 $PID10; do
  wait $PID || { echo "  ⚠️  fetch $PID falló — ver $LOG" ; FETCH_ERRORS=$((FETCH_ERRORS+1)); }
done

# dim_str solo (query pesada de aging — corre sin competencia)
echo "▶ fetch_dim_str_v2 (aging query — corre solo)..."
python3 "$DIR/fetch_dim_str_v2.py" >> "$LOG" 2>&1 \
  && echo "  ✅ dim_str OK" \
  || { echo "  ⚠️  dim_str falló — ver $LOG"; FETCH_ERRORS=$((FETCH_ERRORS+1)); }

echo "  ✅ Fetches completados ($FETCH_ERRORS errores)"
echo ""

# ── 2. Inyectar datos al HTML (orden importa) ─────────────────────────────────
echo "── Inyectando datos al dashboard ──"
run "Backlog Summary + MTD + rawSLA" "inject_backlog_summary.py"
run "NPS + SLA Drilldown"            "inject_nps_and_drilldown.py"
run "Todos los demás datos (STR KPIs, cohorts, etc.)" "inject_all_data.py"
run "Funnel Financing"               "inject_funnel_fin.py"
echo ""

# ── 3. Publicar en GitHub ──────────────────────────────────────────────────────
echo "── Publicando en GitHub Pages ──"
cp "$HTML_SRC" "$HTML_DEST"
git add kavak_str_dashboard_v2.html
git commit -m "Performance update $(date '+%Y-%m-%d %H:%M')" --quiet
git push origin main --quiet
echo "  ✅ Publicado"
echo ""

echo "════════════════════════════════════════"
echo "  ✅ Dashboard actualizado y publicado"
echo "  🔗 https://eloynoriega.github.io/kavak-dashboard/kavak_str_dashboard_v2.html"
echo "  📋 Log: $LOG"
echo "════════════════════════════════════════"
