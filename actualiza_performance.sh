#!/bin/bash
# ══════════════════════════════════════════════════════════════
#  actualiza_performance.sh
#  Uso: bash actualiza_performance.sh
#  Actualiza los 3 dashboards de Kavak MX y publica en GitHub:
#    1. STR Performance Dashboard  (kavak_str_dashboard_v2.html)
#    2. Pipeline & Backlog         (index.html)
#    3. Pipeline Supervisor/Cobro  (pipeline_supervisor_YYYY.html)
# ══════════════════════════════════════════════════════════════

FETCH_ERRORS=0

DIR="/Users/choloynoriega/Documents/Kavak Claude V1"
HTML_SRC="/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html"
HTML_DEST="$DIR/kavak_str_dashboard_v2.html"
LOG="/tmp/actualiza_performance_$(date +%Y%m%d_%H%M).log"

cd "$DIR"

echo "════════════════════════════════════════"
echo "  Kavak MX — Actualizando 3 dashboards"
echo "  $(date '+%Y-%m-%d %H:%M')"
echo "════════════════════════════════════════"
echo ""

run() {
  echo "▶ $1..."
  python3 "$DIR/$2" >> "$LOG" 2>&1 && echo "  ✅ Listo" || { echo "  ❌ Error en $2 — ver $LOG"; exit 1; }
}

run_safe() {
  # Como run pero NO mata el script si falla — para dashboards independientes
  echo "▶ $1..."
  python3 "$DIR/$2" >> "$LOG" 2>&1 && echo "  ✅ Listo" || echo "  ⚠️  $2 falló — ver $LOG (otros dashboards no afectados)"
}

# ── 1. Fetch datos (paralelo, dim_str al final para evitar cancelación) ──────
echo "── Fetching datos desde Redshift ──"
( timeout 300 python3 "$DIR/fetch_backlog_summary.py"  >> "$LOG" 2>&1 ) & PID1=$!
( timeout 300 python3 "$DIR/fetch_sla_drilldown.py"   >> "$LOG" 2>&1 ) & PID2=$!
( timeout 300 python3 "$DIR/fetch_cohort_str.py"      >> "$LOG" 2>&1 ) & PID3=$!
( timeout 300 python3 "$DIR/fetch_cohort_entrega.py"  >> "$LOG" 2>&1 ) & PID4=$!
( timeout 300 python3 "$DIR/fetch_cancel_motivos.py"  >> "$LOG" 2>&1 ) & PID5=$!
( timeout 300 python3 "$DIR/fetch_sla_cierre.py"      >> "$LOG" 2>&1 ) & PID6=$!
( timeout 300 python3 "$DIR/fetch_str_tradein.py"     >> "$LOG" 2>&1 ) & PID7=$!
( timeout 300 python3 "$DIR/fetch_sla_delivery.py"    >> "$LOG" 2>&1 ) & PID8=$!
( timeout 300 python3 "$DIR/fetch_str_kpis.py"        >> "$LOG" 2>&1 ) & PID9=$!
( timeout 300 python3 "$DIR/fetch_funnel_fin.py"             >> "$LOG" 2>&1 ) & PID10=$!
( timeout 300 python3 "$DIR/fetch_funnel_sla_dictamen.py"    >> "$LOG" 2>&1 ) & PID11=$!

# NPS con timeout de 180s — si falla, el JSON anterior se mantiene en /tmp
echo "▶ fetch_nps (corre en serie — query pesada)..."
( timeout 180 python3 "$DIR/fetch_nps.py" >> "$LOG" 2>&1 ) \
  && echo "  ✅ NPS OK" \
  || echo "  ⚠️  NPS: timeout/error — se usará rawNPS.json cacheado si existe"

# Esperar fetches paralelos — fallos individuales no matan el script
for PID in $PID1 $PID2 $PID3 $PID4 $PID5 $PID6 $PID7 $PID8 $PID9 $PID10 $PID11; do
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

# ── 3. Publicar STR Dashboard en GitHub ──────────────────────────────────────
echo "── [1/3] STR Performance Dashboard ──"
cp "$HTML_SRC" "$HTML_DEST"
git add kavak_str_dashboard_v2.html
git commit -m "STR Performance update $(date '+%Y-%m-%d %H:%M')" --quiet
git push origin main --quiet
echo "  ✅ STR Dashboard publicado"
echo ""


# ── 4. Pipeline & Backlog Dashboard (index.html) ──────────────────────────────
echo "── [2/3] Pipeline & Backlog Dashboard ──"
run_safe "Pipeline & Backlog (index.html)" "generate_pipeline_dashboard.py"
if git diff --quiet index.html 2>/dev/null; then
  echo "  ℹ️  Sin cambios en index.html"
else
  git add index.html >> "$LOG" 2>&1
  git commit -m "Pipeline Backlog update $(date '+%Y-%m-%d %H:%M')" --quiet >> "$LOG" 2>&1 \
    && git push origin main --quiet >> "$LOG" 2>&1 \
    && echo "  ✅ Pipeline & Backlog publicado" \
    || echo "  ⚠️  Error publicando Pipeline Backlog — ver $LOG"
fi
echo ""

# ── 5. Pipeline Supervisor / Cobro ────────────────────────────────────────────
echo "── [3/3] Pipeline Supervisor / Cobro ──"
run_safe "Pipeline Supervisor/Cobro" "pipeline_cobro_v2.py"
# pipeline_cobro_v2.py maneja su propio git push a cobro-main — no hay que hacer nada más
echo ""

# ── 6. Slack alert diagnóstico ────────────────────────────────────────────────
echo "── Slack alert ──"
python3 "$DIR/alert_slack.py" 2>&1 | tee -a "$LOG" | grep -E "✅|⚠|❌"

echo "════════════════════════════════════════"
echo "  ✅ 3 dashboards actualizados"
echo "  🔗 STR:      https://eloynoriega.github.io/kavak-dashboard/kavak_str_dashboard_v2.html"
echo "  🔗 Pipeline: https://eloynoriega.github.io/kavak-dashboard/"
echo "  📋 Log: $LOG"
echo "════════════════════════════════════════"
