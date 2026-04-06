"""
inject_funnel_fin.py
Inyecta la sección "Funnel Financing — Conversiones" en el dashboard STR v2.
Posición: dentro de la sección STR, antes del card de SLA de Cierre.

Sección incluye:
  - 3 KPI cards (CR Res→HO, CR Res→App, CR App→Venta) MTD vs LMTD
  - 3 line charts semanales (Total / Sales / TI) para cada conversión
"""
import sys, os, json, re
SRC  = '/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html'
DEST = SRC

html = open(SRC, encoding='utf-8').read()

# ── 1. Load rawFunnelFin data ─────────────────────────────────────────────────
data_path = '/tmp/rawFunnelFin.json'
if not os.path.exists(data_path):
    print("ERROR: /tmp/rawFunnelFin.json not found — run fetch_funnel_fin.py first")
    sys.exit(1)

with open(data_path) as f:
    funnel_data = json.load(f)

funnel_json     = json.dumps(funnel_data['rawFunnelFin'],     separators=(',',':'))
funnel_vta_json = json.dumps(funnel_data['rawFunnelFinVta'],  separators=(',',':'))
funnel_mtd_json = json.dumps(funnel_data['rawFunnelFinMTD'],  separators=(',',':'))
funnel_lmtd_json= json.dumps(funnel_data['rawFunnelFinLMTD'], separators=(',',':'))

# ── 2. Remove existing data + section if re-running ──────────────────────────
for var in ['rawFunnelFin', 'rawFunnelFinVta', 'rawFunnelFinMTD', 'rawFunnelFinLMTD']:
    marker = f'const {var} = '
    while marker in html:
        pos = html.find(marker)
        # bracket-match to find end
        val_start = pos + len(marker)
        ch = html[val_start]
        if ch in ('[', '{'):
            depth, in_str, i = 0, False, val_start
            while i < len(html):
                c = html[i]
                if c == '"' and (i == 0 or html[i-1] != '\\'): in_str = not in_str
                if not in_str:
                    if c in ('[','{'): depth += 1
                    elif c in (']','}'): depth -= 1
                    if depth == 0: break
                i += 1
            end = i + 1
        else:
            end = html.find(';', val_start)
        html = html[:pos] + html[end+1:].lstrip('\n')
        print(f"  ↩ Removed const {var}")

# Remove existing funnel HTML section
FUNNEL_MARKER_START = '<!-- FUNNEL FIN SECTION -->'
FUNNEL_MARKER_END   = '<!-- /FUNNEL FIN SECTION -->'
if FUNNEL_MARKER_START in html:
    s = html.find(FUNNEL_MARKER_START)
    e = html.find(FUNNEL_MARKER_END) + len(FUNNEL_MARKER_END)
    html = html[:s] + html[e:]
    print("  ↩ Removed existing funnel HTML section")

if 'renderFunnelFin' in html:
    s = html.find('// ── FUNNEL FIN JS')
    e_marker = html.find('// ── END FUNNEL FIN JS')
    if s != -1 and e_marker > s:
        # Skip to end of the end-marker LINE (including any trailing ─ chars)
        e = html.find('\n', e_marker) + 1
        html = html[:s] + html[e:]
        print("  ↩ Removed existing funnel JS")

# ── 3. Inject data vars before rawBacklogSummary ─────────────────────────────
anchor_data = 'const rawBacklogSummary'
idx_data = html.find(anchor_data)
if idx_data == -1:
    print("ERROR: rawBacklogSummary anchor not found"); sys.exit(1)

data_block = (
    f'const rawFunnelFin = {funnel_json};\n'
    f'const rawFunnelFinVta = {funnel_vta_json};\n'
    f'const rawFunnelFinMTD = {funnel_mtd_json};\n'
    f'const rawFunnelFinLMTD = {funnel_lmtd_json};\n'
)
html = html[:idx_data] + data_block + html[idx_data:]
print(f"✅ Injected rawFunnelFin ({len(funnel_json)//1024} KB) + FinVta + MTD + LMTD")

# ── 4. Inject HTML section (before SLA de Cierre card) ───────────────────────
SLA_CIERRE_ANCHOR = '<div class="card" style="padding:16px; grid-column: 1 / -1;">\n        <div class="card-title" style="margin-bottom:12px;">SLA de Cierre'
idx_sla = html.find(SLA_CIERRE_ANCHOR)
if idx_sla == -1:
    print("ERROR: SLA Cierre anchor not found"); sys.exit(1)

funnel_html = """<!-- FUNNEL FIN SECTION -->
      <div class="card" style="padding:16px; grid-column: 1 / -1;">
        <div style="display:flex; align-items:center; justify-content:space-between; margin-bottom:14px;">
          <div class="card-title" style="margin-bottom:0;">🏦 Funnel Financing — Conversiones</div>
          <span style="font-size:11px; color:var(--muted);" id="funnel-fin-mtd-label"></span>
        </div>

        <!-- KPI row -->
        <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px; margin-bottom:18px;">
          <div class="kpi-card" style="border-left-color:#1a3a5c;">
            <div class="kpi-label">CR Reserva → Handoff</div>
            <div class="kpi-value" id="kpi-funnel-ho-val">—</div>
            <div class="kpi-delta" id="kpi-funnel-ho-delta"></div>
            <div class="kpi-sub" id="kpi-funnel-ho-sub"></div>
          </div>
          <div class="kpi-card" style="border-left-color:#2980b9;">
            <div class="kpi-label">CR Reserva → Aprobada</div>
            <div class="kpi-value" id="kpi-funnel-app-val">—</div>
            <div class="kpi-delta" id="kpi-funnel-app-delta"></div>
            <div class="kpi-sub" id="kpi-funnel-app-sub"></div>
          </div>
          <div class="kpi-card" style="border-left-color:#27ae60;">
            <div class="kpi-label">CR Aprobada → Venta Fin</div>
            <div class="kpi-value" id="kpi-funnel-vta-val">—</div>
            <div class="kpi-delta" id="kpi-funnel-vta-delta"></div>
            <div class="kpi-sub" id="kpi-funnel-vta-sub"></div>
          </div>
        </div>

        <!-- 3 charts -->
        <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px;">
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:var(--muted); margin-bottom:6px;">CR Res → Handoff (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-ho"></canvas></div>
          </div>
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:var(--muted); margin-bottom:6px;">CR Res → Aprobada (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-app"></canvas></div>
          </div>
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:var(--muted); margin-bottom:6px;">CR Aprobada → Venta Fin (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-vta"></canvas></div>
          </div>
        </div>
        <div style="font-size:10px; color:var(--muted); margin-top:8px;">
          Financing · estimate_flag=1 · b2b=0 · cada etapa por su propia fecha de evento · Sin TI = Sales
        </div>
      </div>
<!-- /FUNNEL FIN SECTION -->
"""
html = html[:idx_sla] + funnel_html + html[idx_sla:]
print("✅ Injected funnel HTML section")

# ── 5. Inject JS renderFunnelFin() before BOOT ────────────────────────────────
js_funnel = """
// ── FUNNEL FIN JS ──────────────────────────────────────────────────────────
function renderFunnelFin() {
  const hub  = currentHub;
  const gran = currentGran;  // 'semanal' | 'mensual'

  // ── Utils ─────────────────────────────────────────────────────────────
  function crFn(n, d) { return d > 0 ? (n / d * 100) : null; }
  function fmtPct(v)  { return v != null ? v.toFixed(1) + '%' : '—'; }
  function deltaPP(curr, prev) {
    if (curr == null || prev == null) return '';
    const d = curr - prev;
    const cls = d >= 0 ? 'delta-up' : 'delta-down';
    const lbl = gran === 'mensual' ? 'vs mes ant.' : 'vs sem. ant.';
    return `<span class="${cls}">${d >= 0 ? '+' : ''}${d.toFixed(1)}pp ${lbl}</span>`;
  }

  // ── Funnel buckets (res / ho / app) — from rawFunnelFin ───────────────
  function emptyBucket() {
    return {res:0,res_s:0,res_t:0,ho:0,ho_s:0,ho_t:0,app:0,app_s:0,app_t:0};
  }
  function addRow(bucket, r) {
    bucket.res += r.res_total||0; bucket.res_s += r.res_sales||0; bucket.res_t += r.res_ti||0;
    bucket.ho  += r.ho_total||0;  bucket.ho_s  += r.ho_sales||0;  bucket.ho_t  += r.ho_ti||0;
    bucket.app += r.app_total||0; bucket.app_s += r.app_sales||0; bucket.app_t += r.app_ti||0;
  }

  // ── Vta buckets (App→Venta STR-style) — from rawFunnelFinVta / MTD / LMTD ──
  function emptyVtaBucket() {
    return {vta:0,vta_s:0,vta_t:0,can:0,can_s:0,can_t:0};
  }
  function addVtaRow(bucket, r) {
    bucket.vta   += r.vta_app_total||0;    bucket.vta_s += r.vta_app_sales||0;    bucket.vta_t += r.vta_app_ti||0;
    bucket.can   += r.cancel_app_total||0; bucket.can_s += r.cancel_app_sales||0; bucket.can_t += r.cancel_app_ti||0;
  }

  // ── Aggregate period rows by hub ──────────────────────────────────────
  function aggPeriod(rows, emptyFn, addFn) {
    const s = emptyFn();
    rows.forEach(r => { if (hub === '__MX__' || r.hub === hub) addFn(s, r); });
    return s;
  }

  // ── Aggregate weekly rows into time buckets (semana or YYYY-MM) ───────
  function aggByTime(rows, keyCol, emptyFn, addFn) {
    const map = {};
    rows.forEach(r => {
      if (hub !== '__MX__' && r.hub !== hub) return;
      const rawKey = r[keyCol];
      const key = gran === 'mensual' ? rawKey.slice(0,7) : rawKey;
      if (!map[key]) map[key] = emptyFn();
      addFn(map[key], r);
    });
    return map;
  }

  // ── KPI cards: última semana o mes completo vs anterior ──────────────
  // (non-cohorted: cada etapa por su propia fecha de evento)
  let cur, prv, curVta, prvVta, periodLabel;

  if (gran === 'mensual') {
    // Aggregate rawFunnelFin by YYYY-MM
    function aggByMonthKey(rows, emptyFn, addFn) {
      const map = {};
      rows.forEach(r => {
        if (hub !== '__MX__' && r.hub !== hub) return;
        const key = (r.semana || '').slice(0, 7);
        if (!key) return;
        if (!map[key]) map[key] = emptyFn();
        addFn(map[key], r);
      });
      return map;
    }
    const mMap  = aggByMonthKey(rawFunnelFin,    emptyBucket,    addRow);
    const mVMap = aggByMonthKey(rawFunnelFinVta, emptyVtaBucket, addVtaRow);
    const mKeys  = Object.keys(mMap).sort();
    const mVKeys = Object.keys(mVMap).sort();
    const lastM  = mKeys[mKeys.length - 1]  || '';
    const prevM  = mKeys[mKeys.length - 2]  || '';
    const lastMV = mVKeys[mVKeys.length - 1] || '';
    const prevMV = mVKeys[mVKeys.length - 2] || '';
    cur    = mMap[lastM]   || emptyBucket();
    prv    = mMap[prevM]   || emptyBucket();
    curVta = mVMap[lastMV] || emptyVtaBucket();
    prvVta = mVMap[prevMV] || emptyVtaBucket();
    periodLabel = lastM ? 'Mes: ' + lastM.slice(5) + '/' + lastM.slice(2, 4) : '—';
  } else {
    const allSemanas = [...new Set(rawFunnelFin.map(r => r.semana))].sort();
    const lastWk = allSemanas[allSemanas.length - 1];
    const prevWk = allSemanas[allSemanas.length - 2];
    cur = aggPeriod(rawFunnelFin.filter(r => r.semana === lastWk), emptyBucket, addRow);
    prv = aggPeriod(rawFunnelFin.filter(r => r.semana === prevWk), emptyBucket, addRow);
    const allSemVta = [...new Set(rawFunnelFinVta.map(r => r.semana))].sort();
    const lastWkV = allSemVta[allSemVta.length - 1];
    const prevWkV = allSemVta[allSemVta.length - 2];
    curVta = aggPeriod(rawFunnelFinVta.filter(r => r.semana === lastWkV), emptyVtaBucket, addVtaRow);
    prvVta = aggPeriod(rawFunnelFinVta.filter(r => r.semana === prevWkV), emptyVtaBucket, addVtaRow);
    periodLabel = lastWk ? 'Sem: ' + lastWk.slice(5).replace('-', '/') : '—';
  }

  const kpiList = [
    { id:'ho',
      cur: crFn(cur.ho,  cur.res),  prv: crFn(prv.ho,  prv.res),
      sub: `Sales: ${fmtPct(crFn(cur.ho_s, cur.res_s))} · TI: ${fmtPct(crFn(cur.ho_t, cur.res_t))}` },
    { id:'app',
      cur: crFn(cur.app, cur.res),  prv: crFn(prv.app, prv.res),
      sub: `Sales: ${fmtPct(crFn(cur.app_s, cur.res_s))} · TI: ${fmtPct(crFn(cur.app_t, cur.res_t))}` },
    { id:'vta',
      cur: crFn(curVta.vta, curVta.vta + curVta.can),
      prv: crFn(prvVta.vta, prvVta.vta + prvVta.can),
      sub: `Sales: ${fmtPct(crFn(curVta.vta_s, curVta.vta_s + curVta.can_s))} · TI: ${fmtPct(crFn(curVta.vta_t, curVta.vta_t + curVta.can_t))}` },
  ];
  kpiList.forEach(k => {
    const setEl = (id, fn) => { const el = document.getElementById(id); if (el) fn(el); };
    setEl('kpi-funnel-'+k.id+'-val',   el => el.textContent = fmtPct(k.cur));
    setEl('kpi-funnel-'+k.id+'-delta', el => el.innerHTML   = deltaPP(k.cur, k.prv));
    setEl('kpi-funnel-'+k.id+'-sub',   el => el.textContent = k.sub);
  });

  // Update period label
  const lbl = document.getElementById('funnel-fin-mtd-label');
  if (lbl) lbl.textContent = periodLabel;

  // ── Build time-series maps ────────────────────────────────────────────
  const timeMap = aggByTime(rawFunnelFin,    'semana', emptyBucket,    addRow);
  const vtaMap  = aggByTime(rawFunnelFinVta, 'semana', emptyVtaBucket, addVtaRow);

  // Merge + sort all week keys
  const allKeys = [...new Set([...Object.keys(timeMap), ...Object.keys(vtaMap)])].sort();
  const xlabels = gran === 'mensual'
    ? allKeys.map(k => { const [y,m] = k.split('-'); return m+'/'+y.slice(2); })
    : allKeys.map(k => k.slice(5).replace('-','/'));

  function series(map, fn) {
    return allKeys.map(k => {
      const m = map[k];
      if (!m) return null;
      const v = fn(m);
      return v != null ? +v.toFixed(2) : null;
    });
  }

  // ── Render 3 charts ───────────────────────────────────────────────────
  const chartDefs = [
    { chartKey:'funnel-ho', map: timeMap, sets:[
        {label:'Total',  fn: m => crFn(m.ho,   m.res),   col:'#1a3a5c'},
        {label:'Sales',  fn: m => crFn(m.ho_s, m.res_s), col:'#2980b9', dash:[4,3]},
        {label:'Con TI', fn: m => crFn(m.ho_t, m.res_t), col:'#e67e22', dash:[2,3]},
    ]},
    { chartKey:'funnel-app', map: timeMap, sets:[
        {label:'Total',  fn: m => crFn(m.app,   m.res),   col:'#1a3a5c'},
        {label:'Sales',  fn: m => crFn(m.app_s, m.res_s), col:'#2980b9', dash:[4,3]},
        {label:'Con TI', fn: m => crFn(m.app_t, m.res_t), col:'#e67e22', dash:[2,3]},
    ]},
    { chartKey:'funnel-vta', map: vtaMap, sets:[
        {label:'Total',  fn: m => crFn(m.vta,   m.vta   + m.can),   col:'#27ae60'},
        {label:'Sales',  fn: m => crFn(m.vta_s, m.vta_s + m.can_s), col:'#2ecc71', dash:[4,3]},
        {label:'Con TI', fn: m => crFn(m.vta_t, m.vta_t + m.can_t), col:'#e67e22', dash:[2,3]},
    ]},
  ];

  chartDefs.forEach(def => {
    destroyChart(def.chartKey);
    const canvas = document.getElementById('chart-' + def.chartKey);
    if (!canvas) return;
    charts[def.chartKey] = new Chart(canvas, {
      type: 'line',
      data: {
        labels: xlabels,
        datasets: def.sets.map(s => ({
          label: s.label,
          data: series(def.map, s.fn),
          borderColor: s.col,
          backgroundColor: s.col + '18',
          borderDash: s.dash || [],
          borderWidth: s.dash ? 1.5 : 2.5,
          pointRadius: 2, pointHoverRadius: 4,
          tension: 0.3, spanGaps: true, fill: false,
        }))
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { display: true, position: 'bottom',
            labels: { boxWidth: 10, font:{size:10}, padding:8 }},
          datalabels: { display: false },
          tooltip: { callbacks: {
            label: c => `${c.dataset.label}: ${c.parsed.y != null ? c.parsed.y.toFixed(1)+'%' : '—'}`
          }}
        },
        scales: {
          x: { ticks: { font:{size:9}, maxRotation:45 } },
          y: { min:50, max:100,
               ticks: { callback: v => v+'%', font:{size:9} },
               grid:  { color:'rgba(0,0,0,0.05)' } }
        }
      }
    });
  });
}
// ── END FUNNEL FIN JS ───────────────────────────────────────────────────────

"""
boot_anchor = '// ═══════════════════════════════════════════════════════════\n// BOOT'
idx_boot = html.find(boot_anchor)
if idx_boot == -1:
    print("ERROR: BOOT anchor not found"); sys.exit(1)
html = html[:idx_boot] + js_funnel + html[idx_boot:]
print("✅ Injected renderFunnelFin() JS function")

# ── 6. Wire into refreshAll and DOMContentLoaded ──────────────────────────────
def wire(html, anchor_old, anchor_new, label):
    if anchor_old in html and anchor_new not in html:
        html = html.replace(anchor_old, anchor_new, 1)
        print(f"✅ Wired renderFunnelFin into {label}")
    else:
        print(f"ℹ️  renderFunnelFin already in {label} — skipping")
    return html

html = wire(html,
    '  renderSLADeliveryChart();\n}',
    '  renderFunnelFin();\n  renderSLADeliveryChart();\n}',
    'refreshAll()')

# Remove any stale duplicate renderNPSSection from DOMContentLoaded before wiring
if '  renderNPSSection();\n  renderFunnelFin();\n  renderNPSSection();\n});' in html:
    html = html.replace(
        '  renderNPSSection();\n  renderFunnelFin();\n  renderNPSSection();\n});',
        '  renderFunnelFin();\n  renderNPSSection();\n});', 1)
    print("  ↩ Deduped double renderNPSSection in DOMContentLoaded")

html = wire(html,
    '  renderNPSSection();\n});',
    '  renderFunnelFin();\n  renderNPSSection();\n});',
    'DOMContentLoaded')


# ── 7. Write ──────────────────────────────────────────────────────────────────
with open(DEST, 'w', encoding='utf-8') as f:
    f.write(html)

size_kb = len(html) // 1024
print(f"\n✅ Done → {DEST} ({size_kb} KB)")

# Sanity
html2 = open(DEST).read()
checks = ['rawFunnelFin', 'rawFunnelFinVta', 'rawFunnelFinMTD', 'rawFunnelFinLMTD',
          'chart-funnel-ho', 'chart-funnel-app', 'chart-funnel-vta',
          'renderFunnelFin', 'kpi-funnel-ho-val', 'kpi-funnel-vta-val']
for c in checks:
    cnt = html2.count(c)
    ok  = '✅' if cnt >= 1 else '❌'
    print(f"  {c}: {ok} ({cnt}x)")
