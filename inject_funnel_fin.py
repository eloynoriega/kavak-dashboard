"""
inject_funnel_fin.py  (v3)
Inyecta la sección "Funnel Financing" completa en el dashboard STR v2.
Sub-secciones:
  A. CR Conversiones (Res→HO, Res→App, App→Vta) con toggle Sin TI / Con TI
  B. SLA por Etapa — 3 columnas (card + chart por SLA)
  C. Dictámenes Kuna — stacked bar por semana de reserva con %
  D. CR por Perfil Crediticio — 4 columnas (X, A, B, C), 3 charts cada una
"""
import sys, os, json

SRC  = '/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html'
DEST = SRC

html = open(SRC, encoding='utf-8').read()

# ── 1. Load data ─────────────────────────────────────────────────────────────
data_path = '/tmp/rawFunnelFin.json'
if not os.path.exists(data_path):
    print("ERROR: /tmp/rawFunnelFin.json not found"); sys.exit(1)
with open(data_path) as f:
    funnel_data = json.load(f)

funnel_json      = json.dumps(funnel_data['rawFunnelFin'],     separators=(',',':'))
funnel_vta_json  = json.dumps(funnel_data['rawFunnelFinVta'],  separators=(',',':'))
funnel_mtd_json  = json.dumps(funnel_data['rawFunnelFinMTD'],  separators=(',',':'))
funnel_lmtd_json = json.dumps(funnel_data['rawFunnelFinLMTD'], separators=(',',':'))

def load_optional(path):
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    print(f"  ⚠️  {path} not found")
    return None

sla_ho_data  = load_optional('/tmp/rawSLAHO.json')
sla_app_data = load_optional('/tmp/rawSLAApp.json')
sla_vta_data = load_optional('/tmp/rawSLAVta.json')
dict_data    = load_optional('/tmp/rawDictamen.json')
perfil_data  = load_optional('/tmp/rawFunnelPerfil.json')

HAS_SLA    = sla_ho_data is not None and sla_app_data is not None and sla_vta_data is not None
HAS_DICT   = dict_data   is not None
HAS_PERFIL = perfil_data is not None

sla_ho_json  = json.dumps(sla_ho_data,  separators=(',',':')) if sla_ho_data  else '[]'
sla_app_json = json.dumps(sla_app_data, separators=(',',':')) if sla_app_data else '[]'
sla_vta_json = json.dumps(sla_vta_data, separators=(',',':')) if sla_vta_data else '[]'
dict_json    = json.dumps(dict_data,    separators=(',',':')) if HAS_DICT     else '[]'
perfil_json  = json.dumps(perfil_data,  separators=(',',':')) if HAS_PERFIL   else '[]'

# ── 2. Remove existing section ───────────────────────────────────────────────
for var in ['rawFunnelFin','rawFunnelFinVta','rawFunnelFinMTD','rawFunnelFinLMTD',
            'rawFunnelSLA','rawSLAHO','rawSLAApp','rawSLAVta',
            'rawDictamen','rawFunnelPerfil']:
    marker = f'const {var} = '
    while marker in html:
        pos = html.find(marker)
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

FUNNEL_START = '<!-- FUNNEL FIN SECTION -->'
FUNNEL_END   = '<!-- /FUNNEL FIN SECTION -->'
if FUNNEL_START in html:
    s = html.find(FUNNEL_START)
    e = html.find(FUNNEL_END) + len(FUNNEL_END)
    html = html[:s] + html[e:]
    print("  ↩ Removed funnel HTML section")

if '// ── FUNNEL FIN JS' in html:
    s = html.find('// ── FUNNEL FIN JS')
    e_marker = html.find('// ── END FUNNEL FIN JS')
    if s != -1 and e_marker > s:
        e = html.find('\n', e_marker) + 1
        html = html[:s] + html[e:]
        print("  ↩ Removed funnel JS")

# ── 3. Inject data vars ───────────────────────────────────────────────────────
anchor_data = 'const rawBacklogSummary'
idx_data = html.find(anchor_data)
if idx_data == -1:
    print("ERROR: rawBacklogSummary anchor not found"); sys.exit(1)

data_block = (
    f'const rawFunnelFin = {funnel_json};\n'
    f'const rawFunnelFinVta = {funnel_vta_json};\n'
    f'const rawFunnelFinMTD = {funnel_mtd_json};\n'
    f'const rawFunnelFinLMTD = {funnel_lmtd_json};\n'
    f'const rawSLAHO = {sla_ho_json};\n'
    f'const rawSLAApp = {sla_app_json};\n'
    f'const rawSLAVta = {sla_vta_json};\n'
    f'const rawDictamen = {dict_json};\n'
    f'const rawFunnelPerfil = {perfil_json};\n'
)
html = html[:idx_data] + data_block + html[idx_data:]
print(f"✅ Injected data vars")

# ── 4. Build HTML sub-sections ────────────────────────────────────────────────

sla_html = """
        <!-- Sub-B: SLA por Etapa — 3 columnas card+chart -->
        <div style="margin-top:20px; border-top:1px solid #e0e6ef; padding-top:16px;">
          <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.6px; color:#6b7fa3; margin-bottom:12px;">⏱ SLA por Etapa — días promedio</div>
          <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:14px;">

            <!-- Res→HO -->
            <div>
              <div class="kpi-card" style="border-left-color:#1a3a5c; margin-bottom:10px;">
                <div class="kpi-label">Res → Handoff</div>
                <div class="kpi-value" id="kpi-sla-res-ho-val">—</div>
                <div class="kpi-delta" id="kpi-sla-res-ho-delta"></div>
                <div class="kpi-sub" id="kpi-sla-res-ho-sub">días promedio</div>
              </div>
              <div class="chart-wrap" style="height:175px;"><canvas id="chart-sla-ho"></canvas></div>
            </div>

            <!-- Res→App -->
            <div>
              <div class="kpi-card" style="border-left-color:#2980b9; margin-bottom:10px;">
                <div class="kpi-label">Res → Aprobada</div>
                <div class="kpi-value" id="kpi-sla-res-app-val">—</div>
                <div class="kpi-delta" id="kpi-sla-res-app-delta"></div>
                <div class="kpi-sub" id="kpi-sla-res-app-sub">días promedio</div>
              </div>
              <div class="chart-wrap" style="height:175px;"><canvas id="chart-sla-app"></canvas></div>
            </div>

            <!-- App→Vta -->
            <div>
              <div class="kpi-card" style="border-left-color:#27ae60; margin-bottom:10px;">
                <div class="kpi-label">Aprobada → Venta</div>
                <div class="kpi-value" id="kpi-sla-app-vta-val">—</div>
                <div class="kpi-delta" id="kpi-sla-app-vta-delta"></div>
                <div class="kpi-sub" id="kpi-sla-app-vta-sub">días promedio</div>
              </div>
              <div class="chart-wrap" style="height:175px;"><canvas id="chart-sla-vta"></canvas></div>
            </div>

          </div>
          <div style="font-size:10px; color:#6b7fa3; margin-top:6px;">SLA por semana de <b>reserva</b> · Financing · b2b=0 · estimate_flag=1 · KPI = última semana completa</div>
        </div>
""" if HAS_SLA else ""

dict_html = """
        <!-- Sub-C: Dictámenes Kuna -->
        <div style="margin-top:20px; border-top:1px solid #e0e6ef; padding-top:16px;">
          <div style="display:flex; align-items:center; gap:16px; margin-bottom:10px; flex-wrap:wrap;">
            <div>
              <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.6px; color:#6b7fa3;">🏦 Dictámenes Kuna</div>
              <div style="font-size:10px; color:#6b7fa3; margin-top:3px;">Primer dictamen por semana de <b>reserva</b> · % del total de reservas Financing</div>
            </div>
            <div class="kpi-card" style="border-left-color:#27ae60; padding:8px 14px; margin:0; display:inline-flex; flex-direction:column; gap:2px;">
              <div class="kpi-label" style="margin-bottom:0;">Aprobada (período activo)</div>
              <div class="kpi-value" id="kpi-dict-aprobada" style="font-size:20px;">—</div>
            </div>
          </div>
          <div class="chart-wrap" style="height:220px;"><canvas id="chart-dictamen"></canvas></div>
        </div>
""" if HAS_DICT else ""

# CR por Perfil — 4 columnas, 3 charts cada una
def perfil_col(p, label, color):
    return f"""
            <!-- Perfil {p} -->
            <div>
              <div class="kpi-card" style="border-left-color:{color}; margin-bottom:10px; text-align:center;">
                <div class="kpi-value" style="font-size:17px; color:{color};">Perfil {p}</div>
                <div class="kpi-sub" style="margin-top:2px;">{label}</div>
              </div>
              <div style="font-size:9px; color:#6b7fa3; text-transform:uppercase; font-weight:700; letter-spacing:.4px; margin-bottom:3px;">CR Res→HO (%)</div>
              <div class="chart-wrap" style="height:130px;"><canvas id="chart-perfil-{p}-ho"></canvas></div>
              <div style="font-size:9px; color:#6b7fa3; text-transform:uppercase; font-weight:700; letter-spacing:.4px; margin:8px 0 3px;">CR Res→Aprobada (%)</div>
              <div class="chart-wrap" style="height:130px;"><canvas id="chart-perfil-{p}-app"></canvas></div>
              <div style="font-size:9px; color:#6b7fa3; text-transform:uppercase; font-weight:700; letter-spacing:.4px; margin:8px 0 3px;">CR Aprobada→Venta (%)</div>
              <div class="chart-wrap" style="height:130px;"><canvas id="chart-perfil-{p}-vta"></canvas></div>
            </div>"""

perfil_html = ("""
        <!-- Sub-D: CR por Perfil -->
        <div style="margin-top:20px; border-top:1px solid #e0e6ef; padding-top:16px;">
          <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.6px; color:#6b7fa3; margin-bottom:12px;">👤 CR por Perfil Crediticio</div>
          <div style="display:grid; grid-template-columns:repeat(4,1fr); gap:14px;">"""
    + perfil_col('X', 'Mejor historial', '#1a3a5c')
    + perfil_col('A', 'Historial sólido', '#2980b9')
    + perfil_col('B', 'Historial medio',  '#8e44ad')
    + perfil_col('C', 'Más restrictivo',  '#e67e22')
    + """
          </div>
          <div style="font-size:10px; color:#6b7fa3; margin-top:8px;">Semanal: últimas 7 sem · Mensual: últimos 4 meses · CR por semana de reserva · Financing · b2b=0 · estimate_flag=1</div>
        </div>
""") if HAS_PERFIL else ""

# ── 5. Inject HTML section ────────────────────────────────────────────────────
SLA_CIERRE_ANCHOR = '<div class="card" style="padding:16px; grid-column: 1 / -1;">\n        <div class="card-title" style="margin-bottom:12px;">SLA de Cierre'
idx_sla = html.find(SLA_CIERRE_ANCHOR)
if idx_sla == -1:
    print("ERROR: SLA Cierre anchor not found"); sys.exit(1)

funnel_html = (
"""<!-- FUNNEL FIN SECTION -->
      <div class="card" style="padding:16px; grid-column: 1 / -1;">

        <!-- Header: título + toggle TI + periodo -->
        <div style="display:flex; align-items:center; justify-content:space-between; flex-wrap:wrap; gap:8px; margin-bottom:16px;">
          <div class="card-title" style="margin-bottom:0;">🏦 Funnel Financing — Conversiones</div>
          <div style="display:flex; align-items:center; gap:5px; flex-wrap:wrap;">
            <span style="font-size:10px; color:#6b7fa3; margin-right:2px;">Filtro TI:</span>
            <button id="funnel-ti-btn-total"  onclick="setFunnelTI('total')"  class="dmbtn active" style="font-size:10px; padding:3px 9px;">Total</button>
            <button id="funnel-ti-btn-sin_ti" onclick="setFunnelTI('sin_ti')" class="dmbtn"        style="font-size:10px; padding:3px 9px;">Sin TI</button>
            <button id="funnel-ti-btn-con_ti" onclick="setFunnelTI('con_ti')" class="dmbtn"        style="font-size:10px; padding:3px 9px;">Con TI</button>
          </div>
          <span style="font-size:11px; color:#6b7fa3;" id="funnel-fin-mtd-label"></span>
        </div>

        <!-- Sub-A: KPI row -->
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

        <!-- Sub-A: 3 charts -->
        <div style="display:grid; grid-template-columns:repeat(3,1fr); gap:12px;">
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:#6b7fa3; margin-bottom:6px;">CR Res → Handoff (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-ho"></canvas></div>
          </div>
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:#6b7fa3; margin-bottom:6px;">CR Res → Aprobada (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-app"></canvas></div>
          </div>
          <div>
            <div style="font-size:11px; font-weight:700; text-transform:uppercase; letter-spacing:.5px; color:#6b7fa3; margin-bottom:6px;">CR Aprobada → Venta Fin (%)</div>
            <div class="chart-wrap" style="height:200px;"><canvas id="chart-funnel-vta"></canvas></div>
          </div>
        </div>
"""
+ sla_html
+ dict_html
+ perfil_html
+
"""
        <div style="font-size:10px; color:#6b7fa3; margin-top:10px;">
          Financing · estimate_flag=1 · b2b=0 · cohort por fecha_origen · SLA y Dictamen: bookings_history
        </div>
      </div>
<!-- /FUNNEL FIN SECTION -->
"""
)

html = html[:idx_sla] + funnel_html + html[idx_sla:]
print("✅ Injected funnel HTML section")

# ── 6. Inject JS ──────────────────────────────────────────────────────────────
js_block = """
// ── FUNNEL FIN JS ──────────────────────────────────────────────────────────
let currentFunnelTI = 'total'; // 'total' | 'sin_ti' | 'con_ti'

function setFunnelTI(val) {
  currentFunnelTI = val;
  ['total','sin_ti','con_ti'].forEach(function(v) {
    var btn = document.getElementById('funnel-ti-btn-' + v);
    if (btn) btn.classList.toggle('active', v === val);
  });
  renderFunnelFin();
  renderFunnelSLA();
  renderFunnelDictamen();
  renderFunnelPerfil();
}

// ── SHARED PERIOD HELPERS ─────────────────────────────────────────────────
function _funnelPeriod(dataRows, semanaCol) {
  // Returns {curW, lastW, prevW, monISO, todISO, curMonth, prevMonth}
  const tod = new Date(); tod.setHours(0,0,0,0);
  const dow = tod.getDay();
  const mon = new Date(tod); mon.setDate(tod.getDate()-(dow===0?6:dow-1));
  const monISO = mon.toISOString().slice(0,10);
  const sems   = [...new Set(dataRows.map(r=>r[semanaCol]))].sort();
  const closed = sems.filter(s=>s<monISO);
  // curW  = semana en curso (puede estar incompleta)
  // lastW = última semana CERRADA (para KPI de semanal/diario)
  // prevW = semana antes de lastW (comparación KPI)
  const lastW = closed[closed.length-1]||'';
  const prevW = closed[closed.length-2]||'';
  const curW  = sems.find(s=>s>=monISO)||lastW;
  const curMonth  = tod.toISOString().slice(0,7);
  const prevMonDt = new Date(tod.getFullYear(),tod.getMonth()-1,1);
  const prevMonth = prevMonDt.toISOString().slice(0,7);
  return {curW,lastW,prevW,curMonth,prevMonth,monISO,sems,closed};
}

function renderFunnelFin() {
  const hub    = currentHub;
  const gran   = currentGran;
  const tiMode = currentFunnelTI;

  function crFn(n,d)  { return d>0?(n/d*100):null; }
  function fmtPct(v)  { return v!=null?v.toFixed(1)+'%':'—'; }
  let _vsLbl='';
  function deltaPP(c,p) {
    if(c==null||p==null) return '';
    const d=c-p; const cls=d>=0?'delta-up':'delta-down';
    return `<span class="${cls}">${d>=0?'+':''}${d.toFixed(1)}pp ${_vsLbl}</span>`;
  }

  function getHO(r)   { return tiMode==='sin_ti'?(r.ho_sales||0):tiMode==='con_ti'?(r.ho_ti||0):(r.ho_total||0); }
  function getCNH(r)  { return tiMode==='sin_ti'?(r.can_no_ho_sales||0):tiMode==='con_ti'?(r.can_no_ho_ti||0):(r.can_no_ho_total||0); }
  function getAPP(r)  { return tiMode==='sin_ti'?(r.app_sales||0):tiMode==='con_ti'?(r.app_ti||0):(r.app_total||0); }
  function getCNA(r)  { return tiMode==='sin_ti'?(r.can_no_app_sales||0):tiMode==='con_ti'?(r.can_no_app_ti||0):(r.can_no_app_total||0); }
  function getVTA(r)  { return tiMode==='sin_ti'?(r.vta_app_sales||0):tiMode==='con_ti'?(r.vta_app_ti||0):(r.vta_app_total||0); }
  function getCANV(r) { return tiMode==='sin_ti'?(r.cancel_app_sales||0):tiMode==='con_ti'?(r.cancel_app_ti||0):(r.cancel_app_total||0); }

  function emptyB()  { return {ho:0,cnh:0,app:0,cna:0}; }
  function addB(b,r) { b.ho+=getHO(r);b.cnh+=getCNH(r);b.app+=getAPP(r);b.cna+=getCNA(r); }
  function emptyV()  { return {vta:0,can:0}; }
  function addV(b,r) { b.vta+=getVTA(r);b.can+=getCANV(r); }

  function aggPeriod(rows,emptyFn,addFn) {
    const s=emptyFn(); rows.forEach(r=>{if(hub==='__MX__'||r.hub===hub)addFn(s,r);}); return s;
  }
  function aggByTime(rows,keyCol,emptyFn,addFn) {
    const map={};
    rows.forEach(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return;
      const key=gran==='mensual'?r[keyCol].slice(0,7):r[keyCol];
      if(!map[key])map[key]=emptyFn(); addFn(map[key],r);
    });
    return map;
  }

  const P = _funnelPeriod(rawFunnelFin,'semana');

  let cur,prv,curVta,prvVta,periodLabel;
  if(gran==='mensual'){
    cur    =aggPeriod(rawFunnelFinMTD, emptyB,addB);
    prv    =aggPeriod(rawFunnelFinLMTD,emptyB,addB);
    curVta =aggPeriod(rawFunnelFinMTD, emptyV,addV);
    prvVta =aggPeriod(rawFunnelFinLMTD,emptyV,addV);
    periodLabel='MTD vs LMTD'; _vsLbl='vs LMTD';
  } else if(gran==='diario'){
    // Diario: WTD vs LWTD
    cur    =aggPeriod(rawFunnelFin.filter(r=>r.semana===P.curW),    emptyB,addB);
    prv    =aggPeriod(rawFunnelFin.filter(r=>r.semana===P.lastW),   emptyB,addB);
    curVta =aggPeriod(rawFunnelFinVta.filter(r=>r.semana===P.curW), emptyV,addV);
    prvVta =aggPeriod(rawFunnelFinVta.filter(r=>r.semana===P.lastW),emptyV,addV);
    periodLabel='WTD vs LWTD'; _vsLbl='vs LWTD';
  } else {
    // Semanal: WTD vs LWTD
    cur    =aggPeriod(rawFunnelFin.filter(r=>r.semana===P.curW),    emptyB,addB);
    prv    =aggPeriod(rawFunnelFin.filter(r=>r.semana===P.lastW),   emptyB,addB);
    curVta =aggPeriod(rawFunnelFinVta.filter(r=>r.semana===P.curW), emptyV,addV);
    prvVta =aggPeriod(rawFunnelFinVta.filter(r=>r.semana===P.lastW),emptyV,addV);
    periodLabel='WTD vs LWTD'; _vsLbl='vs LWTD';
  }

  // Sub text (only total mode)
  let subHO='',subAPP='',subVTA='';
  if(tiMode==='total'){
    const srcR  = gran==='mensual'?rawFunnelFinMTD :rawFunnelFin.filter(r=>r.semana===P.curW);
    const srcVR = gran==='mensual'?rawFunnelFinMTD :rawFunnelFinVta.filter(r=>r.semana===P.curW);
    const sin={ho:0,cnh:0,app:0,cna:0},con={ho:0,cnh:0,app:0,cna:0};
    srcR.forEach(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return;
      sin.ho+=r.ho_sales||0;sin.cnh+=r.can_no_ho_sales||0;sin.app+=r.app_sales||0;sin.cna+=r.can_no_app_sales||0;
      con.ho+=r.ho_ti||0;   con.cnh+=r.can_no_ho_ti||0;   con.app+=r.app_ti||0;   con.cna+=r.can_no_app_ti||0;
    });
    let vs=0,vsc=0,vc=0,vcc=0;
    srcVR.forEach(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return;
      vs+=r.vta_app_sales||0;vsc+=r.cancel_app_sales||0;vc+=r.vta_app_ti||0;vcc+=r.cancel_app_ti||0;
    });
    subHO  =`Sin TI: ${fmtPct(crFn(sin.ho,sin.ho+sin.cnh))} · Con TI: ${fmtPct(crFn(con.ho,con.ho+con.cnh))}`;
    subAPP =`Sin TI: ${fmtPct(crFn(sin.app,sin.app+sin.cna))} · Con TI: ${fmtPct(crFn(con.app,con.app+con.cna))}`;
    subVTA =`Sin TI: ${fmtPct(crFn(vs,vs+vsc))} · Con TI: ${fmtPct(crFn(vc,vc+vcc))}`;
  }

  const kpis=[
    {id:'ho', cur:crFn(cur.ho,cur.ho+cur.cnh),  prv:crFn(prv.ho,prv.ho+prv.cnh),  sub:subHO},
    {id:'app',cur:crFn(cur.app,cur.app+cur.cna), prv:crFn(prv.app,prv.app+prv.cna),sub:subAPP},
    {id:'vta',cur:crFn(curVta.vta,curVta.vta+curVta.can),prv:crFn(prvVta.vta,prvVta.vta+prvVta.can),sub:subVTA},
  ];
  const setEl=(id,fn)=>{const el=document.getElementById(id);if(el)fn(el);};
  kpis.forEach(k=>{
    setEl('kpi-funnel-'+k.id+'-val',  el=>el.textContent=fmtPct(k.cur));
    setEl('kpi-funnel-'+k.id+'-delta',el=>el.innerHTML=deltaPP(k.cur,k.prv));
    setEl('kpi-funnel-'+k.id+'-sub',  el=>el.textContent=k.sub);
  });
  const lbl=document.getElementById('funnel-fin-mtd-label');
  if(lbl) lbl.textContent=periodLabel;

  // Time series
  const timeMap=aggByTime(rawFunnelFin,   'semana',emptyB,addB);
  const vtaMap =aggByTime(rawFunnelFinVta,'semana',emptyV,addV);
  const allKeys=[...new Set([...Object.keys(timeMap),...Object.keys(vtaMap)])].sort();
  const xlabels=gran==='mensual'
    ?allKeys.map(k=>{const[y,m]=k.split('-');return m+'/'+y.slice(2);})
    :allKeys.map(k=>k.slice(5).replace('-','/'));
  function seriesF(map,fn){
    return allKeys.map(k=>{const m=map[k];if(!m)return null;const v=fn(m);return v!=null?+v.toFixed(2):null;});
  }
  const cmap={total:{ho:'#1a3a5c',app:'#2980b9',vta:'#27ae60'},sin_ti:{ho:'#2980b9',app:'#3498db',vta:'#2ecc71'},con_ti:{ho:'#e67e22',app:'#d35400',vta:'#c0392b'}};
  const cols=cmap[tiMode]||cmap.total;
  const modeLabel=tiMode==='sin_ti'?'Sin TI':tiMode==='con_ti'?'Con TI':'Total';

  [{chartKey:'funnel-ho', map:timeMap,col:cols.ho, fn:m=>crFn(m.ho, m.ho +m.cnh)},
   {chartKey:'funnel-app',map:timeMap,col:cols.app,fn:m=>crFn(m.app,m.app+m.cna)},
   {chartKey:'funnel-vta',map:vtaMap, col:cols.vta,fn:m=>crFn(m.vta,m.vta+m.can)},
  ].forEach(def=>{
    destroyChart(def.chartKey);
    const cv=document.getElementById('chart-'+def.chartKey); if(!cv) return;
    charts[def.chartKey]=new Chart(cv,{type:'line',data:{labels:xlabels,datasets:[{
      label:modeLabel,data:seriesF(def.map,def.fn),
      borderColor:def.col,backgroundColor:def.col+'22',borderWidth:2.5,
      pointRadius:2,pointHoverRadius:4,tension:0.3,spanGaps:true,fill:false,
    }]},options:{responsive:true,maintainAspectRatio:false,plugins:{
      legend:{display:true,position:'bottom',labels:{boxWidth:10,font:{size:10},padding:8}},
      datalabels:{display:false},
      tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y!=null?c.parsed.y.toFixed(1)+'%':'—'}`}}
    },scales:{
      x:{ticks:{font:{size:9},maxRotation:45}},
      y:{min:50,max:100,ticks:{callback:v=>v+'%',font:{size:9}},grid:{color:'rgba(0,0,0,0.05)'}}
    }}});
  });
}

function renderFunnelSLA() {
  if(!rawSLAHO||rawSLAHO.length===0) return;
  const hub    = currentHub;
  const gran   = currentGran;
  const tiMode = currentFunnelTI;

  function filterRows(rows) {
    return rows.filter(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return false;
      if(tiMode!=='total'&&r.ti_flag!==tiMode) return false;
      return true;
    });
  }
  function wAvg(rows,sk,ck) {
    let sw=0,swv=0;
    rows.forEach(r=>{const w=r[ck]||0;const v=r[sk];if(v!=null&&w>0){sw+=w;swv+=w*v;}});
    return sw>0?swv/sw:null;
  }

  // Cada SLA tiene su propio período basado en su evento
  const PH = _funnelPeriod(rawSLAHO, 'semana');
  const PA = _funnelPeriod(rawSLAApp,'semana');
  const PV = _funnelPeriod(rawSLAVta,'semana');

  // KPI: usa CURRENT week (puede ser parcial — pero es real: HOs/Apps/Vtas de esta semana)
  // Para mensual: MTD
  let vsLbl = gran==='mensual' ? 'vs mes ant' : 'vs sem ant';
  let subLbl = gran==='mensual' ? 'MTD vs mes ant' : 'WTD vs sem ant';

  function getKpiRows(dataset, P, slaKey, cntKey) {
    if(gran==='mensual') {
      const cur=filterRows(dataset.filter(r=>r.semana.slice(0,7)===P.curMonth));
      const prv=filterRows(dataset.filter(r=>r.semana.slice(0,7)===P.prevMonth));
      return {cur:wAvg(cur,slaKey,cntKey), prv:wAvg(prv,slaKey,cntKey)};
    } else {
      // WTD = incluimos semana en curso (los HOs/Apps/Vtas que ya ocurrieron esta semana son reales)
      const cur=filterRows(dataset.filter(r=>r.semana===P.curW));
      const prv=filterRows(dataset.filter(r=>r.semana===P.lastW));
      return {cur:wAvg(cur,slaKey,cntKey), prv:wAvg(prv,slaKey,cntKey)};
    }
  }

  const kH = getKpiRows(rawSLAHO, PH,'sla_res_ho','n_ho');
  const kA = getKpiRows(rawSLAApp,PA,'sla_res_app','n_app');
  const kV = getKpiRows(rawSLAVta,PV,'sla_app_vta','n_vta');

  renderSLAKPIs(
    [{id:'res-ho', cur:kH.cur, prv:kH.prv},
     {id:'res-app',cur:kA.cur, prv:kA.prv},
     {id:'app-vta',cur:kV.cur, prv:kV.prv}],
    subLbl, vsLbl
  );

  // Trend charts — 3 separados, cada uno con su propio dataset y semana
  const SLA_DEFS = [
    {chartKey:'sla-ho',  dataset:rawSLAHO,  slaKey:'sla_res_ho',  cntKey:'n_ho',  col:'#1a3a5c', label:'Res→HO (por sem de HO)'},
    {chartKey:'sla-app', dataset:rawSLAApp, slaKey:'sla_res_app', cntKey:'n_app', col:'#2980b9', label:'Res→App (por sem de aprobación)'},
    {chartKey:'sla-vta', dataset:rawSLAVta, slaKey:'sla_app_vta', cntKey:'n_vta', col:'#27ae60', label:'App→Vta (por sem de venta)'},
  ];

  SLA_DEFS.forEach(def=>{
    const tmap={};
    filterRows(def.dataset).forEach(r=>{
      const key=gran==='mensual'?r.semana.slice(0,7):r.semana;
      if(!tmap[key])tmap[key]={n:0,wt:0};
      if(r[def.slaKey]!=null&&(r[def.cntKey]||0)>0){
        tmap[key].wt+=r[def.slaKey]*(r[def.cntKey]||0);
        tmap[key].n +=r[def.cntKey]||0;
      }
    });
    const allK=Object.keys(tmap).sort();
    const xlbls=gran==='mensual'
      ?allK.map(k=>{const[y,m]=k.split('-');return m+'/'+y.slice(2);})
      :allK.map(k=>k.slice(5).replace('-','/'));
    const data=allK.map(k=>tmap[k].n>0?+(tmap[k].wt/tmap[k].n).toFixed(1):null);

    destroyChart(def.chartKey);
    const cv=document.getElementById('chart-'+def.chartKey); if(!cv) return;
    charts[def.chartKey]=new Chart(cv,{type:'line',data:{labels:xlbls,datasets:[{
      label:def.label,data,
      borderColor:def.col,backgroundColor:def.col+'22',borderWidth:2.5,
      pointRadius:2,pointHoverRadius:4,tension:0.3,spanGaps:true,fill:false,
    }]},options:{responsive:true,maintainAspectRatio:false,plugins:{
      legend:{display:true,position:'bottom',labels:{boxWidth:10,font:{size:10},padding:8}},
      datalabels:{display:false},
      tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y!=null?c.parsed.y.toFixed(1)+'d':'—'}`}}
    },scales:{
      x:{ticks:{font:{size:9},maxRotation:45}},
      y:{min:0,ticks:{callback:v=>v+'d',font:{size:9}},grid:{color:'rgba(0,0,0,0.05)'}}
    }}});
  });
}

function renderSLAKPIs(kpis, periodLabel, vsLbl) {
  function fmtD(v) { return v!=null?v.toFixed(1)+'d':'—'; }
  function dDay(c,p) {
    if(c==null||p==null) return '';
    const d=c-p; const cls=d<=0?'delta-up':'delta-down';
    return `<span class="${cls}">${d>=0?'+':''}${d.toFixed(1)}d ${vsLbl}</span>`;
  }
  const setEl=(id,fn)=>{const el=document.getElementById(id);if(el)fn(el);};
  kpis.forEach(k=>{
    setEl('kpi-sla-'+k.id+'-val',  el=>el.textContent=fmtD(k.cur));
    setEl('kpi-sla-'+k.id+'-delta',el=>el.innerHTML=dDay(k.cur,k.prv));
    setEl('kpi-sla-'+k.id+'-sub',  el=>el.textContent=periodLabel);
  });
}

function renderFunnelDictamen() {
  if(!rawDictamen||rawDictamen.length===0) return;
  const hub    = currentHub;
  const gran   = currentGran;
  const tiMode = currentFunnelTI;

  function filterRows(rows) {
    return rows.filter(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return false;
      if(tiMode!=='total'&&r.ti_flag!==tiMode) return false;
      return true;
    });
  }

  const P = _funnelPeriod(rawDictamen,'semana');

  // KPI: % Aprobada período activo
  let kpiRows;
  if(gran==='mensual'){
    kpiRows=filterRows(rawDictamen.filter(r=>r.semana.slice(0,7)===P.curMonth));
  } else {
    kpiRows=filterRows(rawDictamen.filter(r=>r.semana===P.curW));
  }
  const totN=kpiRows.reduce((s,r)=>s+(r.n||0),0);
  const aprN=kpiRows.filter(r=>r.dictamen==='Aprobada').reduce((s,r)=>s+(r.n||0),0);
  const kpiEl=document.getElementById('kpi-dict-aprobada');
  if(kpiEl) kpiEl.textContent=totN>0?(aprN/totN*100).toFixed(1)+'%':'—';

  // Stacked bar % por período
  const CATS=['Aprobada','Incompleta','Rechazada','Condicionada','En proceso','Sin dictamen','Otro'];
  const COLORS={'Aprobada':'#27ae60','Incompleta':'#f39c12','Rechazada':'#e74c3c',
                'Condicionada':'#9b59b6','En proceso':'#3498db','Sin dictamen':'#95a5a6','Otro':'#bdc3c7'};

  const tmap={};
  filterRows(rawDictamen).forEach(r=>{
    const key=gran==='mensual'?r.semana.slice(0,7):r.semana;
    if(!tmap[key]){tmap[key]={};CATS.forEach(c=>tmap[key][c]=0);}
    const cat=r.dictamen||'Otro';
    if(tmap[key][cat]!==undefined) tmap[key][cat]+=(r.n||0);
    else tmap[key]['Otro']+=(r.n||0);
  });
  const allK=Object.keys(tmap).sort();
  const xlbls=gran==='mensual'
    ?allK.map(k=>{const[y,m]=k.split('-');return m+'/'+y.slice(2);})
    :allK.map(k=>k.slice(5).replace('-','/'));

  const activeCats=CATS.filter(c=>allK.some(k=>(tmap[k][c]||0)>0));
  function pctD(cat){
    return allK.map(k=>{
      const m=tmap[k]; const tot=CATS.reduce((s,c)=>s+(m[c]||0),0);
      return tot>0?+((m[cat]||0)/tot*100).toFixed(1):0;
    });
  }

  destroyChart('dictamen');
  const cv=document.getElementById('chart-dictamen'); if(!cv) return;
  charts['dictamen']=new Chart(cv,{type:'bar',data:{labels:xlbls,datasets:activeCats.map(cat=>({
    label:cat,data:pctD(cat),backgroundColor:COLORS[cat]||'#bdc3c7',stack:'dict',
  }))},options:{responsive:true,maintainAspectRatio:false,plugins:{
    legend:{display:true,position:'bottom',labels:{boxWidth:10,font:{size:10},padding:6}},
    datalabels:{
      display: ctx => {
        // Solo mostrar % en Aprobada e Incompleta si > 5%
        const cat = activeCats[ctx.datasetIndex];
        return (cat==='Aprobada'||cat==='Incompleta') && ctx.dataset.data[ctx.dataIndex] >= 5;
      },
      formatter: v => v.toFixed(0)+'%',
      color: '#fff',
      font: {size:9, weight:'bold'},
      anchor:'center', align:'center',
    },
    tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y.toFixed(1)}%`}}
  },scales:{
    x:{stacked:true,ticks:{font:{size:9},maxRotation:45}},
    y:{stacked:true,min:0,max:100,ticks:{callback:v=>v+'%',font:{size:9}},grid:{color:'rgba(0,0,0,0.05)'}}
  }}});
}

function renderFunnelPerfil() {
  if(!rawFunnelPerfil||rawFunnelPerfil.length===0) return;
  const hub    = currentHub;
  const gran   = currentGran;
  const tiMode = currentFunnelTI;

  function filterRows(rows) {
    return rows.filter(r=>{
      if(hub!=='__MX__'&&r.hub!==hub) return false;
      if(tiMode!=='total'&&r.ti_flag!==tiMode) return false;
      return true;
    });
  }

  const P = _funnelPeriod(rawFunnelPerfil,'semana');

  // Build time keys — últimas 7 semanas (semanal/diario) o últimos 4 meses (mensual)
  let timeKeys;
  if(gran==='mensual'){
    // últimos 4 meses
    const allMonths=[...new Set(rawFunnelPerfil.map(r=>r.semana.slice(0,7)))].sort();
    timeKeys=allMonths.slice(-4);
  } else {
    // últimas 7 semanas completas
    timeKeys=P.closed.slice(-7);
  }

  function crFn(n,d){return d>0?+(n/d*100).toFixed(1):null;}

  const PERFILES=['X','A','B','C'];
  const COLORS={'X':'#1a3a5c','A':'#2980b9','B':'#8e44ad','C':'#e67e22'};

  PERFILES.forEach(perfil=>{
    const col=COLORS[perfil]||'#1a3a5c';

    // Build time-bucketed aggregates for this perfil
    const bktMap={};
    timeKeys.forEach(k=>bktMap[k]={n_total:0,n_ho:0,n_app:0,n_vta:0});

    filterRows(rawFunnelPerfil).filter(r=>r.perfil===perfil).forEach(r=>{
      const key=gran==='mensual'?r.semana.slice(0,7):r.semana;
      if(!bktMap[key]) return; // outside our time window
      bktMap[key].n_total+=r.n_total||0;
      bktMap[key].n_ho   +=r.n_ho   ||0;
      bktMap[key].n_app  +=r.n_app  ||0;
      bktMap[key].n_vta  +=r.n_vta  ||0;
    });

    const xlbls=gran==='mensual'
      ?timeKeys.map(k=>{const[y,m]=k.split('-');return m+'/'+y.slice(2);})
      :timeKeys.map(k=>k.slice(5).replace('-','/'));

    const crHO  = timeKeys.map(k=>crFn(bktMap[k].n_ho, bktMap[k].n_total));
    const crApp = timeKeys.map(k=>crFn(bktMap[k].n_app,bktMap[k].n_total));
    const crVta = timeKeys.map(k=>crFn(bktMap[k].n_vta,bktMap[k].n_app));

    const chartSpecs=[
      {metric:'ho',  data:crHO,  label:'CR Res→HO'},
      {metric:'app', data:crApp, label:'CR Res→App'},
      {metric:'vta', data:crVta, label:'CR App→Vta'},
    ];

    chartSpecs.forEach(spec=>{
      const chartKey=`perfil-${perfil}-${spec.metric}`;
      destroyChart(chartKey);
      const cv=document.getElementById('chart-'+chartKey); if(!cv) return;
      charts[chartKey]=new Chart(cv,{type:'line',data:{labels:xlbls,datasets:[{
        label:spec.label,
        data:spec.data,
        borderColor:col,backgroundColor:col+'22',borderWidth:2,
        pointRadius:3,pointHoverRadius:5,tension:0.3,spanGaps:true,fill:false,
      }]},options:{responsive:true,maintainAspectRatio:false,plugins:{
        legend:{display:false},
        datalabels:{
          display:true,
          formatter:v=>v!=null?v.toFixed(0)+'%':'',
          color:col, font:{size:8,weight:'bold'},
          anchor:'end',align:'top',offset:-2,
          clip:false,
        },
        tooltip:{callbacks:{label:c=>`${c.dataset.label}: ${c.parsed.y!=null?c.parsed.y.toFixed(1)+'%':'—'}`}}
      },scales:{
        x:{ticks:{font:{size:8},maxRotation:45}},
        y:{min:0,max:100,ticks:{callback:v=>v+'%',font:{size:8}},grid:{color:'rgba(0,0,0,0.05)'}}
      }}});
    });
  });
}
// ── END FUNNEL FIN JS ───────────────────────────────────────────────────────

"""

boot_anchor = '// ═══════════════════════════════════════════════════════════\n// BOOT'
idx_boot = html.find(boot_anchor)
if idx_boot == -1:
    print("ERROR: BOOT anchor not found"); sys.exit(1)
html = html[:idx_boot] + js_block + html[idx_boot:]
print("✅ Injected funnel JS")

# ── 7. Wire into refreshAll ───────────────────────────────────────────────────
OLD_REFRESH = '  renderFunnelFin();\n  renderSLADeliveryChart();\n}'
NEW_REFRESH  = '  renderFunnelFin();\n  renderFunnelSLA();\n  renderFunnelDictamen();\n  renderFunnelPerfil();\n  renderSLADeliveryChart();\n}'

if NEW_REFRESH in html:
    print("ℹ️  refreshAll ya wired")
elif OLD_REFRESH in html:
    html = html.replace(OLD_REFRESH, NEW_REFRESH, 1)
    print("✅ Wired into refreshAll()")
else:
    alt = '  renderSLADeliveryChart();\n}'
    if alt in html:
        html = html.replace(alt, '  renderFunnelFin();\n  renderFunnelSLA();\n  renderFunnelDictamen();\n  renderFunnelPerfil();\n'+alt, 1)
        print("✅ Wired into refreshAll() (fallback)")
    else:
        print("⚠️  refreshAll wire failed")

# ── 8. Wire into DOMContentLoaded ─────────────────────────────────────────────
NEW_DOM = '  renderFunnelFin();\n  renderFunnelSLA();\n  renderFunnelDictamen();\n  renderFunnelPerfil();\n  renderNPSSection();\n});'

for old_pat in [
    '  renderFunnelFin();\n  renderFunnelSLA();\n  renderFunnelDictamen();\n  renderFunnelPerfil();\n  renderNPSSection();\n});',
    '  renderFunnelFin();\n  renderNPSSection();\n});',
    '  renderFunnelFin();\n  renderNPSSection();\n  renderNPSSection();\n});',
    '  renderFunnelFin();\n});',
]:
    if old_pat in html:
        if old_pat == NEW_DOM:
            print("ℹ️  DOMContentLoaded ya wired")
        else:
            html = html.replace(old_pat, NEW_DOM, 1)
            print("✅ Wired into DOMContentLoaded")
        break

# ── 9. Write ──────────────────────────────────────────────────────────────────
with open(DEST, 'w', encoding='utf-8') as f:
    f.write(html)
print(f"\n✅ Done → {DEST} ({len(html)//1024} KB)")

# Sanity
html2 = open(DEST).read()
checks = [
    ('rawFunnelSLA',1),('rawDictamen',1),('rawFunnelPerfil',1),
    ('chart-sla-ho',2),('chart-sla-app',2),('chart-sla-vta',2),
    ('chart-dictamen',2),
    ('chart-perfil-X-ho',2),('chart-perfil-A-ho',2),('chart-perfil-B-ho',2),('chart-perfil-C-ho',2),
    ('renderFunnelSLA',3),('renderFunnelDictamen',3),('renderFunnelPerfil',3),
    ('renderSLAKPIs',2),('_funnelPeriod',3),('setFunnelTI',2),
]
print("\nSanity:")
for name,mn in checks:
    cnt=html2.count(name)
    print(f"  {'✅' if cnt>=mn else '❌'} {name}: {cnt}x")
