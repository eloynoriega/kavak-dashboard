"""
inject_all_data.py
Inyecta / reemplaza TODOS los data variables del STR Dashboard v2
que no cubren inject_backlog_summary.py ni inject_nps_and_drilldown.py:

  rawCancelMotivos   ← /tmp/rawCancelMotivos.json
  rawTestimonios     ← /tmp/rawTestimonios.json
  rawSTRtradein      ← /tmp/rawSTRtradein.json
  rawSLACierre       ← /tmp/rawSLACierre_real.json
  rawDimSTR          ← /tmp/rawDimSTR_v2.json
  cohortFin          ← /tmp/rawCohort.json ['fin']
  cohortCash         ← /tmp/rawCohort.json ['cash']
  cohortEntregaFin   ← /tmp/rawCohortEntrega.json ['fin']
  cohortEntregaCash  ← /tmp/rawCohortEntrega.json ['cash']
  rawMTD             ← /tmp/rawSTRKPIs.json ['rawMTD']
  rawLMTD            ← /tmp/rawSTRKPIs.json ['rawLMTD']
  rawE_w             ← /tmp/rawSTRKPIs.json ['rawE_w']
  rawA_w             ← /tmp/rawSTRKPIs.json ['rawA_w']
"""
import json, sys, os

SRC  = '/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html'
DEST = '/Users/choloynoriega/Desktop/kavak_str_dashboard_v2.html'

with open(SRC, 'r', encoding='utf-8') as f:
    html = f.read()

# ── Helper: replace a JS const variable in the HTML ───────────────────────────
def replace_var(html, var_name, new_json_str, end_token=';'):
    """
    Finds `const {var_name} = ...;` and replaces the value part.
    Returns updated html.
    """
    marker = f'const {var_name} = '
    idx = html.find(marker)
    if idx == -1:
        print(f"  ⚠️  {var_name}: NOT FOUND in HTML — skipping")
        return html

    # Find the end of the declaration (look for ;\n or ;)
    # Handle objects {} and arrays []
    idx_val_start = idx + len(marker)
    first_char = html[idx_val_start:idx_val_start+1]

    if first_char in ('{', '['):
        # Find matching close bracket
        depth = 0
        in_str = False
        escape_next = False
        i = idx_val_start
        while i < len(html):
            c = html[i]
            if escape_next:
                escape_next = False
            elif c == '\\' and in_str:
                escape_next = True
            elif c == '"' and not escape_next:
                in_str = not in_str
            elif not in_str:
                if c in ('{', '['):
                    depth += 1
                elif c in ('}', ']'):
                    depth -= 1
                    if depth == 0:
                        idx_val_end = i + 1  # include the closing bracket
                        break
            i += 1
        else:
            print(f"  ⚠️  {var_name}: Could not find end of value — skipping")
            return html
    else:
        # String or number — find the semicolon
        idx_val_end = html.find(';', idx_val_start)
        if idx_val_end == -1:
            print(f"  ⚠️  {var_name}: Could not find semicolon — skipping")
            return html

    html = html[:idx_val_start] + new_json_str + html[idx_val_end:]
    return html

# ── 1. rawCancelMotivos ───────────────────────────────────────────────────────
path = '/tmp/rawCancelMotivos.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    j = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    if 'const rawCancelMotivos = ' in html:
        html = replace_var(html, 'rawCancelMotivos', j)
    else:
        # Insert before rawTestimonios (or as fallback before rawSTRtradein)
        anchor = 'const rawTestimonios = '
        if anchor not in html:
            anchor = 'const rawSTRtradein = '
        idx_anc = html.find(anchor)
        if idx_anc != -1:
            html = html[:idx_anc] + f'const rawCancelMotivos = {j};\n' + html[idx_anc:]
            print(f"  ℹ️  rawCancelMotivos inserted (was missing)")
        else:
            print("  ⚠️  rawCancelMotivos: could not find anchor to insert")
    print(f"✅ rawCancelMotivos ({len(j)//1024} KB)")
else:
    print("⚠️  rawCancelMotivos.json not found")

# ── 2. rawTestimonios ─────────────────────────────────────────────────────────
path = '/tmp/rawTestimonios.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    j = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    html = replace_var(html, 'rawTestimonios', j)
    print(f"✅ rawTestimonios ({len(j)//1024} KB)")
else:
    print("⚠️  rawTestimonios.json not found")

# ── 3. rawSTRtradein ─────────────────────────────────────────────────────────
path = '/tmp/rawSTRtradein.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    j = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    html = replace_var(html, 'rawSTRtradein', j)
    print(f"✅ rawSTRtradein ({len(j)//1024} KB)")
else:
    print("⚠️  rawSTRtradein.json not found")

# ── 4. rawSLACierre ──────────────────────────────────────────────────────────
path = '/tmp/rawSLACierre_real.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    j = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    html = replace_var(html, 'rawSLACierre', j)
    print(f"✅ rawSLACierre ({len(j)//1024} KB)")
else:
    print("⚠️  rawSLACierre_real.json not found")

# ── 5. rawDimSTR ─────────────────────────────────────────────────────────────
path = '/tmp/rawDimSTR_v2.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    j = json.dumps(data, separators=(',', ':'), ensure_ascii=False)
    html = replace_var(html, 'rawDimSTR', j)
    print(f"✅ rawDimSTR ({len(j)//1024} KB)")
else:
    print("⚠️  rawDimSTR_v2.json not found")

# ── 6. cohortFin / cohortCash ─────────────────────────────────────────────────
path = '/tmp/rawCohort.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    for key, var in [('fin', 'cohortFin'), ('cash', 'cohortCash')]:
        if key in data:
            j = json.dumps(data[key], separators=(',', ':'), ensure_ascii=False)
            html = replace_var(html, var, j)
            print(f"✅ {var} ({len(j)//1024} KB)")
        else:
            print(f"⚠️  {key} not in rawCohort.json")
else:
    print("⚠️  rawCohort.json not found")

# ── 7. cohortEntregaFin / cohortEntregaCash ───────────────────────────────────
path = '/tmp/rawCohortEntrega.json'
if os.path.exists(path):
    with open(path) as f:
        data = json.load(f)
    for key, var in [('fin', 'cohortEntregaFin'), ('cash', 'cohortEntregaCash')]:
        if key in data:
            j = json.dumps(data[key], separators=(',', ':'), ensure_ascii=False)
            html = replace_var(html, var, j)
            print(f"✅ {var} ({len(j)//1024} KB)")
        else:
            print(f"⚠️  {key} not in rawCohortEntrega.json")
else:
    print("⚠️  rawCohortEntrega.json not found")

# ── 8. rawMTD / rawLMTD / rawE_w / rawA_w (from fetch_str_kpis.py) ──────────
path = '/tmp/rawSTRKPIs.json'
if os.path.exists(path):
    with open(path) as f:
        kpis = json.load(f)

    for var in ['rawMTD', 'rawLMTD']:
        if var in kpis:
            j = json.dumps(kpis[var], separators=(',', ':'), ensure_ascii=False)
            html = replace_var(html, var, j)
            print(f"✅ {var} ({len(j)//1024} KB)")

    for var in ['rawE_w', 'rawA_w', 'rawB_w', 'rawDaily']:
        if var in kpis:
            j = json.dumps(kpis[var], separators=(',', ':'), ensure_ascii=False)
            html = replace_var(html, var, j)
            print(f"✅ {var} ({len(j)//1024} KB)")

    if 'rawLastMonth' in kpis:
        j = json.dumps(kpis['rawLastMonth'], separators=(',', ':'), ensure_ascii=False)
        html = replace_var(html, 'rawLastMonth', j)
        print(f"✅ rawLastMonth ({len(j)//1024} KB)")

    # Also update MTD_LABEL if present in HTML (handles both single and double quotes)
    if 'mtd_label' in kpis:
        mtd_label = kpis['mtd_label']
        import re as _re
        ml_match = _re.search(r"const MTD_LABEL = (['\"])(.+?)\1;", html)
        if ml_match:
            html = html[:ml_match.start()] + f'const MTD_LABEL = "{mtd_label}";' + html[ml_match.end():]
            print(f"✅ MTD_LABEL → '{mtd_label}'")
        else:
            print("  ℹ️  MTD_LABEL not found (may use dynamic format)")
else:
    print("⚠️  rawSTRKPIs.json not found — run fetch_str_kpis.py first")

# ── 9. Write ──────────────────────────────────────────────────────────────────
with open(DEST, 'w', encoding='utf-8') as f:
    f.write(html)

size_kb = len(html) // 1024
print(f"\n✅ Done → {DEST} ({size_kb} KB)")

# ── Sanity check ──────────────────────────────────────────────────────────────
print("\nSanity checks:")
for var in ['rawCancelMotivos', 'rawTestimonios', 'rawSTRtradein', 'rawSLACierre',
            'rawDimSTR', 'cohortFin', 'cohortCash', 'cohortEntregaFin', 'cohortEntregaCash',
            'rawMTD', 'rawLMTD', 'rawE_w', 'rawA_w', 'rawB_w', 'rawDaily', 'rawLastMonth']:
    count = html.count(f'const {var} ')
    status = '✅' if count == 1 else (f'⚠️  {count}x' if count > 1 else '❌ missing')
    print(f"  {var}: {status}")
