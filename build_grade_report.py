"""
build_grade_report.py
=====================
DROP THIS FILE into your SlateIQ root folder (next to run_grader.ps1).

Reads graded_nba/cbb/nhl/soccer_DATE.xlsx and writes:
  ui_runner/ui_runner/templates/slate_eval_DATE.html

Usage (called by run_grader.ps1 automatically):
    py -3 build_grade_report.py --date 2026-03-06 --nba ... --cbb ...

Manual run (auto-detects yesterday):
    py -3 build_grade_report.py
    py -3 build_grade_report.py --date 2026-03-06
"""
from __future__ import annotations
import argparse, html as html_lib, sys, tempfile, os
from datetime import datetime, timedelta
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("ERROR: pip install pandas openpyxl"); sys.exit(1)

# ── Self-locating paths ────────────────────────────────────────────────────────
def _find_root() -> Path:
    here = Path(__file__).resolve().parent
    for candidate in [here, here.parent, here.parent.parent]:
        if (candidate / "ui_runner").exists():
            return candidate
    return here

BASE_DIR      = _find_root()
TEMPLATES_DIR = BASE_DIR / "ui_runner" / "ui_runner" / "templates"

SPORT_COLORS = {"NBA":"#3b82f6","CBB":"#8b5cf6","NHL":"#06b6d4","SOCCER":"#10b981","MLB":"#f59e0b"}
SPORT_ICONS  = {"NBA":"🏀","CBB":"🎓","NHL":"🏒","SOCCER":"⚽","MLB":"⚾"}

# ── Helpers ────────────────────────────────────────────────────────────────────
def h(v) -> str:
    return html_lib.escape(str(v) if v is not None else "")

def pct(n, d):
    return f"{n/d*100:.1f}%" if d else "—"

def pct_f(n, d):
    return n/d*100 if d else 0.0

def rate_color(p):
    return "var(--green)" if p >= 65 else ("var(--amber)" if p >= 50 else "var(--red)")

def rate_bar(hits, decided):
    p = pct_f(hits, decided)
    col = rate_color(p)
    return (f'<div class="rate-cell">'
            f'<div class="rate-bar-bg"><div class="rate-bar-fill" style="width:{min(p,100):.1f}%;background:{col}"></div></div>'
            f'<div class="rate-num" style="color:{col}">{pct(hits,decided)}</div></div>')

def hmv(df):
    if "Outcome" not in df.columns:
        return 0,0,len(df)
    o = df["Outcome"].astype(str).str.upper()
    return int((o=="HIT").sum()), int((o=="MISS").sum()), int((o=="VOID").sum())

def pick_chip(p):
    p = str(p).lower()
    if "goblin" in p: return '<span class="chip chip-goblin">🎃 Goblin</span>'
    if "demon"  in p: return '<span class="chip chip-demon">😈 Demon</span>'
    return '<span class="chip chip-std">⭐ Standard</span>'

def tier_chip(t):
    t = str(t).strip().upper()
    cls = {"A":"chip-a","B":"chip-b","C":"chip-c","D":"chip-d"}.get(t,"chip-d")
    return f'<span class="chip {cls}">Tier {h(t)}</span>'

def outcome_label(o):
    o = str(o).upper()
    if o=="HIT":  return '<span class="pos">✓ HIT</span>'
    if o=="MISS": return '<span class="neg">✗ MISS</span>'
    return '<span class="neu">— VOID</span>'

# ── Data loading ───────────────────────────────────────────────────────────────
def find_file(date_str, pattern):
    name = pattern % date_str
    for d in [BASE_DIR, BASE_DIR/"outputs"/date_str, BASE_DIR/"outputs",
              BASE_DIR/"NBA", BASE_DIR/"CBB", BASE_DIR/"NHL", BASE_DIR/"Soccer"]:
        p = Path(d) / name
        if p.exists(): return p
    return None

def load_xlsx(path: Path) -> pd.DataFrame:
    xf = pd.ExcelFile(path)
    frames = []
    for sheet in xf.sheet_names:
        try:
            df = pd.read_excel(path, sheet_name=sheet)
            if not df.empty: frames.append(df)
        except Exception: pass
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

def normalize(df: pd.DataFrame, sport: str) -> pd.DataFrame:
    df = df.copy()
    df.columns = [c.strip() for c in df.columns]

    # Outcome
    for col in ["Outcome","outcome","Result","result","Grade","grade","leg_result","LegResult"]:
        if col in df.columns:
            df["Outcome"] = df[col].astype(str).str.strip().str.upper()
            break
    else:
        df["Outcome"] = "VOID"

    df["Sport"] = sport

    col_map = {
        "Player":    ["Player","player","player_name","Name","name"],
        "Prop":      ["Prop","prop","prop_norm","stat_type","Prop Type","StatType"],
        "Line":      ["Line","line","line_score","PP Line","pp_line","LineScore"],
        "Dir":       ["Dir","dir","direction","Direction","pick_direction"],
        "Pick_Type": ["Pick_Type","pick_type","PickType","Pick Type","type","line_type"],
        "Tier":      ["Tier","tier","rank_tier","RankTier"],
        "Actual":    ["Actual","actual","actual_value","actual_stat","ActualValue","stat_actual"],
        "Edge":      ["Edge","edge","edge_adj","EdgeAdj"],
        "Hit_Rate":  ["Hit_Rate","hit_rate","composite_hit_rate","line_hit_rate","HitRate"],
        "Def_Tier":  ["Def_Tier","def_tier","opp_def_tier","DefTier","defense_tier"],
        "Minutes":   ["Minutes","minutes","min","MIN","minutes_played","MP"],
        "Rank_Score":["Rank_Score","rank_score","score","RankScore"],
        "Team":      ["Team","team","team_abbr"],
        "Opp":       ["Opp","opp","opponent","opp_team"],
    }
    for canon, alts in col_map.items():
        if canon not in df.columns:
            for a in alts:
                if a in df.columns:
                    df[canon] = df[a]; break

    for c in ["Line","Actual","Edge","Hit_Rate","Rank_Score","Minutes"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

# ── HTML section builders ──────────────────────────────────────────────────────
def stat_card(color, label, value, sub, val_color=None):
    vc = val_color or "var(--text)"
    return f'''<div class="stat-card {color}">
  <div class="stat-label">{label}</div>
  <div class="stat-val" style="color:{vc}">{value}</div>
  <div class="stat-sub">{sub}</div>
</div>'''

def breakdown_table(df, group_col, title):
    if group_col not in df.columns or df.empty:
        return ""
    rows = ""
    for name, g in sorted(df.groupby(group_col), key=lambda x: -pct_f(*hmv(x[1])[:2])):
        hi,mi,vo = hmv(g)
        dec = hi+mi
        if group_col == "Pick_Type":  cell = pick_chip(name)
        elif group_col == "Tier":     cell = tier_chip(name)
        elif group_col == "Dir":
            cell = (f'<span style="color:var(--green)">▲ OVER</span>' if str(name).upper()=="OVER"
                    else f'<span style="color:var(--amber)">▼ UNDER</span>')
        else: cell = h(name)
        rows += f"""<tr>
          <td>{cell}</td>
          <td class="right mono">{dec}</td>
          <td class="right mono pos">{hi}</td>
          <td class="right mono neg">{mi}</td>
          <td class="right mono neu">{vo}</td>
          <td>{rate_bar(hi,dec)}</td>
        </tr>"""
    if not rows: return ""
    return f"""<div class="section-label">{h(title)}</div>
<div class="table-wrap"><table>
  <thead><tr><th>{h(group_col.replace('_',' '))}</th>
  <th class="right">DECIDED</th><th class="right">HITS</th>
  <th class="right">MISSES</th><th class="right">VOIDS</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def prop_table(df):
    if "Prop" not in df.columns or df.empty: return ""
    rows = ""
    data = []
    for name, g in df.groupby("Prop"):
        hi,mi,vo = hmv(g)
        avg_line = g["Line"].mean() if "Line" in g.columns else float("nan")
        data.append((str(name), hi, mi, vo, hi+mi, avg_line))
    for name,hi,mi,vo,dec,avg_line in sorted(data, key=lambda x:-x[4])[:25]:
        ls = f"{avg_line:.1f}" if not pd.isna(avg_line) else "—"
        rows += f"""<tr>
          <td class="mono">{h(name)}</td>
          <td class="right mono">{ls}</td>
          <td class="right mono">{dec}</td>
          <td class="right mono pos">{hi}</td>
          <td class="right mono neg">{mi}</td>
          <td class="right mono neu">{vo}</td>
          <td>{rate_bar(hi,dec)}</td>
        </tr>"""
    if not rows: return ""
    return f"""<div class="section-label">BY PROP TYPE</div>
<div class="table-wrap"><table>
  <thead><tr><th>PROP</th><th class="right">AVG LINE</th><th class="right">DECIDED</th>
  <th class="right">HITS</th><th class="right">MISSES</th><th class="right">VOIDS</th>
  <th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def minutes_table(df):
    if "Minutes" not in df.columns or df["Minutes"].isna().all(): return ""
    buckets = [(0,10,"0–10 min"),(10,20,"10–20 min"),(20,30,"20–30 min"),
               (30,40,"30–40 min"),(40,999,"40+ min")]
    rows = ""
    for lo,hi_,label in buckets:
        g = df[(df["Minutes"]>=lo)&(df["Minutes"]<hi_)]
        if g.empty: continue
        hi,mi,vo = hmv(g)
        dec=hi+mi
        rows += f"""<tr><td class="mono">{label}</td>
          <td class="right mono">{len(g)}</td><td class="right mono">{dec}</td>
          <td class="right mono pos">{hi}</td><td class="right mono neg">{mi}</td>
          <td>{rate_bar(hi,dec)}</td></tr>"""
    if not rows: return ""
    return f"""<div class="section-label">BY MINUTES PLAYED</div>
<div class="table-wrap"><table>
  <thead><tr><th>BUCKET</th><th class="right">PROPS</th><th class="right">DECIDED</th>
  <th class="right">HITS</th><th class="right">MISSES</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def player_tables(df, min_props=3):
    if "Player" not in df.columns or df.empty: return "", ""
    data = []
    for name, g in df.groupby("Player"):
        hi,mi,vo = hmv(g)
        dec = hi+mi
        if dec < min_props: continue
        props = ", ".join(g["Prop"].dropna().unique()[:2]) if "Prop" in g.columns else ""
        data.append((str(name), hi, mi, vo, dec, props))

    def rows_html(items, is_top):
        out = ""
        cls = "player-hit" if is_top else "player-miss"
        for name,hi,mi,vo,dec,props in items[:12]:
            out += f"""<tr class="{cls}">
              <td><div style="font-weight:600;font-size:13px">{h(name)}</div>
                  <div style="font-size:11px;color:var(--muted2)">{h(props)}</div></td>
              <td class="right mono">{dec}</td>
              <td class="right mono pos">{hi}</td>
              <td class="right mono neg">{mi}</td>
              <td>{rate_bar(hi,dec)}</td>
            </tr>"""
        return out

    top_data = sorted(data, key=lambda x: -pct_f(x[1],x[4]))
    bot_data = sorted(data, key=lambda x:  pct_f(x[1],x[4]))
    thead = """<thead><tr><th>PLAYER</th><th class="right">DEC</th>
      <th class="right">HITS</th><th class="right">MISS</th><th>HIT RATE</th></tr></thead>"""

    top_r = rows_html(top_data, True)
    bot_r = rows_html(bot_data, False)
    no_data = f'<div class="muted-note">No players with ≥{min_props} decided props.</div>'

    top_html = (f'<div class="table-wrap"><table>{thead}<tbody>{top_r}</tbody></table></div>'
                if top_r else no_data)
    bot_html = (f'<div class="table-wrap"><table>{thead}<tbody>{bot_r}</tbody></table></div>'
                if bot_r else no_data)
    return top_html, bot_html

def full_legs_table(df, limit=150):
    if df.empty: return '<div class="muted-note">No leg data.</div>'
    cols = ["Player","Prop","Dir","Pick_Type","Tier","Line","Actual","Outcome","Edge","Hit_Rate"]
    avail = [c for c in cols if c in df.columns]
    right_cols = {"Line","Actual","Edge","Hit_Rate"}

    header = "".join(
        ('<th class="right">' if c in right_cols else '<th>') + h(c.replace("_"," ")) + '</th>'
        for c in avail
    )
    # Sort: HITs first, then MISSes, then VOIDs
    if "Outcome" in df.columns:
        order = {"HIT":0,"MISS":1,"VOID":2}
        df = df.copy()
        df["_sort"] = df["Outcome"].astype(str).str.upper().map(order).fillna(3)
        df = df.sort_values("_sort").drop(columns=["_sort"])
    sample = df.head(limit)

    rows = ""
    for _, row in sample.iterrows():
        outcome = str(row.get("Outcome","")).upper()
        tr_cls = "player-hit" if outcome=="HIT" else ("player-miss" if outcome=="MISS" else "")
        cells = ""
        for c in avail:
            v = row.get(c,"")
            if c == "Outcome":    cells += f"<td>{outcome_label(str(v))}</td>"
            elif c == "Dir":
                dv = str(v).upper()
                cells += f'<td><span class="{"pos" if dv=="OVER" else "neu" if dv=="UNDER" else ""}">{"▲ " if dv=="OVER" else "▼ " if dv=="UNDER" else ""}{h(v)}</span></td>'
            elif c == "Pick_Type":cells += f"<td>{pick_chip(str(v))}</td>"
            elif c == "Tier":     cells += f"<td>{tier_chip(str(v))}</td>"
            elif c in right_cols:
                try:
                    fv = float(v)
                    disp = f"{fv*100:.0f}%" if c=="Hit_Rate" else f"{fv:.1f}"
                    col = ""
                    if c=="Edge": col = f' style="color:{"var(--green)" if fv>0 else "var(--red)"}"'
                    cells += f'<td class="right mono"{col}>{disp}</td>'
                except: cells += f'<td class="right mono neu">{h(v)}</td>'
            else: cells += f"<td>{h(v)}</td>"
        rows += f'<tr class="{tr_cls}">{cells}</tr>'

    note = (f'<div style="font-size:10px;color:var(--muted);padding:8px 14px;text-align:right">'
            f'Showing {len(sample):,} of {len(df):,} legs</div>') if len(df)>limit else ""
    return f"""<div class="table-wrap" style="overflow-x:auto">
<table><thead><tr>{header}</tr></thead><tbody>{rows}</tbody></table>{note}</div>"""

def sport_section(sport, df):
    if df.empty: return ""
    hi,mi,vo = hmv(df)
    total = len(df)
    dec = hi+mi
    color = SPORT_COLORS.get(sport,"#888")
    icon  = SPORT_ICONS.get(sport,"🏟")
    hr = pct_f(hi,dec)
    hr_col = rate_color(hr)

    # KPI cards
    cards = (
        stat_card("green","OVERALL HIT RATE", pct(hi,dec), f"{hi} hits / {dec} decided", hr_col) +
        stat_card("blue","TOTAL PROPS", f"{total:,}", f"<strong>{dec:,}</strong> decided · {vo:,} voids") +
        stat_card("amber","HITS", f"{hi:,}", f"{pct(hi,dec)} hit rate", "var(--green)") +
        stat_card("red","MISSES", f"{mi:,}", f"{pct(mi,dec)} miss rate", "var(--red)")
    )

    pick_html = breakdown_table(df,"Pick_Type","BY PICK TYPE")
    dir_html  = breakdown_table(df,"Dir","BY DIRECTION")
    tier_html = breakdown_table(df,"Tier","BY TIER")
    def_html  = breakdown_table(df,"Def_Tier","BY DEFENSE TIER")
    prop_html = prop_table(df)
    min_html  = minutes_table(df)
    top_html, bot_html = player_tables(df)
    legs_html = full_legs_table(df)

    return f"""<div class="sport-section">
  <div class="sport-header">
    <div class="sport-label" style="color:{color}">{icon} {h(sport)}</div>
    <div class="sport-header-line"></div>
    <div style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted2)">
      {total:,} TOTAL PROPS &nbsp;·&nbsp; {dec:,} DECIDED &nbsp;·&nbsp; {vo:,} VOIDS
    </div>
  </div>

  <div class="section-label">OVERALL PERFORMANCE</div>
  <div class="stat-grid stat-grid-4" style="margin-bottom:20px">{cards}</div>

  <div class="two-col">
    <div>{pick_html}{dir_html}</div>
    <div>{tier_html}{def_html}</div>
  </div>

  {prop_html}
  {min_html}

  <div class="two-col">
    <div>
      <div class="section-label">🏆 TOP PERFORMERS</div>
      {top_html}
    </div>
    <div>
      <div class="section-label">💀 WORST PERFORMERS</div>
      {bot_html}
    </div>
  </div>

  <details style="margin-top:8px">
    <summary style="font-family:'DM Mono',monospace;font-size:10px;letter-spacing:2px;
      color:var(--muted);cursor:pointer;padding:10px 0;list-style:none">
      ▶ FULL LEGS TABLE ({total:,} props — click to expand)
    </summary>
    <div style="margin-top:12px">{legs_html}</div>
  </details>
</div>"""

def combined_summary(df):
    hi,mi,vo = hmv(df)
    total = len(df)
    dec = hi+mi
    hr = pct_f(hi,dec)
    hr_col = rate_color(hr)

    sport_rows = ""
    if "Sport" in df.columns:
        for sport, g in df.groupby("Sport"):
            shi,smi,svo = hmv(g)
            sdec=shi+smi
            color=SPORT_COLORS.get(str(sport).upper(),"#888")
            icon=SPORT_ICONS.get(str(sport).upper(),"🏟")
            sport_rows += f"""<tr>
              <td><span style="color:{color};font-weight:700">{icon} {h(sport)}</span></td>
              <td class="right mono">{len(g):,}</td><td class="right mono">{sdec:,}</td>
              <td class="right mono pos">{shi:,}</td><td class="right mono neg">{smi:,}</td>
              <td class="right mono neu">{svo:,}</td><td>{rate_bar(shi,sdec)}</td>
            </tr>"""

    sport_table = f"""<div class="section-label">BY SPORT</div>
<div class="table-wrap"><table>
  <thead><tr><th>SPORT</th><th class="right">TOTAL</th><th class="right">DECIDED</th>
  <th class="right">HITS</th><th class="right">MISSES</th><th class="right">VOIDS</th>
  <th>HIT RATE</th></tr></thead>
  <tbody>{sport_rows}</tbody>
</table></div>""" if sport_rows else ""

    return f"""<div class="sport-section" style="border-color:rgba(59,130,246,.3)">
  <div class="sport-header">
    <div class="sport-label" style="color:#3b82f6">📊 ALL SPORTS COMBINED</div>
    <div class="sport-header-line"></div>
    <div style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted2)">{total:,} TOTAL PROPS</div>
  </div>
  <div class="stat-grid stat-grid-4" style="margin-bottom:20px">
    {stat_card("green","COMBINED HIT RATE",pct(hi,dec),f"{hi} hits / {dec} decided",hr_col)}
    {stat_card("blue","TOTAL PROPS",f"{total:,}",f"{dec:,} decided props")}
    {stat_card("amber","TOTAL HITS",f"{hi:,}",f"{pct(hi,dec)} hit rate","var(--green)")}
    {stat_card("red","TOTAL MISSES",f"{mi:,}",f"{pct(mi,dec)} miss rate","var(--red)")}
  </div>
  {sport_table}
  <div class="two-col">
    <div>{breakdown_table(df,"Pick_Type","COMBINED PICK TYPE")}</div>
    <div>{breakdown_table(df,"Dir","COMBINED OVER / UNDER")}</div>
  </div>
</div>"""

# ── CSS (matches existing slate_eval style exactly) ────────────────────────────
CSS = """:root{--bg:#070a10;--bg2:#0c1018;--bg3:#111722;--border:#1c2333;--bd2:#243044;--text:#e8edf5;--muted:#4a5568;--muted2:#6b7a94;--blue:#3b82f6;--green:#10b981;--amber:#f59e0b;--red:#ef4444;--purple:#8b5cf6;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:60px}
body::before{content:'';position:fixed;top:-20%;left:-10%;width:55%;height:55%;background:radial-gradient(ellipse,rgba(59,130,246,.04) 0%,transparent 70%);pointer-events:none}
body::after{content:'';position:fixed;bottom:-20%;right:-10%;width:50%;height:50%;background:radial-gradient(ellipse,rgba(16,185,129,.03) 0%,transparent 70%);pointer-events:none}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:var(--bg2)}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:4px}
header{background:rgba(7,10,16,.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--border);padding:18px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.logo{display:flex;align-items:center;gap:14px}
.logo-icon{width:42px;height:42px;background:linear-gradient(135deg,#3b82f6,#10b981);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:20px;box-shadow:0 0 20px rgba(59,130,246,.3)}
.logo-title{font-family:'Bebas Neue',sans-serif;font-size:26px;letter-spacing:2px;background:linear-gradient(135deg,#fff 40%,#94a3b8);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:2.5px;margin-top:2px}
.date-badge{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted2);background:var(--bg3);border:1px solid var(--bd2);border-radius:8px;padding:6px 14px;letter-spacing:1px}
.main{max-width:1200px;margin:0 auto;padding:28px 20px}
.sport-header{display:flex;align-items:center;gap:14px;margin-bottom:22px}
.sport-label{font-family:'Bebas Neue',sans-serif;font-size:32px;letter-spacing:3px;line-height:1}
.sport-header-line{flex:1;height:1px;background:linear-gradient(90deg,var(--bd2),transparent)}
.sport-section{margin-bottom:48px;background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:24px}
.section-label{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:3px;display:flex;align-items:center;gap:10px;margin-bottom:16px;margin-top:8px}
.section-label::after{content:'';flex:1;height:1px;background:var(--border)}
.stat-grid{display:grid;gap:14px;margin-bottom:24px}
.stat-grid-4{grid-template-columns:repeat(4,1fr)}
.stat-card{background:var(--bg3);border:1px solid var(--border);border-radius:14px;padding:16px 18px;position:relative;overflow:hidden;transition:border-color .2s}
.stat-card:hover{border-color:var(--bd2)}
.stat-card::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.stat-card.green::before{background:linear-gradient(90deg,var(--green),transparent)}
.stat-card.blue::before{background:linear-gradient(90deg,var(--blue),transparent)}
.stat-card.amber::before{background:linear-gradient(90deg,var(--amber),transparent)}
.stat-card.red::before{background:linear-gradient(90deg,var(--red),transparent)}
.stat-card.purple::before{background:linear-gradient(90deg,var(--purple),transparent)}
.stat-label{font-family:'DM Mono',monospace;font-size:9px;color:var(--muted);letter-spacing:2.5px;margin-bottom:8px}
.stat-val{font-family:'Bebas Neue',sans-serif;font-size:36px;letter-spacing:1px;line-height:1}
.stat-sub{font-size:12px;color:var(--muted2);margin-top:5px}
.stat-sub strong{font-weight:700}
.table-wrap{background:var(--bg2);border:1px solid var(--border);border-radius:14px;overflow:hidden;margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{font-family:'DM Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted);padding:10px 14px;text-align:left;background:var(--bg3);border-bottom:1px solid var(--border);white-space:nowrap}
th.right{text-align:right}
td{padding:9px 14px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.015)}
td.right{text-align:right}td.mono{font-family:'DM Mono',monospace;font-size:12px}
.rate-cell{display:flex;align-items:center;gap:10px}
.rate-bar-bg{flex:1;height:5px;background:rgba(255,255,255,.06);border-radius:3px;overflow:hidden;min-width:60px}
.rate-bar-fill{height:100%;border-radius:3px}
.rate-num{font-family:'DM Mono',monospace;font-size:12px;width:44px;text-align:right;flex-shrink:0}
.chip{display:inline-block;border-radius:6px;padding:2px 9px;font-size:11px;font-weight:700;font-family:'DM Mono',monospace;letter-spacing:.5px}
.chip-a{background:rgba(16,185,129,.12);color:#6ee7b7;border:1px solid rgba(16,185,129,.25)}
.chip-b{background:rgba(59,130,246,.12);color:#93c5fd;border:1px solid rgba(59,130,246,.25)}
.chip-c{background:rgba(245,158,11,.12);color:#fcd34d;border:1px solid rgba(245,158,11,.25)}
.chip-d{background:rgba(100,116,139,.12);color:#94a3b8;border:1px solid rgba(100,116,139,.25)}
.chip-goblin{background:rgba(139,92,246,.15);color:#c4b5fd;border:1px solid rgba(139,92,246,.3)}
.chip-demon{background:rgba(239,68,68,.12);color:#fca5a5;border:1px solid rgba(239,68,68,.25)}
.chip-std{background:rgba(59,130,246,.12);color:#93c5fd;border:1px solid rgba(59,130,246,.25)}
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
.player-hit td:first-child{border-left:3px solid var(--green)}
.player-miss td:first-child{border-left:3px solid var(--red)}
.pos{color:var(--green);font-weight:700}.neg{color:var(--red);font-weight:700}.neu{color:var(--muted2)}
.muted-note{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);padding:14px;text-align:center}
details summary::-webkit-details-marker{display:none}
.footer{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);text-align:center;margin-top:40px;letter-spacing:1.5px}
@media(max-width:768px){.stat-grid-4{grid-template-columns:repeat(2,1fr)}.two-col{grid-template-columns:1fr}.sport-section{padding:16px}}"""

# ── Main builder ───────────────────────────────────────────────────────────────
def build_html(df: pd.DataFrame, date_str: str, legs_path_name: str = "") -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        display_date = d.strftime("%b %d, %Y").upper()
    except ValueError:
        display_date = date_str.upper()

    hi,mi,vo = hmv(df)
    total = len(df)

    summary = combined_summary(df)
    sections = ""
    if "Sport" in df.columns:
        for sport in ["NBA","CBB","NHL","SOCCER","MLB"]:
            sdf = df[df["Sport"]==sport]
            if not sdf.empty:
                sections += sport_section(sport, sdf)
        known = {"NBA","CBB","NHL","SOCCER","MLB"}
        for sport, sdf in df.groupby("Sport"):
            if str(sport).upper() not in known:
                sections += sport_section(str(sport).upper(), sdf)
    else:
        sections = sport_section("ALL", df)

    generated = datetime.now().strftime("%Y-%m-%d %H:%M")

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Slate Eval — {h(display_date)}</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet"/>
<style>{CSS}</style>
</head>
<body>
<header>
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <div class="logo-title">SLATE EVALUATION</div>
      <div class="logo-sub">POST-GAME GRADE REPORT</div>
    </div>
  </div>
  <div class="date-badge">📅 {h(display_date)} &nbsp;·&nbsp; {total:,} props &nbsp;·&nbsp; {hi:,} hits &nbsp;·&nbsp; {mi:,} misses &nbsp;·&nbsp; {vo:,} voids</div>
</header>
<div class="main">
  {summary}
  {sections}
  <div class="footer">GENERATED {generated}{f" &nbsp;·&nbsp; {h(legs_path_name)}" if legs_path_name else ""}</div>
</div>
</body>
</html>"""

# ── File loading ───────────────────────────────────────────────────────────────
def load_any(path: Path) -> pd.DataFrame:
    if path.suffix.lower() in (".xlsx",".xls"):
        return load_xlsx(path)
    return pd.read_csv(path, low_memory=False)

# ── CLI ────────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    type=str)
    parser.add_argument("--nba",     type=str)
    parser.add_argument("--cbb",     type=str)
    parser.add_argument("--nhl",     type=str)
    parser.add_argument("--soccer",  type=str)
    parser.add_argument("--mlb",     type=str)
    parser.add_argument("--legs",    type=str)
    parser.add_argument("--out",     type=str)
    args = parser.parse_args()

    date_str = args.date or (datetime.now()-timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Date: {date_str}")

    sport_files = {"NBA":args.nba,"CBB":args.cbb,"NHL":args.nhl,
                   "SOCCER":args.soccer,"MLB":args.mlb}
    frames = []
    names  = []
    for sport, path_str in sport_files.items():
        if not path_str: continue
        p = Path(path_str).resolve()
        if not p.exists():
            # Try relative to BASE_DIR
            p2 = BASE_DIR / path_str
            if p2.exists(): p = p2
            else:
                print(f"  WARNING: {sport} file not found: {p}"); continue
        print(f"  Loading {sport}: {p.name}")
        raw = load_any(p)
        if raw.empty: print(f"  WARNING: {sport} file is empty"); continue
        frames.append(normalize(raw, sport))
        names.append(p.name)

    if not frames and args.legs:
        p = Path(args.legs).resolve()
        if p.exists():
            raw = load_any(p)
            frames.append(normalize(raw, "ALL"))
            names.append(p.name)

    if not frames:
        for sport, pat in [("NBA","graded_nba_%s.xlsx"),("CBB","graded_cbb_%s.xlsx"),
                           ("NHL","graded_nhl_%s.xlsx"),("SOCCER","graded_soccer_%s.xlsx")]:
            p = find_file(date_str, pat)
            if p:
                print(f"  Auto-detected {sport}: {p.name}")
                raw = load_any(p)
                if not raw.empty:
                    frames.append(normalize(raw, sport))
                    names.append(p.name)

    if not frames:
        print(f"ERROR: No graded files found for {date_str}"); sys.exit(1)

    df = pd.concat(frames, ignore_index=True)
    sports = df["Sport"].nunique() if "Sport" in df.columns else 1
    print(f"  Combined: {len(df):,} rows across {sports} sport(s)")

    html = build_html(df, date_str, " · ".join(names))

    out = Path(args.out).resolve() if args.out else TEMPLATES_DIR / f"slate_eval_{date_str}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"  Saved → {out}  ({len(html):,} bytes)")
    print("  Done.")

if __name__ == "__main__":
    main()
