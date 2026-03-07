"""
build_grade_report.py
=====================
DROP THIS FILE into your SlateIQ root folder (next to run_grader.ps1).

Reads graded_nba/cbb/nhl/soccer_YYYY-MM-DD.xlsx from your outputs folder
and writes a rich HTML grade report to:
  ui_runner/ui_runner/templates/slate_eval_YYYY-MM-DD.html

This file is self-locating — it finds the SlateIQ root automatically
regardless of which subfolder you drop it in.

Usage (called automatically by run_grader.ps1):
    py -3 build_grade_report.py --date 2026-03-06 --nba outputs/2026-03-06/graded_nba_2026-03-06.xlsx ...

Manual run (auto-detects yesterday):
    py -3 build_grade_report.py
    py -3 build_grade_report.py --date 2026-03-06
"""

from __future__ import annotations
import argparse, html as html_lib, re, sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed.  pip install pandas openpyxl"); sys.exit(1)

# ── Paths ──────────────────────────────────────────────────────────────────────
# ── Path resolution ────────────────────────────────────────────────────────────
# Works whether this file lives in:
#   SlateIQ/build_grade_report.py          (root — correct location)
#   SlateIQ/grader/build_grade_report.py   (grader subfolder)
#   SlateIQ/scripts/build_grade_report.py  (scripts subfolder)
# In all cases we walk up until we find the folder containing ui_runner.

def _find_root() -> Path:
    here = Path(__file__).resolve().parent
    for candidate in [here, here.parent, here.parent.parent]:
        if (candidate / "ui_runner").exists():
            return candidate
    return here  # fallback

SCRIPT_DIR    = Path(__file__).resolve().parent
BASE_DIR      = _find_root()
TEMPLATES_DIR = BASE_DIR / "ui_runner" / "ui_runner" / "templates"

SPORT_COLORS = {
    "NBA":    "#c8ff00",
    "CBB":    "#a78bfa",
    "NHL":    "#5b9cf6",
    "SOCCER": "#39ff6e",
    "MLB":    "#f0a500",
}
SPORT_ICONS = {"NBA":"🏀","CBB":"🎓","NHL":"🏒","SOCCER":"⚽","MLB":"⚾"}

# ── Helpers ────────────────────────────────────────────────────────────────────
def h(v: Any) -> str:
    return html_lib.escape(str(v) if v is not None else "")

def pct(n: int, d: int) -> str:
    return f"{n/d*100:.1f}%" if d else "—"

def pct_f(n: int, d: int) -> float:
    return n/d*100 if d else 0.0

def rate_color(p: float) -> str:
    if p >= 75: return "#39ff6e"
    if p >= 60: return "#f0a500"
    return "#ff4d4d"

def bar_html(hit: int, total: int) -> str:
    p = pct_f(hit, total)
    col = rate_color(p)
    return (f'<div style="display:flex;align-items:center;gap:8px;min-width:120px">'
            f'<div style="flex:1;height:5px;background:rgba(255,255,255,.06);border-radius:3px;overflow:hidden">'
            f'<div style="width:{min(p,100):.1f}%;height:100%;background:{col};border-radius:3px"></div></div>'
            f'<span style="font-family:\'Share Tech Mono\',monospace;font-size:11px;color:{col};width:42px;text-align:right">'
            f'{pct(hit,total)}</span></div>')

def outcome_chip(o: str) -> str:
    o = str(o).strip().upper()
    if o == "HIT":  return '<span style="background:rgba(57,255,110,.15);color:#39ff6e;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">HIT</span>'
    if o == "MISS": return '<span style="background:rgba(255,77,77,.15);color:#ff4d4d;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">MISS</span>'
    return '<span style="background:rgba(153,153,153,.12);color:#888;padding:2px 8px;border-radius:4px;font-size:11px;font-weight:700">VOID</span>'

def dir_chip(d: str) -> str:
    d = str(d).strip().upper()
    if d == "OVER":  return '<span style="color:#39ff6e;font-size:11px">▲ OVER</span>'
    if d == "UNDER": return '<span style="color:#f0a500;font-size:11px">▼ UNDER</span>'
    return f'<span style="color:#888;font-size:11px">{h(d)}</span>'

def pick_chip(p: str) -> str:
    p = str(p).strip().lower()
    if "goblin" in p: return '<span style="background:rgba(167,139,250,.12);color:#a78bfa;padding:2px 7px;border-radius:4px;font-size:10px">👺 Goblin</span>'
    if "demon"  in p: return '<span style="background:rgba(255,77,77,.1);color:#ff8080;padding:2px 7px;border-radius:4px;font-size:10px">😈 Demon</span>'
    return '<span style="background:rgba(0,229,255,.1);color:#00e5ff;padding:2px 7px;border-radius:4px;font-size:10px">⭐ Std</span>'

def tier_chip(t: str) -> str:
    t = str(t).strip().upper()
    colors = {"A":"#39ff6e","B":"#00e5ff","C":"#f0a500","D":"#888"}
    col = colors.get(t, "#888")
    return f'<span style="background:rgba(255,255,255,.06);color:{col};padding:2px 7px;border-radius:4px;font-size:10px;font-weight:700">T{h(t)}</span>'

# ── Data loading ───────────────────────────────────────────────────────────────
def find_file(date_str: str, pattern: str) -> Path | None:
    """Search common locations for a file matching pattern % date_str."""
    name = pattern % date_str
    search_dirs = [
        SCRIPT_DIR,
        BASE_DIR,
        BASE_DIR / "outputs" / date_str,
        BASE_DIR / "outputs",
        BASE_DIR / "NBA",
        BASE_DIR / "CBB",
        BASE_DIR / "NHL",
        BASE_DIR / "Soccer",
        BASE_DIR / "grader",
        BASE_DIR / "Grader",
        TEMPLATES_DIR,
    ]
    for d in search_dirs:
        p = Path(d) / name
        if p.exists():
            return p
    # Glob fallback
    for d in search_dirs:
        matches = list(Path(d).glob(f"*{date_str}*")) if Path(d).exists() else []
        for m in matches:
            if pattern.split("%")[0].replace("*","").strip("_") in m.name.lower():
                return m
    return None

def load_legs_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize an already-loaded DataFrame the same way load_legs does."""
    import io
    tmp = df.to_csv(index=False)
    import tempfile, os
    with tempfile.NamedTemporaryFile(mode='w', suffix='.csv', delete=False, encoding='utf-8') as f:
        f.write(tmp)
        fname = f.name
    result = load_legs(Path(fname))
    os.unlink(fname)
    return result


def load_legs(path: Path) -> pd.DataFrame:
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    # Normalize outcome column
    for col in ["Outcome","outcome","Result","result","Grade","grade"]:
        if col in df.columns:
            df["Outcome"] = df[col].astype(str).str.strip().str.upper()
            break
    else:
        df["Outcome"] = "VOID"

    # Normalize sport column
    for col in ["Sport","sport","SPORT"]:
        if col in df.columns:
            df["Sport"] = df[col].astype(str).str.strip().str.upper()
            break

    # Normalize key columns with fallbacks
    col_map = {
        "Player":     ["Player","player","PLAYER","player_name","Player Name"],
        "Prop":       ["Prop","prop","PROP","prop_norm","Prop Type","stat_type"],
        "Line":       ["Line","line","LINE","line_score","PP Line"],
        "Dir":        ["Dir","dir","DIR","direction","Direction"],
        "Pick_Type":  ["Pick_Type","pick_type","PickType","Pick Type","type"],
        "Tier":       ["Tier","tier","TIER"],
        "Actual":     ["Actual","actual","ACTUAL","actual_value","actual_stat","Result Value"],
        "Edge":       ["Edge","edge","EDGE","edge_adj"],
        "Hit_Rate":   ["Hit_Rate","hit_rate","HitRate","Hit Rate","line_hit_rate","composite_hit_rate"],
        "Def_Tier":   ["Def_Tier","def_tier","DefTier","Def Tier","opp_def_tier","defense_tier"],
        "Minutes":    ["Minutes","minutes","Min","min","MIN","minutes_played"],
        "Rank_Score": ["Rank_Score","rank_score","RankScore","Rank Score","score"],
        "Team":       ["Team","team","TEAM"],
        "Opp":        ["Opp","opp","OPP","opponent","Opponent"],
    }
    for canon, alts in col_map.items():
        if canon not in df.columns:
            for a in alts:
                if a in df.columns:
                    df[canon] = df[a]
                    break

    # Ensure numeric
    for c in ["Line","Actual","Edge","Hit_Rate","Rank_Score","Minutes"]:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")

    return df

def load_tickets(path: Path | None) -> pd.DataFrame:
    if path is None or not path.exists():
        return pd.DataFrame()
    df = pd.read_csv(path, low_memory=False)
    df.columns = [c.strip() for c in df.columns]
    for col in ["Outcome","outcome","Result","result","ticket_result","Ticket_Result"]:
        if col in df.columns:
            df["Outcome"] = df[col].astype(str).str.strip().str.upper()
            break
    return df

# ── Section builders ───────────────────────────────────────────────────────────
def hmv(df: pd.DataFrame) -> tuple[int,int,int]:
    o = df["Outcome"] if "Outcome" in df.columns else pd.Series(dtype=str)
    hits   = int((o == "HIT").sum())
    misses = int((o == "MISS").sum())
    voids  = int((o == "VOID").sum())
    return hits, misses, voids

def build_kpi_cards(hits:int, misses:int, voids:int, total:int, label:str="OVERALL") -> str:
    decided = hits + misses
    hr = pct_f(hits, decided)
    col = rate_color(hr)
    return f"""
<div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px">
  <div class="sc green">
    <div class="sl">{label} HIT RATE</div>
    <div class="sv" style="color:{col}">{pct(hits,decided)}</div>
    <div class="ss">{hits} hits / {decided} decided</div>
  </div>
  <div class="sc blue">
    <div class="sl">HITS</div>
    <div class="sv" style="color:#39ff6e">{hits:,}</div>
    <div class="ss">of {decided:,} decided props</div>
  </div>
  <div class="sc red">
    <div class="sl">MISSES</div>
    <div class="sv" style="color:#ff4d4d">{misses:,}</div>
    <div class="ss">{pct(misses,decided)} miss rate</div>
  </div>
  <div class="sc amber">
    <div class="sl">VOIDS</div>
    <div class="sv" style="color:#888">{voids:,}</div>
    <div class="ss">{pct(voids,total)} of {total:,} total</div>
  </div>
</div>"""

def build_breakdown_table(df: pd.DataFrame, group_col: str, label: str) -> str:
    if group_col not in df.columns or df.empty:
        return f'<div class="mn">No {label.lower()} data.</div>'
    rows = ""
    groups = df.groupby(group_col)
    data = []
    for name, g in groups:
        h_,m_,v_ = hmv(g)
        dec = h_+m_
        data.append((str(name), h_, m_, v_, dec))
    data.sort(key=lambda x: -pct_f(x[1],x[4]) if x[4] else -1)
    for name, h_, m_, v_, dec in data:
        row_col = ""
        if group_col == "Pick_Type":
            row_col = pick_chip(name)
        elif group_col == "Tier":
            row_col = tier_chip(name)
        elif group_col == "Dir":
            row_col = dir_chip(name)
        elif group_col == "Def_Tier":
            dtc = {"Elite":"#39ff6e","Strong":"#00e5ff","Average":"#f0a500","Weak":"#ff8080","Very Weak":"#ff4d4d"}
            col = dtc.get(name, "#888")
            row_col = f'<span style="color:{col};font-size:11px">{h(name)}</span>'
        else:
            row_col = h(name)
        rows += f"""<tr>
          <td>{row_col}</td>
          <td class="r mono">{dec}</td>
          <td class="r pos">{h_}</td>
          <td class="r neg">{m_}</td>
          <td class="r muted">{v_}</td>
          <td>{bar_html(h_, dec)}</td>
        </tr>"""
    if not rows:
        return f'<div class="mn">No {label.lower()} data.</div>'
    return f"""<div class="section-label">{h(label)}</div>
<div class="tw"><table>
  <thead><tr><th>{h(group_col.replace('_',' '))}</th><th class="r">DECIDED</th>
  <th class="r">HITS</th><th class="r">MISSES</th><th class="r">VOIDS</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def build_prop_breakdown(df: pd.DataFrame) -> str:
    if "Prop" not in df.columns or df.empty:
        return '<div class="mn">No prop data.</div>'
    rows = ""
    data = []
    for name, g in df.groupby("Prop"):
        h_,m_,v_ = hmv(g)
        dec = h_+m_
        avg_line = g["Line"].mean() if "Line" in g.columns else float("nan")
        data.append((str(name), h_, m_, v_, dec, avg_line))
    data.sort(key=lambda x: -x[4])  # sort by volume
    for name, h_, m_, v_, dec, avg_line in data[:30]:
        line_str = f"{avg_line:.1f}" if not pd.isna(avg_line) else "—"
        rows += f"""<tr>
          <td class="mono">{h(name)}</td>
          <td class="r mono muted">{line_str}</td>
          <td class="r">{dec}</td>
          <td class="r pos">{h_}</td>
          <td class="r neg">{m_}</td>
          <td class="r muted">{v_}</td>
          <td>{bar_html(h_, dec)}</td>
        </tr>"""
    return f"""<div class="section-label">BY PROP TYPE</div>
<div class="tw"><table>
  <thead><tr><th>PROP</th><th class="r">AVG LINE</th><th class="r">DECIDED</th>
  <th class="r">HITS</th><th class="r">MISSES</th><th class="r">VOIDS</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def build_player_table(df: pd.DataFrame, top: bool = True, min_props: int = 3) -> str:
    if "Player" not in df.columns or df.empty:
        return f'<div class="mn">No player data.</div>'
    data = []
    for name, g in df.groupby("Player"):
        h_,m_,v_ = hmv(g)
        dec = h_+m_
        if dec < min_props: continue
        sport = g["Sport"].iloc[0] if "Sport" in g.columns else ""
        props = ", ".join(g["Prop"].dropna().unique()[:3]) if "Prop" in g.columns else ""
        data.append((str(name), h_, m_, v_, dec, str(sport), props))
    data.sort(key=lambda x: -pct_f(x[1],x[4]) if x[4] else 0, reverse=not top)
    # For top: highest HR. For bottom: lowest HR
    if top:
        data.sort(key=lambda x: (-pct_f(x[1],x[4]) if x[4] else 0))
    else:
        data.sort(key=lambda x: (pct_f(x[1],x[4]) if x[4] else 100))
    label = "🏆 TOP PERFORMERS" if top else "💀 WORST PERFORMERS"
    rows = ""
    for name, h_, m_, v_, dec, sport, props in data[:15]:
        sc = SPORT_COLORS.get(sport, "#888")
        sport_chip = f'<span style="background:rgba(0,0,0,.3);color:{sc};font-size:10px;padding:1px 6px;border-radius:4px;border:1px solid {sc}44">{h(sport)}</span>'
        rows += f"""<tr class="{'player-hit' if top else 'player-miss'}">
          <td><div style="font-weight:700">{h(name)}</div><div style="font-size:10px;color:#666;margin-top:2px">{h(props)}</div></td>
          <td>{sport_chip}</td>
          <td class="r">{dec}</td>
          <td class="r pos">{h_}</td>
          <td class="r neg">{m_}</td>
          <td>{bar_html(h_,dec)}</td>
        </tr>"""
    if not rows:
        return f'<div class="mn">No players with ≥{min_props} decided props.</div>'
    return f"""<div class="section-label">{label}</div>
<div class="tw"><table>
  <thead><tr><th>PLAYER</th><th>SPORT</th><th class="r">DEC</th>
  <th class="r">HITS</th><th class="r">MISS</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def build_minutes_breakdown(df: pd.DataFrame) -> str:
    """Break down hit rates by minutes bucket."""
    if "Minutes" not in df.columns or df["Minutes"].isna().all():
        return ""
    buckets = [(0,10,"0–10 min"),(10,20,"10–20 min"),(20,30,"20–30 min"),(30,40,"30–40 min"),(40,999,"40+ min")]
    rows = ""
    for lo, hi, label in buckets:
        g = df[(df["Minutes"] >= lo) & (df["Minutes"] < hi)]
        if g.empty: continue
        h_,m_,v_ = hmv(g)
        dec = h_+m_
        rows += f"""<tr>
          <td class="mono">{h(label)}</td>
          <td class="r">{len(g)}</td><td class="r">{dec}</td>
          <td class="r pos">{h_}</td><td class="r neg">{m_}</td>
          <td>{bar_html(h_,dec)}</td>
        </tr>"""
    if not rows:
        return ""
    return f"""<div class="section-label">BY MINUTES PLAYED</div>
<div class="tw"><table>
  <thead><tr><th>MINUTES BUCKET</th><th class="r">PROPS</th><th class="r">DECIDED</th>
  <th class="r">HITS</th><th class="r">MISSES</th><th>HIT RATE</th></tr></thead>
  <tbody>{rows}</tbody>
</table></div>"""

def build_full_legs_table(df: pd.DataFrame, limit: int = 200) -> str:
    if df.empty:
        return '<div class="mn">No leg data available.</div>'
    show_cols = ["Player","Sport","Prop","Dir","Pick_Type","Tier","Line","Actual","Outcome","Edge","Hit_Rate","Rank_Score"]
    avail = [c for c in show_cols if c in df.columns]
    right_cols = ("Line","Actual","Edge","Hit_Rate","Rank_Score")
    header = "".join(
        ('<th class="r">' if c in right_cols else '<th>') + h(c.replace("_"," ")) + '</th>'
        for c in avail
    )
    rows = ""
    sample = df.sample(min(limit, len(df)), random_state=42) if len(df) > limit else df
    sample = sample.sort_values("Outcome", key=lambda s: s.map({"HIT":0,"MISS":1,"VOID":2}), na_position="last") if "Outcome" in sample.columns else sample
    for _, row in sample.iterrows():
        outcome = str(row.get("Outcome",""))
        tr_class = {"HIT":"player-hit","MISS":"player-miss"}.get(outcome,"")
        cells = ""
        for c in avail:
            v = row.get(c,"")
            if c == "Outcome":    cells += f"<td>{outcome_chip(str(v))}</td>"
            elif c == "Dir":      cells += f"<td>{dir_chip(str(v))}</td>"
            elif c == "Pick_Type":cells += f"<td>{pick_chip(str(v))}</td>"
            elif c == "Tier":     cells += f"<td>{tier_chip(str(v))}</td>"
            elif c == "Sport":
                sc = SPORT_COLORS.get(str(v).upper(), "#888")
                cells += f'<td><span style="color:{sc};font-size:10px;font-weight:700">{h(v)}</span></td>'
            elif c in ("Line","Actual","Edge","Hit_Rate","Rank_Score"):
                try:
                    fv = float(v)
                    disp = f"{fv:.2f}" if c in ("Edge","Rank_Score") else (f"{fv*100:.0f}%" if c=="Hit_Rate" else f"{fv:.1f}")
                    col = ""
                    if c == "Edge":     col = f' style="color:{"#39ff6e" if fv>0 else "#ff4d4d"};font-weight:600"'
                    elif c == "Outcome" and outcome=="HIT": col = ' style="color:#39ff6e"'
                    cells += f'<td class="r mono"{col}>{disp}</td>'
                except (TypeError, ValueError):
                    cells += f'<td class="r mono muted">{h(v)}</td>'
            else:
                cells += f"<td>{h(v)}</td>"
        rows += f'<tr class="{tr_class}">{cells}</tr>'
    note = f'<div style="font-size:10px;color:#555;padding:6px 12px;text-align:right">Showing {len(sample)} of {len(df)} legs</div>' if len(df) > limit else ""
    return f"""<div class="tw" style="overflow-x:auto"><table>
  <thead><tr>{header}</tr></thead>
  <tbody>{rows}</tbody>
</table>{note}</div>"""

def build_ticket_summary(tdf: pd.DataFrame) -> str:
    if tdf.empty:
        return ""
    h_, m_, v_ = hmv(tdf)
    dec = h_ + m_
    total = len(tdf)
    hr = pct_f(h_, dec)
    col = rate_color(hr)
    # By ticket type if column exists
    type_breakdown = ""
    for tc in ["Ticket_Type","ticket_type","Type","type","Leg_Count","leg_count"]:
        if tc in tdf.columns:
            tb_rows = ""
            for name, g in tdf.groupby(tc):
                th, tm, tv = hmv(g)
                td_ = th + tm
                tb_rows += f"""<tr>
                  <td class="mono">{h(name)}</td>
                  <td class="r">{len(g)}</td>
                  <td class="r pos">{th}</td><td class="r neg">{tm}</td><td class="r muted">{tv}</td>
                  <td>{bar_html(th,td_)}</td>
                </tr>"""
            if tb_rows:
                type_breakdown = f"""<div class="section-label">BY TICKET TYPE</div>
<div class="tw"><table>
  <thead><tr><th>TYPE</th><th class="r">TOTAL</th><th class="r">HITS</th><th class="r">MISSES</th><th class="r">VOIDS</th><th>HIT RATE</th></tr></thead>
  <tbody>{tb_rows}</tbody>
</table></div>"""
            break

    return f"""<div class="sport-section">
  <div class="sport-header">
    <div class="sport-label" style="color:#f0a500">🎟 TICKET RESULTS</div>
    <div class="sport-header-line"></div>
    <div class="sport-meta">{total:,} TICKETS</div>
  </div>
  <div style="display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:24px">
    <div class="sc amber">
      <div class="sl">TICKET HIT RATE</div>
      <div class="sv" style="color:{col}">{pct(h_,dec)}</div>
      <div class="ss">{h_} hits / {dec} decided</div>
    </div>
    <div class="sc green">
      <div class="sl">WINNING TICKETS</div>
      <div class="sv" style="color:#39ff6e">{h_:,}</div>
      <div class="ss">All legs hit</div>
    </div>
    <div class="sc red">
      <div class="sl">LOSING TICKETS</div>
      <div class="sv" style="color:#ff4d4d">{m_:,}</div>
      <div class="ss">At least 1 miss</div>
    </div>
    <div class="sc">
      <div class="sl">VOID TICKETS</div>
      <div class="sv" style="color:#888">{v_:,}</div>
      <div class="ss">{pct(v_,total)} of total</div>
    </div>
  </div>
  {type_breakdown}
</div>"""

def build_sport_section(sport: str, df: pd.DataFrame) -> str:
    if df.empty:
        return ""
    hits, misses, voids = hmv(df)
    total = len(df)
    decided = hits + misses
    color = SPORT_COLORS.get(sport, "#888")
    icon  = SPORT_ICONS.get(sport, "🏟")
    hr = pct_f(hits, decided)
    col = rate_color(hr)

    # All sub-breakdowns
    pick_type_html = build_breakdown_table(df, "Pick_Type", "BY PICK TYPE (GOBLIN / STANDARD / DEMON)")
    dir_html       = build_breakdown_table(df, "Dir",       "BY DIRECTION (OVER / UNDER)")
    tier_html      = build_breakdown_table(df, "Tier",      "BY TIER")
    def_html       = build_breakdown_table(df, "Def_Tier",  "BY OPPONENT DEFENSE TIER")
    prop_html      = build_prop_breakdown(df)
    min_html       = build_minutes_breakdown(df)
    top_html       = build_player_table(df, top=True)
    bot_html       = build_player_table(df, top=False)
    full_html      = build_full_legs_table(df)

    # Over/under insight
    over_df  = df[df.get("Dir","").str.upper() == "OVER"]  if "Dir" in df.columns else pd.DataFrame()
    under_df = df[df.get("Dir","").str.upper() == "UNDER"] if "Dir" in df.columns else pd.DataFrame()
    oh, om, _ = hmv(over_df) if not over_df.empty else (0,0,0)
    uh, um, _ = hmv(under_df) if not under_df.empty else (0,0,0)
    insight_over  = pct_f(oh, oh+om) if oh+om else 0
    insight_under = pct_f(uh, uh+um) if uh+um else 0

    return f"""<div class="sport-section">
  <div class="sport-header">
    <div class="sport-label" style="color:{color}">{icon} {h(sport)}</div>
    <div class="sport-header-line"></div>
    <div class="sport-meta">{total:,} TOTAL PROPS &nbsp;·&nbsp; {decided:,} DECIDED &nbsp;·&nbsp; {voids:,} VOIDS</div>
  </div>

  <div class="section-label">OVERALL PERFORMANCE</div>
  {build_kpi_cards(hits, misses, voids, total, sport)}

  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">
    <div>
      {pick_type_html}
      {dir_html}
    </div>
    <div>
      {tier_html}
      {def_html}
    </div>
  </div>

  {prop_html}
  {min_html}

  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:24px">
    <div>{top_html}</div>
    <div>{bot_html}</div>
  </div>

  <details style="margin-bottom:24px">
    <summary style="font-family:'Share Tech Mono',monospace;font-size:10px;letter-spacing:2px;
      color:#666;cursor:pointer;padding:10px 0;list-style:none;display:flex;align-items:center;gap:8px">
      <span style="color:#c8ff00">▶</span> FULL LEGS TABLE ({total:,} props · click to expand)
    </summary>
    <div style="margin-top:12px">{full_html}</div>
  </details>
</div>"""

def build_summary_section(df: pd.DataFrame) -> str:
    """Cross-sport combined summary at the top."""
    hits, misses, voids = hmv(df)
    total = len(df)
    decided = hits + misses

    sport_rows = ""
    if "Sport" in df.columns:
        for sport, g in df.groupby("Sport"):
            h_, m_, v_ = hmv(g)
            dec = h_ + m_
            color = SPORT_COLORS.get(str(sport).upper(), "#888")
            icon  = SPORT_ICONS.get(str(sport).upper(), "🏟")
            sport_rows += f"""<tr>
              <td><span style="color:{color};font-weight:700">{icon} {h(sport)}</span></td>
              <td class="r">{len(g):,}</td>
              <td class="r">{dec:,}</td>
              <td class="r pos">{h_:,}</td>
              <td class="r neg">{m_:,}</td>
              <td class="r muted">{v_:,}</td>
              <td>{bar_html(h_,dec)}</td>
            </tr>"""

    sport_table = f"""<div class="section-label">BY SPORT</div>
<div class="tw"><table>
  <thead><tr><th>SPORT</th><th class="r">TOTAL</th><th class="r">DECIDED</th>
  <th class="r">HITS</th><th class="r">MISSES</th><th class="r">VOIDS</th><th>HIT RATE</th></tr></thead>
  <tbody>{sport_rows}</tbody>
</table></div>""" if sport_rows else ""

    pick_html = build_breakdown_table(df, "Pick_Type", "COMBINED PICK TYPE BREAKDOWN")
    dir_html  = build_breakdown_table(df, "Dir",       "COMBINED OVER / UNDER")

    return f"""<div class="sport-section" style="border-color:#c8ff0033">
  <div class="sport-header">
    <div class="sport-label" style="color:#c8ff00">📊 COMBINED SLATE</div>
    <div class="sport-header-line"></div>
    <div class="sport-meta">{total:,} TOTAL PROPS</div>
  </div>
  <div class="section-label">ALL SPORTS COMBINED</div>
  {build_kpi_cards(hits, misses, voids, total, "COMBINED")}
  {sport_table}
  <div style="display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:8px">
    <div>{pick_html}</div>
    <div>{dir_html}</div>
  </div>
</div>"""

# ── CSS ────────────────────────────────────────────────────────────────────────
CSS = """
@import url('https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Share+Tech+Mono&display=swap');
:root{
  --bg:#05050f;--bg2:#0d0d1f;--bg3:#111128;--border:#1e1e3a;--bd2:#2a2a4a;
  --text:#e8e8f0;--muted:#888;--muted2:#555;
  --accent:#c8ff00;--cyan:#00e5ff;
  --green:#39ff6e;--amber:#f0a500;--red:#ff4d4d;--purple:#a78bfa;
}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'Share Tech Mono',monospace;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:60px}
body::before{content:'';position:fixed;inset:0;background-image:linear-gradient(rgba(200,255,0,.03) 1px,transparent 1px),linear-gradient(90deg,rgba(200,255,0,.03) 1px,transparent 1px);background-size:40px 40px;pointer-events:none;z-index:0}
::-webkit-scrollbar{width:4px;height:4px}::-webkit-scrollbar-track{background:var(--bg2)}::-webkit-scrollbar-thumb{background:var(--bd2)}

.main{position:relative;z-index:1;max-width:1300px;margin:0 auto;padding:24px 20px}
.page-title{font-family:'Bebas Neue',sans-serif;font-size:clamp(28px,4vw,42px);letter-spacing:.08em;color:var(--accent);margin-bottom:4px}
.page-sub{font-size:10px;color:var(--muted);letter-spacing:2.5px;margin-bottom:20px}
.date-chip{display:inline-block;font-size:10px;color:var(--muted);background:var(--bg2);border:1px solid var(--border);border-radius:6px;padding:4px 12px;letter-spacing:1px;margin-right:8px}

/* stat cards */
.sc{background:var(--bg2);border:1px solid var(--border);border-radius:12px;padding:14px 16px;position:relative;overflow:hidden}
.sc::before{content:'';position:absolute;top:0;left:0;right:0;height:2px}
.sc.green::before{background:linear-gradient(90deg,var(--green),transparent)}
.sc.blue::before{background:linear-gradient(90deg,var(--cyan),transparent)}
.sc.amber::before{background:linear-gradient(90deg,var(--amber),transparent)}
.sc.red::before{background:linear-gradient(90deg,var(--red),transparent)}
.sc.purple::before{background:linear-gradient(90deg,var(--purple),transparent)}
.sl{font-size:9px;color:var(--muted);letter-spacing:2.5px;margin-bottom:6px}
.sv{font-family:'Bebas Neue',sans-serif;font-size:32px;letter-spacing:1px;line-height:1}
.ss{font-size:11px;color:var(--muted2);margin-top:4px}

/* sport section */
.sport-section{background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:20px;margin-bottom:28px}
.sport-header{display:flex;align-items:center;gap:12px;margin-bottom:18px}
.sport-label{font-family:'Bebas Neue',sans-serif;font-size:28px;letter-spacing:.08em;line-height:1}
.sport-header-line{flex:1;height:1px;background:linear-gradient(90deg,var(--bd2),transparent)}
.sport-meta{font-size:10px;color:var(--muted);letter-spacing:1px}

/* section labels */
.section-label{font-size:9px;color:var(--muted);letter-spacing:2.5px;display:flex;align-items:center;gap:10px;margin:16px 0 10px}
.section-label::after{content:'';flex:1;height:1px;background:var(--border)}

/* tables */
.tw{background:var(--bg3);border:1px solid var(--border);border-radius:10px;overflow:hidden;margin-bottom:16px;overflow-x:auto}
table{width:100%;border-collapse:collapse;font-size:12px}
th{font-size:9px;letter-spacing:1.5px;color:var(--accent);padding:8px 10px;text-align:left;background:rgba(200,255,0,.03);border-bottom:1px solid var(--border);white-space:nowrap;font-family:'Bebas Neue',sans-serif}
th.r{text-align:right}
td{padding:7px 10px;border-bottom:1px solid rgba(255,255,255,.03);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(200,255,0,.015)}
td.r{text-align:right}td.mono{font-family:'Share Tech Mono',monospace;font-size:11px}
td.muted{color:var(--muted2)}td.pos{color:var(--green);font-weight:600}td.neg{color:var(--red);font-weight:600}

.player-hit td:first-child{border-left:2px solid var(--green)}
.player-miss td:first-child{border-left:2px solid var(--red)}
.mn{font-size:11px;color:var(--muted2);padding:14px;text-align:center}

details summary::-webkit-details-marker{display:none}
details[open] summary span:first-of-type{transform:rotate(90deg);display:inline-block}

.footer{font-size:9px;color:var(--muted2);text-align:center;margin-top:40px;letter-spacing:1.5px}

@media(max-width:768px){
  .sport-section{padding:14px}
  .sv{font-size:24px}
}
"""

# ── Main HTML builder ──────────────────────────────────────────────────────────
def _build_html_core(df: pd.DataFrame, tdf: pd.DataFrame, date_str: str, legs_path, tickets_path) -> str:
    generated = datetime.now().strftime("%Y-%m-%d %H:%M")
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        display_date = d.strftime("%b %d, %Y").upper()
    except ValueError:
        display_date = date_str.upper()

    # Build sections
    summary_html  = build_summary_section(df)
    ticket_html   = build_ticket_summary(tdf)

    sport_sections = ""
    if "Sport" in df.columns:
        for sport in ["NBA","CBB","NHL","SOCCER","MLB"]:
            sdf = df[df["Sport"] == sport]
            if not sdf.empty:
                sport_sections += build_sport_section(sport, sdf)
        # Any other sport not in list
        known = {"NBA","CBB","NHL","SOCCER","MLB"}
        for sport, sdf in df.groupby("Sport"):
            if str(sport).upper() not in known and not sdf.empty:
                sport_sections += build_sport_section(str(sport).upper(), sdf)
    else:
        sport_sections = build_sport_section("ALL", df)

    total = len(df)
    hits, misses, voids = hmv(df)

    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>SlateIQ Grade Report — {h(display_date)}</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=Share+Tech+Mono&display=swap" rel="stylesheet"/>
<style>{CSS}</style>
</head>
<body>
<div class="main">
  <div class="page-title">📊 SLATE GRADE REPORT</div>
  <div class="page-sub">POST-GAME EVALUATION · ALL SPORTS</div>
  <div style="margin-bottom:20px">
    <span class="date-chip">📅 {h(display_date)}</span>
    <span class="date-chip">📋 {total:,} PROPS GRADED</span>
    <span class="date-chip" style="color:#39ff6e">✅ {hits:,} HITS</span>
    <span class="date-chip" style="color:#ff4d4d">❌ {misses:,} MISSES</span>
    <span class="date-chip">⬜ {voids:,} VOIDS</span>
  </div>

  {summary_html}
  {ticket_html}
  {sport_sections}

  <div class="footer">
    SLATEIQ GRADE REPORT &nbsp;·&nbsp; GENERATED {generated}
    &nbsp;·&nbsp; {h(legs_path.name)}
    {f'&nbsp;·&nbsp; {h(tickets_path.name)}' if tickets_path else ''}
  </div>
</div>
</body>
</html>"""

# ── Convenience wrappers ───────────────────────────────────────────────────────
def build_html_from_df(df: pd.DataFrame, tickets_path, date_str: str, legs_path) -> str:
    tdf = load_tickets(tickets_path)
    if not tdf.empty:
        print(f"  Loaded {len(tdf)} ticket rows.")
    return _build_html_core(df, tdf, date_str, legs_path, tickets_path)

def build_html(legs_path: Path, tickets_path, date_str: str) -> str:
    print(f"  Loading legs: {legs_path}")
    df = load_legs(legs_path)
    print(f"  Loaded {len(df)} leg rows.")
    tdf = load_tickets(tickets_path)
    return _build_html_core(df, tdf, date_str, legs_path, tickets_path)

# ── CLI ────────────────────────────────────────────────────────────────────────
def load_any(path: Path) -> pd.DataFrame:
    """Load CSV or XLSX, return normalized DataFrame."""
    if path.suffix.lower() in (".xlsx", ".xls"):
        # Try each sheet, combine all rows
        xf = pd.ExcelFile(path)
        frames = []
        for sheet in xf.sheet_names:
            try:
                df = pd.read_excel(path, sheet_name=sheet)
                if not df.empty:
                    frames.append(df)
            except Exception:
                pass
        if not frames:
            return pd.DataFrame()
        return pd.concat(frames, ignore_index=True)
    else:
        return pd.read_csv(path, low_memory=False)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    type=str, help="YYYY-MM-DD (default: yesterday)")
    # Per-sport graded xlsx (matches run_grader.ps1 output paths)
    parser.add_argument("--nba",     type=str, help="graded_nba_DATE.xlsx")
    parser.add_argument("--cbb",     type=str, help="graded_cbb_DATE.xlsx")
    parser.add_argument("--nhl",     type=str, help="graded_nhl_DATE.xlsx")
    parser.add_argument("--soccer",  type=str, help="graded_soccer_DATE.xlsx")
    parser.add_argument("--mlb",     type=str, help="graded_mlb_DATE.xlsx")
    # Generic fallbacks
    parser.add_argument("--legs",    type=str, help="graded_legs CSV (any sport combined)")
    parser.add_argument("--tickets", type=str, help="graded_tickets CSV (optional)")
    parser.add_argument("--out",     type=str, help="Output HTML path")
    args = parser.parse_args()

    date_str = args.date or (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  Date: {date_str}")

    # Build combined legs DataFrame from all provided sport files
    sport_files = {
        "NBA": args.nba, "CBB": args.cbb, "NHL": args.nhl,
        "SOCCER": args.soccer, "MLB": args.mlb
    }
    frames = []
    for sport, path_str in sport_files.items():
        if not path_str:
            continue
        p = Path(path_str).resolve()
        if not p.exists():
            print(f"  WARNING: {sport} file not found: {p}")
            continue
        print(f"  Loading {sport}: {p.name}")
        df = load_any(p)
        if df.empty:
            print(f"  WARNING: {sport} file is empty")
            continue
        # Normalize legs via load_legs
        # write temp CSV so load_legs can handle it
        df2 = load_legs_df(df)
        if "Sport" not in df2.columns or df2["Sport"].isna().all():
            df2["Sport"] = sport
        else:
            df2["Sport"] = df2["Sport"].fillna(sport)
        frames.append(df2)

    # Fallback: --legs CSV
    if not frames and args.legs:
        p = Path(args.legs).resolve()
        if p.exists():
            frames.append(load_legs(p))

    # Auto-discover if nothing provided
    if not frames:
        for sport, pattern in [("NBA","graded_nba_%s.xlsx"),("CBB","graded_cbb_%s.xlsx"),
                                ("NHL","graded_nhl_%s.xlsx"),("SOCCER","graded_soccer_%s.xlsx")]:
            p = find_file(date_str, pattern)
            if p:
                print(f"  Auto-detected {sport}: {p.name}")
                df = load_any(p)
                if not df.empty:
                    df2 = load_legs_df(df)
                    df2["Sport"] = sport
                    frames.append(df2)
        if not frames:
            p = find_file(date_str, "graded_legs_%s.csv")
            if p:
                frames.append(load_legs(p))

    if not frames:
        print(f"ERROR: No graded files found for {date_str}")
        print("  Pass --nba, --cbb, --nhl, --soccer flags pointing to your graded xlsx files")
        sys.exit(1)

    combined = pd.concat(frames, ignore_index=True)
    print(f"  Combined: {len(combined)} rows across {combined['Sport'].nunique() if 'Sport' in combined.columns else 1} sports")

    tickets_path = Path(args.tickets).resolve() if args.tickets else find_file(date_str, "graded_tickets_%s.csv")

    # Use a fake path object for footer display
    class FakePath:
        name = f"graded_*_{date_str}.xlsx"
    
    html = build_html_from_df(combined, tickets_path, date_str, FakePath())
    out = Path(args.out).resolve() if args.out else TEMPLATES_DIR / f"slate_eval_{date_str}.html"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"  Saved → {out}  ({len(html):,} bytes)")
    print("  Done.")

if __name__ == "__main__":
    main()
