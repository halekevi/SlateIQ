"""
build_grades_html.py  (v2 — summary-xlsx parser)
=================================================
Reads the pre-aggregated nba_graded_*.xlsx / cbb_graded_*.xlsx files that
have section headers + stats rows (not raw prop-per-row data) and produces
a styled slate_eval_{date}.html matching the full visual report style.

Usage:
    python build_grades_html.py --date 2026-02-26
    python build_grades_html.py --nba nba_graded_2026-02-26.xlsx --cbb cbb_graded_2026-02-26.xlsx
    python build_grades_html.py --nba nba_graded.xlsx            (NBA only)
"""

from __future__ import annotations
import argparse, html as html_lib, sys
from datetime import datetime
from pathlib import Path

try:
    import openpyxl
except ImportError:
    print("ERROR: openpyxl not installed. Run: pip install openpyxl")
    sys.exit(1)

SCRIPT_DIR    = Path(__file__).resolve().parent
TEMPLATES_DIR = SCRIPT_DIR / "ui_runner" / "templates"

# ── Load xlsx → list of rows ──────────────────────────────────────────────────
def load_rows(path: Path) -> list[tuple]:
    wb = openpyxl.load_workbook(path, read_only=True, data_only=True)
    ws = wb.active
    return [tuple(r) for r in ws.iter_rows(values_only=True)]

def safe_float(v) -> float | None:
    try:
        f = float(v)
        return f if 0 <= f <= 1 else None
    except (TypeError, ValueError):
        return None

def safe_int(v) -> int:
    try: return int(v)
    except (TypeError, ValueError): return 0

# ── Parse the sectioned summary format ───────────────────────────────────────
# Each section starts with a header row like ('BY PICK TYPE', 'Direction', ...)
# Data rows that follow have the label in col-0; sub-rows for OVER/UNDER have
# None in col-0.  Blank rows (all None) separate sections.
#
# Returns dict with keys:
#   overall, pick_types, tiers, def_tiers, def_rank_buckets,
#   minutes_tiers, player_roles
# Each value is a list of dicts: {label, dir, total, decided, hits, misses, voids, rate}

def parse_summary(rows: list[tuple]) -> dict:
    def make_row(label, row_tuple):
        cols = list(row_tuple)
        while len(cols) < 8:
            cols.append(None)
        return {
            "label":   str(label or "").strip(),
            "dir":     str(cols[1] or "ALL").strip().upper(),
            "total":   safe_int(cols[2]),
            "decided": safe_int(cols[3]),
            "hits":    safe_int(cols[4]),
            "misses":  safe_int(cols[5]),
            "voids":   safe_int(cols[6]),
            "rate":    safe_float(cols[7]),
        }

    sections: dict[str, list] = {}
    current_section = None
    current_label   = None

    SECTION_KEYS = {
        "OVERALL":              "overall",
        "BY PICK TYPE":         "pick_types",
        "BY TIER":              "tiers",
        "BY OPP DEF TIER":      "def_tiers",
        "BY DEF TIER":          "def_tiers",
        "BY OPP DEF RANK":      "def_rank_buckets",
        "BY MINUTES TIER":      "minutes_tiers",
        "BY PLAYER ROLE":       "player_roles",
    }

    # Header indicator words in col1 that signal a section header row
    HEADER_WORDS = {"DIRECTION", "DIR", "TOTAL", "TOTAL PROPS", "DECIDED"}

    for row in rows:
        if all(v is None for v in row):
            current_section = None
            current_label   = None
            continue

        col0 = str(row[0] or "").strip()
        col1 = str(row[1] or "").strip().upper()

        # Skip the very first title row (contains pipe chars)
        if "|" in col0:
            continue

        # Check if this row is a section header (col0 matches a section key
        # AND col1 is a header word like "Direction" / "Total" etc.)
        matched_section = None
        if col1 in HEADER_WORDS:
            for key, sname in SECTION_KEYS.items():
                if col0.upper().startswith(key):
                    matched_section = sname
                    break

        if matched_section is not None:
            current_section = matched_section
            if matched_section not in sections:
                sections[matched_section] = []
            current_label = None
            continue

        if current_section is None:
            continue

        # Data row
        if col0:
            # New named entry
            current_label = col0
            sections[current_section].append(make_row(col0, row))
        elif current_label and col1 in ("OVER", "UNDER", "O", "U"):
            # Sub-row: OVER or UNDER breakdown for current label
            # Store as label__OVER / label__UNDER
            sub_label = f"{current_label}__{col1}"
            sections[current_section].append(make_row(sub_label, row))

    return sections


def find_rows(section: list, label: str, direction: str = "ALL") -> dict | None:
    """Return the first matching row by label (case-insensitive) and direction."""
    lu = label.upper()
    du = direction.upper()
    for r in section:
        rl = r["label"].upper()
        # strip sub-row suffix for matching
        base = rl.split("__")[0] if "__" in rl else rl
        dir_part = rl.split("__")[1] if "__" in rl else r["dir"]
        if base == lu and dir_part == du:
            return r
        if base == lu and r["dir"] == du:
            return r
    return None


def section_all_rows(section: list, direction: str = "ALL") -> list[dict]:
    """Return only base (non-sub) rows for a given direction."""
    du = direction.upper()
    return [r for r in section if "__" not in r["label"] and r["dir"] == du]


# ── HTML helpers ──────────────────────────────────────────────────────────────
def pct(v: float | None) -> str:
    if v is None: return "—"
    return f"{v * 100:.1f}%"

def rate_color(v: float | None) -> str:
    if v is None: return "#94a3b8"
    if v >= 0.60: return "#6ee7b7"
    if v >= 0.54: return "#fcd34d"
    if v >= 0.48: return "#93c5fd"
    return "#fca5a5"

def bar_color(v: float | None) -> str:
    if v is None: return "var(--muted)"
    if v >= 0.60: return "var(--green)"
    if v >= 0.54: return "var(--amber)"
    if v >= 0.48: return "var(--blue)"
    return "var(--red)"

def rate_bar(v: float | None) -> str:
    if v is None:
        return '<div class="rate-cell"><div class="rate-bar-bg"><div class="rate-bar-fill" style="width:0%;background:var(--muted)"></div></div><div class="rate-num" style="color:#94a3b8">—</div></div>'
    col  = bar_color(v)
    rcol = rate_color(v)
    w    = min(v * 100, 100)
    return f'<div class="rate-cell"><div class="rate-bar-bg"><div class="rate-bar-fill" style="width:{w:.1f}%;background:{col}"></div></div><div class="rate-num" style="color:{rcol}">{pct(v)}</div></div>'

def pick_chip(pt: str) -> str:
    u = pt.upper()
    if "GOBLIN" in u: return f'<span class="chip chip-goblin">🎃 {html_lib.escape(pt)}</span>'
    if "DEMON"  in u: return f'<span class="chip chip-demon">😈 {html_lib.escape(pt)}</span>'
    return f'<span class="chip chip-std">⭐ {html_lib.escape(pt)}</span>'

TIER_CHIP = {"A": "chip-a", "B": "chip-b", "C": "chip-c", "D": "chip-d"}
def tier_chip(t: str) -> str:
    key = t.replace("TIER","").strip().upper()
    cls = TIER_CHIP.get(key, "chip-d")
    return f'<span class="chip {cls}">TIER {html_lib.escape(key)}</span>'

def def_tier_label(dt: str) -> str:
    u = dt.upper()
    if "ELITE"  in u:                                   return '<span style="color:#10b981;font-weight:700">🟢 Elite</span>'
    if "ABOVE"  in u:                                   return '<span style="color:#10b981;font-weight:700">🟢 Above Avg</span>'
    if "AVG"    in u and "ABOVE" not in u and "BELOW" not in u: return '<span style="color:#f59e0b;font-weight:700">🟡 Avg</span>'
    if "WEAK"   in u or "BELOW" in u:                  return '<span style="color:#f59e0b;font-weight:700">🟡 Weak</span>'
    if "POOR"   in u or "BAD"   in u:                  return '<span style="color:#ef4444;font-weight:700">🔴 Poor</span>'
    return f'<span style="color:#94a3b8;font-weight:700">{html_lib.escape(dt)}</span>'

def row_cls(v: float | None) -> str:
    if v is None: return ""
    if v >= 0.54: return "player-hit"
    if v < 0.48:  return "player-miss"
    return ""

# ── Section builders ──────────────────────────────────────────────────────────
def build_sport_section(sport: str, color: str, icon: str, sections: dict) -> str:

    # ── Overall KPIs ──────────────────────────────────────────────────────────
    overall_rows = sections.get("overall", [])
    full = find_rows(overall_rows, "Full Slate", "ALL") or {}

    total   = full.get("total",   0)
    decided = full.get("decided", 0)
    hits    = full.get("hits",    0)
    misses  = full.get("misses",  0)
    voids   = full.get("voids",   0)
    rate    = full.get("rate")
    void_pct = voids / total * 100 if total else 0

    # OVER/UNDER split
    over_row  = find_rows(overall_rows, "Full Slate", "OVER")
    under_row = find_rows(overall_rows, "Full Slate", "UNDER")
    ou_html = ""
    if over_row or under_row:
        or_ = over_row  or {}
        ur_ = under_row or {}
        ou_html = f"""
    <div class="stat-grid stat-grid-2" style="margin-bottom:20px">
      <div class="stat-card {'green' if (or_.get('rate') or 0)>=0.53 else 'amber'}">
        <div class="stat-label">OVER HIT RATE</div>
        <div class="stat-val" style="color:{rate_color(or_.get('rate'))}">{pct(or_.get('rate'))}</div>
        <div class="stat-sub">{or_.get('hits',0):,} hits / {or_.get('decided',0):,} decided</div>
      </div>
      <div class="stat-card {'green' if (ur_.get('rate') or 0)>=0.53 else 'red'}">
        <div class="stat-label">UNDER HIT RATE</div>
        <div class="stat-val" style="color:{rate_color(ur_.get('rate'))}">{pct(ur_.get('rate'))}</div>
        <div class="stat-sub">{ur_.get('hits',0):,} hits / {ur_.get('decided',0):,} decided</div>
      </div>
    </div>"""

    # Tier A rate
    tier_rows = sections.get("tiers", [])
    tier_a    = find_rows(tier_rows, "Tier A", "ALL") or {}
    tier_a_rate = tier_a.get("rate")

    # Goblin rate
    pt_rows     = sections.get("pick_types", [])
    goblin_row  = find_rows(pt_rows, "Goblin", "ALL") or {}
    goblin_rate = goblin_row.get("rate")

    kpi = f"""
    <div class="section-label">OVERALL PERFORMANCE</div>
    <div class="stat-grid stat-grid-4" style="margin-bottom:20px">
      <div class="stat-card {'green' if (rate or 0)>=0.53 else 'amber'}">
        <div class="stat-label">OVERALL HIT RATE</div>
        <div class="stat-val" style="color:{rate_color(rate)}">{pct(rate)}</div>
        <div class="stat-sub">{hits:,} hits / {decided:,} decided</div>
      </div>
      <div class="stat-card blue">
        <div class="stat-label">TOTAL PROPS</div>
        <div class="stat-val" style="color:#60a5fa">{total:,}</div>
        <div class="stat-sub"><strong>{voids:,}</strong> voids ({void_pct:.1f}%)</div>
      </div>
      <div class="stat-card amber">
        <div class="stat-label">TIER A HIT RATE</div>
        <div class="stat-val" style="color:#f59e0b">{pct(tier_a_rate)}</div>
        <div class="stat-sub">{tier_a.get('hits',0):,} hits / {tier_a.get('decided',0):,} decided</div>
      </div>
      <div class="stat-card purple">
        <div class="stat-label">GOBLIN HIT RATE</div>
        <div class="stat-val" style="color:#a78bfa">{pct(goblin_rate)}</div>
        <div class="stat-sub">{goblin_row.get('hits',0):,} hits / {goblin_row.get('decided',0):,} decided</div>
      </div>
    </div>
    {ou_html}"""

    # ── Pick type table ───────────────────────────────────────────────────────
    pick_rows_html = ""
    for r in section_all_rows(pt_rows, "ALL"):
        if r["decided"] == 0: continue
        # sub-row OVER / UNDER
        over_sub  = find_rows(pt_rows, r["label"], "OVER")
        under_sub = find_rows(pt_rows, r["label"], "UNDER")
        sub_html  = ""
        if over_sub and over_sub["decided"]:
            sub_html += f'<div class="sub-dir"><span style="color:var(--green);font-size:10px">▲ OVER</span> {pct(over_sub["rate"])} ({over_sub["decided"]:,} dec)</div>'
        if under_sub and under_sub["decided"]:
            sub_html += f'<div class="sub-dir"><span style="color:var(--amber);font-size:10px">▼ UNDER</span> {pct(under_sub["rate"])} ({under_sub["decided"]:,} dec)</div>'
        pick_rows_html += f"""<tr>
          <td>{pick_chip(r['label'])}{('<div style="font-size:10px;color:var(--muted2);margin-top:4px">'+sub_html+'</div>') if sub_html else ''}</td>
          <td class="right mono">{r['decided']:,}</td>
          <td class="right mono pos">{r['hits']:,}</td>
          <td class="right mono neg">{r['misses']:,}</td>
          <td>{rate_bar(r['rate'])}</td>
        </tr>"""

    # ── Tier table ────────────────────────────────────────────────────────────
    tier_rows_html = ""
    for t_label in ["Tier A", "Tier B", "Tier C", "Tier D"]:
        tr = find_rows(tier_rows, t_label, "ALL")
        if not tr or tr["decided"] == 0: continue
        # check OVER / UNDER sub
        to = find_rows(tier_rows, t_label, "OVER")
        tu = find_rows(tier_rows, t_label, "UNDER")
        sub_html = ""
        if to and to["decided"]: sub_html += f'<span style="color:var(--green);font-size:10px;margin-right:8px">▲ {pct(to["rate"])}</span>'
        if tu and tu["decided"]: sub_html += f'<span style="color:var(--amber);font-size:10px">▼ {pct(tu["rate"])}</span>'
        key = t_label.replace("Tier ","").strip()
        tier_rows_html += f"""<tr class="{row_cls(tr['rate'])}">
          <td>{tier_chip(key)}{('<div style="font-size:10px;color:var(--muted2);margin-top:3px">'+sub_html+'</div>') if sub_html else ''}</td>
          <td class="right mono">{tr['decided']:,}</td>
          <td class="right mono pos">{tr['hits']:,}</td>
          <td>{rate_bar(tr['rate'])}</td>
        </tr>"""

    two_col_1 = f"""
    <div class="two-col">
      <div>
        <div class="section-label">BY PICK TYPE</div>
        <div class="table-wrap"><table>
          <thead><tr><th>TYPE</th><th class="right">DECIDED</th><th class="right">HITS</th><th class="right">MISSES</th><th>HIT RATE</th></tr></thead>
          <tbody>{pick_rows_html}</tbody>
        </table></div>
      </div>
      <div>
        <div class="section-label">BY TIER</div>
        <div class="table-wrap"><table>
          <thead><tr><th>TIER</th><th class="right">DECIDED</th><th class="right">HITS</th><th>HIT RATE</th></tr></thead>
          <tbody>{tier_rows_html}</tbody>
        </table></div>
      </div>
    </div>"""

    # ── Def tier table ────────────────────────────────────────────────────────
    def_rows_html = ""
    DEF_ORDER = ["ELITE","ABOVE AVG","ABOVE","AVG","AVERAGE","WEAK","BELOW AVG","BELOW","POOR","BAD"]
    def dt_sort(label):
        u = label.upper()
        for i, d in enumerate(DEF_ORDER):
            if d in u: return i
        return 99

    dt_section = sections.get("def_tiers", [])
    dt_base    = sorted(section_all_rows(dt_section, "ALL"), key=lambda r: dt_sort(r["label"]))
    for r in dt_base:
        if r["decided"] == 0: continue
        over_sub  = find_rows(dt_section, r["label"], "OVER")
        under_sub = find_rows(dt_section, r["label"], "UNDER")
        os = f'<span style="color:{rate_color(over_sub["rate"])}">{pct(over_sub["rate"])}</span>'  if over_sub  and over_sub.get("decided")  else '<span class="neu">—</span>'
        us = f'<span style="color:{rate_color(under_sub["rate"])}">{pct(under_sub["rate"])}</span>' if under_sub and under_sub.get("decided") else '<span class="neu">—</span>'
        def_rows_html += f"""<tr class="{row_cls(r['rate'])}">
          <td>{def_tier_label(r['label'])}</td>
          <td class="right mono">{r['decided']:,}</td>
          <td class="right mono">{r['hits']:,}</td>
          <td>{rate_bar(r['rate'])}</td>
          <td class="mono">{os}</td>
          <td class="mono">{us}</td>
        </tr>"""

    def_section_html = ""
    if def_rows_html:
        def_section_html = f"""
    <div class="section-label">BY OPP DEFENSIVE TIER</div>
    <div class="table-wrap" style="margin-bottom:20px"><table>
      <thead><tr><th>DEF TIER</th><th class="right">DECIDED</th><th class="right">HITS</th><th>HIT RATE</th><th>OVER</th><th>UNDER</th></tr></thead>
      <tbody>{def_rows_html}</tbody>
    </table></div>"""

    # ── Def rank buckets ──────────────────────────────────────────────────────
    drb_section = sections.get("def_rank_buckets", [])
    drb_rows_html = ""
    for r in section_all_rows(drb_section, "ALL"):
        if r["decided"] == 0: continue
        over_sub  = find_rows(drb_section, r["label"], "OVER")
        under_sub = find_rows(drb_section, r["label"], "UNDER")
        os = f'<span style="color:{rate_color(over_sub["rate"])}">{pct(over_sub["rate"])}</span>'  if over_sub  and over_sub.get("decided")  else '<span class="neu">—</span>'
        us = f'<span style="color:{rate_color(under_sub["rate"])}">{pct(under_sub["rate"])}</span>' if under_sub and under_sub.get("decided") else '<span class="neu">—</span>'
        drb_rows_html += f"""<tr class="{row_cls(r['rate'])}">
          <td class="mono">Rank {html_lib.escape(r['label'])}</td>
          <td class="right mono">{r['decided']:,}</td>
          <td class="right mono">{r['hits']:,}</td>
          <td>{rate_bar(r['rate'])}</td>
          <td class="mono">{os}</td>
          <td class="mono">{us}</td>
        </tr>"""

    drb_section_html = ""
    if drb_rows_html:
        drb_section_html = f"""
    <div class="section-label">BY OPP DEF RANK BUCKET</div>
    <div class="table-wrap" style="margin-bottom:20px"><table>
      <thead><tr><th>RANK BUCKET</th><th class="right">DECIDED</th><th class="right">HITS</th><th>HIT RATE</th><th>OVER</th><th>UNDER</th></tr></thead>
      <tbody>{drb_rows_html}</tbody>
    </table></div>"""

    # ── Minutes tier ──────────────────────────────────────────────────────────
    min_section = sections.get("minutes_tiers", [])
    min_rows_html = ""
    for r in section_all_rows(min_section, "ALL"):
        if r["decided"] == 0 or r["label"].upper() == "UNKNOWN": continue
        min_rows_html += f"""<tr class="{row_cls(r['rate'])}">
          <td class="mono">{html_lib.escape(r['label'])}</td>
          <td class="right mono">{r['decided']:,}</td>
          <td class="right mono">{r['hits']:,}</td>
          <td>{rate_bar(r['rate'])}</td>
        </tr>"""

    # ── Player role ───────────────────────────────────────────────────────────
    role_section = sections.get("player_roles", [])
    role_rows_html = ""
    for r in section_all_rows(role_section, "ALL"):
        if r["decided"] == 0 or r["label"].upper() == "UNKNOWN": continue
        over_sub  = find_rows(role_section, r["label"], "OVER")
        under_sub = find_rows(role_section, r["label"], "UNDER")
        os = f'<span style="color:{rate_color(over_sub["rate"])}">{pct(over_sub["rate"])}</span>'  if over_sub  and over_sub.get("decided")  else '<span class="neu">—</span>'
        us = f'<span style="color:{rate_color(under_sub["rate"])}">{pct(under_sub["rate"])}</span>' if under_sub and under_sub.get("decided") else '<span class="neu">—</span>'
        role_rows_html += f"""<tr class="{row_cls(r['rate'])}">
          <td class="mono">{html_lib.escape(r['label'])}</td>
          <td class="right mono">{r['decided']:,}</td>
          <td class="right mono">{r['hits']:,}</td>
          <td>{rate_bar(r['rate'])}</td>
          <td class="mono">{os}</td>
          <td class="mono">{us}</td>
        </tr>"""

    extra_tables = ""
    if min_rows_html or role_rows_html:
        left = right = ""
        if min_rows_html:
            left = f"""<div>
        <div class="section-label">BY MINUTES TIER</div>
        <div class="table-wrap"><table>
          <thead><tr><th>MINUTES</th><th class="right">DECIDED</th><th class="right">HITS</th><th>HIT RATE</th></tr></thead>
          <tbody>{min_rows_html}</tbody>
        </table></div></div>"""
        if role_rows_html:
            right = f"""<div>
        <div class="section-label">BY PLAYER ROLE</div>
        <div class="table-wrap"><table>
          <thead><tr><th>ROLE</th><th class="right">DECIDED</th><th class="right">HITS</th><th>HIT RATE</th><th>OVER</th><th>UNDER</th></tr></thead>
          <tbody>{role_rows_html}</tbody>
        </table></div></div>"""
        extra_tables = f'<div class="two-col">{left}{right}</div>'

    # ── Alerts ────────────────────────────────────────────────────────────────
    alerts = ""
    demon_row = find_rows(pt_rows, "Demon", "ALL")
    if demon_row and demon_row["decided"] >= 5:
        dr = demon_row["rate"] or 0
        if dr < 0.40:
            alerts += f"""
    <div class="alert alert-red">
      <div class="alert-title">🚨 {sport} Demon Lines — {pct(demon_row['rate'])} on {demon_row['decided']:,} decided</div>
      Demon hit rate is well below breakeven. Demon props are a net negative — exclude from slips until further notice.
    </div>"""
        else:
            alerts += f"""
    <div class="alert alert-amber">
      <div class="alert-title">⚠️ {sport} Demon Lines — {pct(demon_row['rate'])} on {demon_row['decided']:,} decided</div>
      Monitor demon performance before including in slips.
    </div>"""

    if tier_a_rate is not None and tier_a_rate >= 0.60:
        alerts += f"""
    <div class="alert alert-green">
      <div class="alert-title">✅ {sport} Tier A performing well — {pct(tier_a_rate)} on {tier_a.get('decided',0):,} decided</div>
      Tier A props are hitting well above breakeven. Anchor slips on Tier A picks.
    </div>"""

    return f"""
  <div class="sport-section">
    <div class="sport-header">
      <div class="sport-label" style="color:{color}">{icon} {sport}</div>
      <div class="sport-header-line"></div>
      <div style="font-family:'DM Mono',monospace;font-size:11px;color:var(--muted2)">{total:,} TOTAL PROPS</div>
    </div>
    {kpi}
    {two_col_1}
    {def_section_html}
    {drb_section_html}
    {extra_tables}
    {alerts}
  </div>"""


# ── Takeaways section ─────────────────────────────────────────────────────────
def build_takeaways(nba: dict | None, cbb: dict | None) -> str:
    cards = []

    def get_rate(sections, section_key, label, direction="ALL"):
        if not sections: return None
        rows = sections.get(section_key, [])
        r = find_rows(rows, label, direction)
        return r["rate"] if r else None

    # Tier A + Goblin summary
    nba_ta  = get_rate(nba, "tiers",      "Tier A")
    cbb_ta  = get_rate(cbb, "tiers",      "Tier A")
    nba_gb  = get_rate(nba, "pick_types", "Goblin")
    cbb_gb  = get_rate(cbb, "pick_types", "Goblin")
    ta_body = ""
    if nba_ta is not None: ta_body += f"NBA Tier A: <strong>{pct(nba_ta)}</strong>. "
    if cbb_ta is not None: ta_body += f"CBB Tier A: <strong>{pct(cbb_ta)}</strong>. "
    if nba_gb is not None: ta_body += f"NBA Goblin: <strong>{pct(nba_gb)}</strong>. "
    if cbb_gb is not None: ta_body += f"CBB Goblin: <strong>{pct(cbb_gb)}</strong>."
    if ta_body:
        cards.append(f"""<div class="insight-card">
      <div class="insight-icon">✅</div>
      <div class="insight-title">Tier A &amp; Goblin Performance</div>
      <div class="insight-body">{ta_body}</div>
    </div>""")

    # Demon check
    nba_dem = get_rate(nba, "pick_types", "Demon")
    cbb_dem = get_rate(cbb, "pick_types", "Demon")
    dem_body = ""
    if nba_dem is not None: dem_body += f"NBA Demons: <strong>{pct(nba_dem)}</strong>. "
    if cbb_dem is not None: dem_body += f"CBB Demons: <strong>{pct(cbb_dem)}</strong>. "
    if dem_body:
        both_bad = (nba_dem or 1) < 0.40 and (cbb_dem or 1) < 0.40
        dem_body += "Demon props are a net negative — exclude until further notice." if both_bad else "Monitor demon performance before including in slips."
        dem_icon = "🚨" if both_bad else "⚠️"
        cards.append(f"""<div class="insight-card">
      <div class="insight-icon">{dem_icon}</div>
      <div class="insight-title">Demon Line Performance</div>
      <div class="insight-body">{dem_body}</div>
    </div>""")

    # OVER vs UNDER
    nba_over  = get_rate(nba, "overall", "Full Slate", "OVER")
    nba_under = get_rate(nba, "overall", "Full Slate", "UNDER")
    cbb_over  = get_rate(cbb, "overall", "Full Slate", "OVER")
    cbb_under = get_rate(cbb, "overall", "Full Slate", "UNDER")
    ou_body = ""
    if nba_over  is not None: ou_body += f"NBA OVERs: <strong>{pct(nba_over)}</strong>. "
    if nba_under is not None: ou_body += f"NBA UNDERs: <strong>{pct(nba_under)}</strong>. "
    if cbb_over  is not None: ou_body += f"CBB OVERs: <strong>{pct(cbb_over)}</strong>. "
    if cbb_under is not None: ou_body += f"CBB UNDERs: <strong>{pct(cbb_under)}</strong>."
    if ou_body:
        cards.append(f"""<div class="insight-card">
      <div class="insight-icon">📊</div>
      <div class="insight-title">Over vs Under Performance</div>
      <div class="insight-body">{ou_body}</div>
    </div>""")

    # Overall summary
    parts = []
    if nba:
        r = get_rate(nba, "overall", "Full Slate")
        nr = find_rows(nba.get("overall",[]), "Full Slate", "ALL") or {}
        if r is not None: parts.append(f"NBA: <strong>{pct(r)}</strong> overall ({nr.get('decided',0):,} decided)")
    if cbb:
        r = get_rate(cbb, "overall", "Full Slate")
        cr = find_rows(cbb.get("overall",[]), "Full Slate", "ALL") or {}
        if r is not None: parts.append(f"CBB: <strong>{pct(r)}</strong> overall ({cr.get('decided',0):,} decided)")
    if parts:
        cards.append(f"""<div class="insight-card">
      <div class="insight-icon">📋</div>
      <div class="insight-title">Overall Slate Summary</div>
      <div class="insight-body">{". ".join(parts)}.</div>
    </div>""")

    grid = "\n".join(cards)
    return f"""
  <div class="sport-section">
    <div class="sport-header">
      <div class="sport-label" style="color:#8b5cf6">📋 TAKEAWAYS</div>
      <div class="sport-header-line"></div>
    </div>
    <div class="insight-grid">{grid}</div>
  </div>"""


# ── CSS ───────────────────────────────────────────────────────────────────────
CSS = """
:root{--bg:#070a10;--bg2:#0c1018;--bg3:#111722;--border:#1c2333;--bd2:#243044;--text:#e8edf5;--muted:#4a5568;--muted2:#6b7a94;--blue:#3b82f6;--green:#10b981;--amber:#f59e0b;--red:#ef4444;--purple:#8b5cf6;}
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:'DM Sans',sans-serif;background:var(--bg);color:var(--text);min-height:100vh;padding-bottom:60px}
body::before{content:'';position:fixed;top:-20%;left:-10%;width:55%;height:55%;background:radial-gradient(ellipse,rgba(59,130,246,.04) 0%,transparent 70%);pointer-events:none}
body::after{content:'';position:fixed;bottom:-20%;right:-10%;width:50%;height:50%;background:radial-gradient(ellipse,rgba(16,185,129,.03) 0%,transparent 70%);pointer-events:none}
::-webkit-scrollbar{width:4px}::-webkit-scrollbar-track{background:var(--bg2)}::-webkit-scrollbar-thumb{background:var(--bd2);border-radius:4px}
/* NAV */
.snav{position:sticky;top:0;z-index:200;background:rgba(7,10,16,.95);backdrop-filter:blur(16px);border-bottom:1px solid var(--border);padding:0 24px;display:flex;align-items:stretch;height:54px}
.snav-brand{display:flex;align-items:center;gap:9px;margin-right:28px;flex-shrink:0;text-decoration:none}
.snav-mark{width:28px;height:28px;background:linear-gradient(135deg,#c8ff00,#00e5ff);border-radius:7px;display:flex;align-items:center;justify-content:center;font-size:15px}
.snav-name{font-family:'Bebas Neue',sans-serif;font-size:19px;letter-spacing:3px;color:#e8edf5}
.snav-links{display:flex;align-items:stretch;gap:0;list-style:none;flex:1}
.snav-links a{display:flex;align-items:center;gap:7px;padding:0 14px;font-family:'DM Mono',monospace;font-size:10px;font-weight:500;letter-spacing:1.5px;text-transform:uppercase;color:#4a5568;text-decoration:none;border-bottom:2px solid transparent;transition:color .15s,border-color .15s;white-space:nowrap}
.snav-links a svg{opacity:.55;transition:opacity .15s}
.snav-links a:hover svg{opacity:1}
.snav-links li:nth-child(1) a:hover{color:#00d4ff}
.snav-links li:nth-child(2) a:hover{color:#c8ff00}
.snav-links li:nth-child(3) a:hover{color:#9b6dff}
.snav-links li:nth-child(4) a:hover{color:#00e5a0}
.snav-links li:nth-child(5) a:hover{color:#ffb830}
.snav-links a.snav-active{color:#c8ff00;border-bottom-color:#c8ff00}
.snav-right{display:flex;align-items:center;margin-left:auto}
.snav-live{font-family:'DM Mono',monospace;font-size:9px;letter-spacing:1.5px;color:#10b981;display:flex;align-items:center;gap:5px}
.snav-live::before{content:'';width:5px;height:5px;border-radius:50%;background:#10b981;animation:snavpulse 2s infinite}
@keyframes snavpulse{0%,100%{opacity:1}50%{opacity:.3}}
/* HEADER */
header{background:rgba(7,10,16,.92);backdrop-filter:blur(12px);border-bottom:1px solid var(--border);padding:18px 32px;display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:12px}
.logo{display:flex;align-items:center;gap:14px}
.logo-icon{width:42px;height:42px;background:linear-gradient(135deg,#3b82f6,#10b981);border-radius:11px;display:flex;align-items:center;justify-content:center;font-size:20px;box-shadow:0 0 20px rgba(59,130,246,.3)}
.logo-title{font-family:'Bebas Neue',sans-serif;font-size:26px;letter-spacing:2px;background:linear-gradient(135deg,#fff 40%,#94a3b8);-webkit-background-clip:text;-webkit-text-fill-color:transparent}
.logo-sub{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:2.5px;margin-top:2px}
.date-badge{font-family:'DM Mono',monospace;font-size:11px;color:var(--muted2);background:var(--bg3);border:1px solid var(--bd2);border-radius:8px;padding:6px 14px;letter-spacing:1px}
/* LAYOUT */
.main{max-width:1100px;margin:0 auto;padding:28px 20px}
.sport-header{display:flex;align-items:center;gap:14px;margin-bottom:22px}
.sport-label{font-family:'Bebas Neue',sans-serif;font-size:32px;letter-spacing:3px;line-height:1}
.sport-header-line{flex:1;height:1px;background:linear-gradient(90deg,var(--bd2),transparent)}
.sport-section{margin-bottom:48px}
.section-label{font-family:'DM Mono',monospace;font-size:10px;color:var(--muted);letter-spacing:3px;display:flex;align-items:center;gap:10px;margin-bottom:16px}
.section-label::after{content:'';flex:1;height:1px;background:var(--border)}
/* STAT CARDS */
.stat-grid{display:grid;gap:14px;margin-bottom:24px}
.stat-grid-4{grid-template-columns:repeat(4,1fr)}
.stat-grid-2{grid-template-columns:repeat(2,1fr)}
.stat-card{background:var(--bg2);border:1px solid var(--border);border-radius:14px;padding:16px 18px;position:relative;overflow:hidden;transition:border-color .2s}
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
/* TABLES */
.table-wrap{background:var(--bg2);border:1px solid var(--border);border-radius:14px;overflow:hidden;margin-bottom:20px}
table{width:100%;border-collapse:collapse;font-size:13px}
th{font-family:'DM Mono',monospace;font-size:9px;letter-spacing:2px;color:var(--muted);padding:10px 14px;text-align:left;background:var(--bg3);border-bottom:1px solid var(--border);white-space:nowrap}
th.right{text-align:right}
td{padding:10px 14px;border-bottom:1px solid rgba(255,255,255,.04);vertical-align:middle}
tr:last-child td{border-bottom:none}
tr:hover td{background:rgba(255,255,255,.015)}
td.right{text-align:right}td.mono{font-family:'DM Mono',monospace;font-size:12px}
/* RATE BARS */
.rate-cell{display:flex;align-items:center;gap:10px}
.rate-bar-bg{flex:1;height:5px;background:rgba(255,255,255,.06);border-radius:3px;overflow:hidden}
.rate-bar-fill{height:100%;border-radius:3px;transition:width .4s}
.rate-num{font-family:'DM Mono',monospace;font-size:12px;width:44px;text-align:right;flex-shrink:0}
/* CHIPS */
.chip{display:inline-block;border-radius:6px;padding:2px 9px;font-size:11px;font-weight:700;font-family:'DM Mono',monospace;letter-spacing:.5px}
.chip-a{background:rgba(16,185,129,.12);color:#6ee7b7;border:1px solid rgba(16,185,129,.25)}
.chip-b{background:rgba(59,130,246,.12);color:#93c5fd;border:1px solid rgba(59,130,246,.25)}
.chip-c{background:rgba(245,158,11,.12);color:#fcd34d;border:1px solid rgba(245,158,11,.25)}
.chip-d{background:rgba(100,116,139,.12);color:#94a3b8;border:1px solid rgba(100,116,139,.25)}
.chip-goblin{background:rgba(139,92,246,.15);color:#c4b5fd;border:1px solid rgba(139,92,246,.3)}
.chip-demon{background:rgba(239,68,68,.12);color:#fca5a5;border:1px solid rgba(239,68,68,.25)}
.chip-std{background:rgba(59,130,246,.12);color:#93c5fd;border:1px solid rgba(59,130,246,.25)}
/* LAYOUT GRIDS */
.two-col{display:grid;grid-template-columns:1fr 1fr;gap:20px;margin-bottom:20px}
.insight-grid{display:grid;grid-template-columns:repeat(3,1fr);gap:14px;margin-bottom:24px}
.insight-card{background:var(--bg3);border:1px solid var(--border);border-radius:12px;padding:14px 16px}
.insight-icon{font-size:22px;margin-bottom:8px}
.insight-title{font-weight:700;font-size:13px;margin-bottom:6px}
.insight-body{font-size:12px;color:var(--muted2);line-height:1.6}
.insight-body strong{color:var(--text)}
/* ROW STATES */
.player-hit td:first-child{border-left:3px solid var(--green)}
.player-miss td:first-child{border-left:3px solid var(--red)}
.player-warn td:first-child{border-left:3px solid var(--amber)}
.pos{color:var(--green);font-weight:700}.neg{color:var(--red);font-weight:700}.neu{color:var(--muted2)}
/* ALERTS */
.alert{border-radius:12px;padding:14px 18px;margin-bottom:20px;border:1px solid;font-size:13px;line-height:1.6}
.alert-red{background:rgba(239,68,68,.06);border-color:rgba(239,68,68,.25)}
.alert-green{background:rgba(16,185,129,.06);border-color:rgba(16,185,129,.25)}
.alert-amber{background:rgba(245,158,11,.06);border-color:rgba(245,158,11,.25)}
.alert-title{font-weight:700;margin-bottom:4px}
.sub-dir{display:inline-block;margin-right:8px}
@media(max-width:768px){.stat-grid-4,.stat-grid-2{grid-template-columns:repeat(2,1fr)}.two-col,.insight-grid{grid-template-columns:1fr}}
"""

NAV_SVG = {
    "home":    '<svg width="13" height="13" viewBox="0 0 14 14" fill="none"><path d="M7 1L1 5.5V13h4V9h4v4h4V5.5L7 1Z" stroke="currentColor" stroke-width="1.3" stroke-linejoin="round"/></svg>',
    "slate":   '<svg width="13" height="13" viewBox="0 0 14 14" fill="none"><rect x="1.5" y="1.5" width="11" height="11" rx="1.5" stroke="currentColor" stroke-width="1.3"/><line x1="4" y1="5" x2="10" y2="5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><line x1="4" y1="7.5" x2="10" y2="7.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/><line x1="4" y1="10" x2="7.5" y2="10" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>',
    "tickets": '<svg width="13" height="13" viewBox="0 0 14 14" fill="none"><rect x="1.5" y="3" width="11" height="8" rx="1.5" stroke="currentColor" stroke-width="1.3"/><path d="M1.5 6h11" stroke="currentColor" stroke-width="1" stroke-dasharray="2 1.5"/><circle cx="4" cy="9" r="1" fill="currentColor"/><circle cx="7" cy="9" r="1" fill="currentColor"/><circle cx="10" cy="9" r="1" fill="currentColor"/></svg>',
    "grades":  '<svg width="13" height="13" viewBox="0 0 14 14" fill="none"><path d="M2 10.5L5.5 7l2.5 2.5L11 5" stroke="currentColor" stroke-width="1.3" stroke-linecap="round" stroke-linejoin="round"/><rect x="1.5" y="1.5" width="11" height="11" rx="1.5" stroke="currentColor" stroke-width="1.3" fill="none"/></svg>',
    "payouts": '<svg width="13" height="13" viewBox="0 0 14 14" fill="none"><circle cx="7" cy="7" r="5.5" stroke="currentColor" stroke-width="1.3"/><path d="M7 3.5v7M5 5.5c0-.8.9-1.5 2-1.5s2 .7 2 1.5-1 1.3-2 1.5-2 .7-2 1.5.9 1.5 2 1.5 2-.7 2-1.5" stroke="currentColor" stroke-width="1.2" stroke-linecap="round"/></svg>',
}

def build_html(date_str: str, nba: dict | None, cbb: dict | None) -> str:
    try:
        d = datetime.strptime(date_str, "%Y-%m-%d")
        date_display = d.strftime("%b %d, %Y").upper()
    except Exception:
        date_display = date_str.upper()

    nba_section = build_sport_section("NBA", "#3b82f6", "🏀", nba) if nba else ""
    cbb_section = build_sport_section("CBB", "#10b981", "🎓", cbb) if cbb else ""
    takeaways   = build_takeaways(nba, cbb)

    sv = NAV_SVG
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Slate Eval — {date_display}</title>
<link href="https://fonts.googleapis.com/css2?family=Bebas+Neue&family=DM+Mono:wght@400;500&family=DM+Sans:wght@400;500;700&display=swap" rel="stylesheet"/>
<style>{CSS}</style>
</head>
<body>
<nav class="snav">
  <a class="snav-brand" href="/">
    <div class="snav-mark">🧠</div>
    <span class="snav-name">SlateIQ</span>
  </a>
  <ul class="snav-links">
    <li><a href="/" data-page="control">{sv['home']} Home</a></li>
    <li><a href="/slate" data-page="slate">{sv['slate']} Slate</a></li>
    <li><a href="/tickets" data-page="tickets">{sv['tickets']} Tickets</a></li>
    <li><a href="/grades" data-page="grades" class="snav-active">{sv['grades']} Grades</a></li>
    <li><a href="/payout" data-page="payout">{sv['payouts']} Payouts</a></li>
  </ul>
  <div class="snav-right"><span class="snav-live">LIVE</span></div>
</nav>
<header>
  <div class="logo">
    <div class="logo-icon">📊</div>
    <div>
      <div class="logo-title">SLATE EVALUATION</div>
      <div class="logo-sub">POST-GAME GRADE REPORT</div>
    </div>
  </div>
  <div class="date-badge">📅 {date_display}</div>
</header>
<div class="main">
{nba_section}
{cbb_section}
{takeaways}
</div>
<script>
(function(){{
  var path = window.location.pathname.replace(/\\/$/,'') || '/';
  var map  = {{'/':'control','/tickets':'tickets','/slate':'slate','/payout':'payout','/grades':'grades'}};
  var el   = document.querySelector('.snav-links a[data-page="'+(map[path]||'')+'"]');
  if(el){{ el.classList.add('snav-active'); }}
}})();
</script>
</body>
</html>"""


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, default="")
    parser.add_argument("--nba",  type=str, default="")
    parser.add_argument("--cbb",  type=str, default="")
    parser.add_argument("--out",  type=str, default="")
    args = parser.parse_args()

    date_str = args.date or datetime.today().strftime("%Y-%m-%d")

    def find_graded(sport: str) -> Path | None:
        candidates  = list(SCRIPT_DIR.glob(f"outputs/{date_str}/{sport.lower()}_graded_{date_str}.xlsx"))
        candidates += list(SCRIPT_DIR.glob(f"outputs/**/{sport.lower()}_graded*.xlsx"))
        candidates += list(SCRIPT_DIR.glob(f"{sport.lower()}_graded*.xlsx"))
        return sorted(candidates, key=lambda p: p.stat().st_mtime, reverse=True)[0] if candidates else None

    nba_path = Path(args.nba) if args.nba else find_graded("nba")
    cbb_path = Path(args.cbb) if args.cbb else find_graded("cbb")

    if not nba_path and not cbb_path:
        print("ERROR: No graded xlsx found. Use --nba and/or --cbb to specify paths.")
        sys.exit(1)

    nba_sections = cbb_sections = None

    if nba_path and nba_path.exists():
        print(f"  Loading NBA: {nba_path.name}")
        rows = load_rows(nba_path)
        nba_sections = parse_summary(rows)
        overall = find_rows(nba_sections.get("overall",[]), "Full Slate", "ALL") or {}
        print(f"  NBA  decided={overall.get('decided',0):,}  hit rate={pct(overall.get('rate'))}")
    elif nba_path:
        print(f"  WARNING: NBA file not found: {nba_path}")

    if cbb_path and cbb_path.exists():
        print(f"  Loading CBB: {cbb_path.name}")
        rows = load_rows(cbb_path)
        cbb_sections = parse_summary(rows)
        overall = find_rows(cbb_sections.get("overall",[]), "Full Slate", "ALL") or {}
        print(f"  CBB  decided={overall.get('decided',0):,}  hit rate={pct(overall.get('rate'))}")
    elif cbb_path:
        print(f"  WARNING: CBB file not found: {cbb_path}")

    html = build_html(date_str, nba_sections, cbb_sections)

    if args.out:
        out = Path(args.out)
    else:
        out = TEMPLATES_DIR / f"slate_eval_{date_str}.html"

    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"  Saved → {out}  ({len(html):,} bytes)")


if __name__ == "__main__":
    main()
