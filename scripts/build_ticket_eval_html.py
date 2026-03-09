"""
build_ticket_eval_html.py
==========================
Converts combined_tickets_graded_*.xlsx into an HTML eval report
showing ticket performance, leg results, and stats.

Usage:
    py -3.14 build_ticket_eval_html.py --date 2026-03-07 --graded path/to/graded.xlsx --out output.html
"""

from __future__ import annotations

import argparse
import html as html_lib
import sys
from datetime import datetime
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except ImportError:
    print("ERROR: pandas not installed. Run: pip install pandas")
    sys.exit(1)


def h(v: Any) -> str:
    """HTML escape."""
    return html_lib.escape(str(v) if v is not None else "")


def fmt(v: Any, dec: int = 2) -> str:
    """Format number."""
    try:
        return f"{float(v):.{dec}f}"
    except (TypeError, ValueError):
        return str(v) if v is not None else "—"


def pct(v: Any) -> str:
    """Format percentage."""
    try:
        f = float(v)
        return f"{f*100:.1f}%" if f <= 1.0 else f"{f:.1f}%"
    except (TypeError, ValueError):
        return "—"


def outcome_class(outcome: str) -> str:
    """CSS class for outcome."""
    if outcome == "HIT":
        return "outcome-hit"
    elif outcome == "MISS":
        return "outcome-miss"
    elif outcome == "PUSH":
        return "outcome-push"
    else:
        return "outcome-void"


def outcome_badge(outcome: str) -> str:
    """HTML badge for outcome."""
    labels = {
        "HIT": "✓ HIT",
        "MISS": "✗ MISS",
        "PUSH": "↔ PUSH",
        "NO_ACTUAL": "⊘ NO ACTUAL",
        "VOID": "∅ VOID",
    }
    label = labels.get(outcome, str(outcome))
    cls = outcome_class(outcome)
    return f'<span class="badge {cls}">{label}</span>'


CSS = """
* { margin: 0; padding: 0; box-sizing: border-box; }

:root {
  --accent: #00d9ff;
  --green: #6ee7b7;
  --red: #fca5a5;
  --amber: #fcd34d;
  --blue: #93c5fd;
  --slate-50: #f8fafc;
  --slate-100: #f1f5f9;
  --slate-200: #e2e8f0;
  --slate-400: #94a3b8;
  --slate-600: #475569;
  --slate-700: #334155;
  --slate-800: #1e293b;
  --slate-900: #0f172a;
}

body {
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
  background: var(--slate-900);
  color: var(--slate-100);
  line-height: 1.5;
  overflow-x: hidden;
}

a { color: var(--accent); text-decoration: none; }
a:hover { text-decoration: underline; }

header {
  background: linear-gradient(135deg, rgba(15,23,42,0.95), rgba(30,41,59,0.95));
  border-bottom: 2px solid var(--accent);
  padding: 20px;
  text-align: center;
}

.logo-title {
  font-size: 28px;
  font-weight: bold;
  color: var(--accent);
  letter-spacing: 2px;
}

.logo-sub {
  font-size: 12px;
  color: var(--slate-400);
  margin-top: 4px;
  text-transform: uppercase;
  letter-spacing: 1px;
}

.date-badge {
  position: absolute;
  top: 20px;
  right: 20px;
  padding: 6px 12px;
  background: var(--slate-700);
  border: 1px solid var(--slate-600);
  border-radius: 4px;
  font-size: 12px;
  color: var(--slate-300);
}

.main {
  max-width: 1400px;
  margin: 0 auto;
  padding: 20px;
}

.section {
  margin-bottom: 30px;
}

.section-title {
  font-size: 18px;
  font-weight: 600;
  color: var(--accent);
  margin-bottom: 12px;
  text-transform: uppercase;
  letter-spacing: 1px;
  border-bottom: 2px solid var(--accent);
  padding-bottom: 8px;
}

.kpi-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(120px, 1fr));
  gap: 12px;
  margin-bottom: 20px;
}

.kpi-card {
  background: var(--slate-800);
  border: 1px solid var(--slate-700);
  border-radius: 6px;
  padding: 12px;
  text-align: center;
}

.kpi-val {
  font-size: 20px;
  font-weight: bold;
  color: var(--accent);
  margin-bottom: 4px;
}

.kpi-label {
  font-size: 11px;
  color: var(--slate-400);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.table-wrapper {
  overflow-x: auto;
  background: var(--slate-800);
  border: 1px solid var(--slate-700);
  border-radius: 6px;
  margin-bottom: 20px;
}

table {
  width: 100%;
  border-collapse: collapse;
  font-size: 12px;
}

th {
  background: var(--slate-700);
  padding: 10px;
  text-align: left;
  font-weight: 600;
  color: var(--accent);
  border-bottom: 1px solid var(--slate-600);
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

td {
  padding: 8px 10px;
  border-bottom: 1px solid var(--slate-700);
}

tr:last-child td {
  border-bottom: none;
}

tr:hover {
  background: rgba(0, 217, 255, 0.05);
}

.badge {
  display: inline-block;
  padding: 4px 8px;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.5px;
}

.outcome-hit {
  background: rgba(110, 231, 183, 0.15);
  color: #6ee7b7;
}

.outcome-miss {
  background: rgba(252, 165, 165, 0.15);
  color: #fca5a5;
}

.outcome-push {
  background: rgba(252, 211, 77, 0.15);
  color: #fcd34d;
}

.outcome-void {
  background: rgba(148, 163, 184, 0.15);
  color: var(--slate-400);
}

.chip {
  display: inline-block;
  padding: 3px 7px;
  border-radius: 3px;
  font-size: 10px;
  font-weight: 500;
}

.chip-a { background: rgba(110, 231, 183, 0.2); color: #6ee7b7; }
.chip-b { background: rgba(0, 217, 255, 0.2); color: var(--accent); }
.chip-c { background: rgba(252, 211, 77, 0.2); color: #fcd34d; }
.chip-d { background: rgba(148, 163, 184, 0.2); color: var(--slate-400); }

.num { font-family: 'Courier New', monospace; }

.footer {
  text-align: center;
  padding: 20px;
  color: var(--slate-400);
  font-size: 11px;
  border-top: 1px solid var(--slate-700);
  margin-top: 40px;
}

.ticket-row {
  cursor: pointer;
  user-select: none;
}

.ticket-row.winner {
  background: rgba(110, 231, 183, 0.08);
}

.ticket-row.winner:hover {
  background: rgba(110, 231, 183, 0.15);
}

.profit {
  font-weight: 600;
}

.profit.positive {
  color: #6ee7b7;
}

.profit.negative {
  color: #fca5a5;
}

.profit.breakeven {
  color: var(--slate-400);
}
"""


def build_html(graded_path: Path) -> str:
    """Build HTML from graded tickets workbook."""
    
    # Read sheets
    try:
        summary_df = pd.read_excel(graded_path, sheet_name="SUMMARY")
        tickets_df = pd.read_excel(graded_path, sheet_name="TICKET_RESULTS")
        legs_df = pd.read_excel(graded_path, sheet_name="LEG_RESULTS")
    except Exception as e:
        print(f"ERROR reading {graded_path}: {e}")
        return ""
    
    if tickets_df.empty:
        print("WARNING: No graded ticket data found — HTML not written.")
        return ""
    
    # Extract summary metrics
    summary_dict = dict(zip(summary_df["metric"], summary_df["value"]))
    
    power_tickets = int(summary_dict.get("power_tickets", 0))
    power_eligible = int(summary_dict.get("power_eligible_tickets", 0))
    power_no_actual = int(summary_dict.get("power_no_actual_tickets", 0))
    
    flex_tickets = int(summary_dict.get("flex_tickets", 0))
    flex_eligible = int(summary_dict.get("flex_eligible_tickets", 0))
    flex_no_actual = int(summary_dict.get("flex_no_actual_tickets", 0))
    
    # Overall stats
    total_tickets = len(tickets_df)
    winners = len(tickets_df[tickets_df["is_win"] == 1])
    cashers = len(tickets_df[tickets_df["is_cash"] == 1])
    total_staked = tickets_df["stake"].sum()
    total_payout = tickets_df["payout"].sum()
    total_profit = tickets_df["profit"].sum()
    
    win_rate = winners / total_tickets if total_tickets > 0 else 0
    roi = total_profit / total_staked if total_staked > 0 else 0
    
    # Date
    display_date = datetime.now().strftime("%b %d, %Y").upper()
    generated = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # KPI section
    kpi_html = f"""
    <div class="section">
      <div class="section-title">📊 Overview</div>
      <div class="kpi-grid">
        <div class="kpi-card">
          <div class="kpi-val">{total_tickets}</div>
          <div class="kpi-label">Total Tickets</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-val">{winners}</div>
          <div class="kpi-label">Winners</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-val">{pct(win_rate)}</div>
          <div class="kpi-label">Win Rate</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-val">${fmt(total_profit)}</div>
          <div class="kpi-label">Net Profit</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-val">{pct(roi)}</div>
          <div class="kpi-label">ROI</div>
        </div>
        <div class="kpi-card">
          <div class="kpi-val">${fmt(total_staked)}</div>
          <div class="kpi-label">Total Staked</div>
        </div>
      </div>
    </div>
    """
    
    # Ticket results table
    tickets_rows = []
    for _, row in tickets_df.iterrows():
        is_win = row.get("is_win", 0) == 1
        css_class = "ticket-row winner" if is_win else "ticket-row"
        
        profit = row.get("profit")
        if pd.isna(profit):
            profit_html = "—"
            profit_class = ""
        else:
            profit_val = float(profit)
            if profit_val > 0:
                profit_class = "positive"
            elif profit_val < 0:
                profit_class = "negative"
            else:
                profit_class = "breakeven"
            profit_html = f"<span class='profit {profit_class}'>${fmt(profit_val)}</span>"
        
        stake = row.get("stake", 0)
        payout = row.get("payout")
        if pd.isna(payout):
            payout_html = "—"
        else:
            payout_html = f"${fmt(payout)}"
        
        legs = row.get("legs", 0)
        hits = row.get("hits", 0)
        misses = row.get("misses", 0)
        no_actual = row.get("no_actual", 0)
        
        tickets_rows.append(f"""
        <tr class="{css_class}">
          <td>{h(row.get("sheet", "—"))}</td>
          <td class="num">{int(row.get("ticket_no", 0))}</td>
          <td class="num">{legs}</td>
          <td class="num">{hits}/{misses}/{no_actual}</td>
          <td class="num">${fmt(stake)}</td>
          <td>{payout_html}</td>
          <td>{profit_html}</td>
          <td>{outcome_badge(row.get("payout_status", "VOID"))}</td>
        </tr>
        """)
    
    tickets_html = f"""
    <div class="section">
      <div class="section-title">🎫 Ticket Results</div>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Sheet</th>
              <th>#</th>
              <th>Legs</th>
              <th>H/M/N</th>
              <th>Stake</th>
              <th>Payout</th>
              <th>Profit</th>
              <th>Status</th>
            </tr>
          </thead>
          <tbody>
            {"".join(tickets_rows)}
          </tbody>
        </table>
      </div>
    </div>
    """
    
    # Leg results - sample (top 20)
    leg_rows = []
    for _, row in legs_df.head(20).iterrows():
        leg_rows.append(f"""
        <tr>
          <td>{h(row.get("sheet", "—"))}</td>
          <td class="num">{int(row.get("ticket_no", 0))}</td>
          <td class="num">{int(row.get("leg_no", 0))}</td>
          <td>{h(row.get("player", "—"))}</td>
          <td>{h(row.get("prop_norm", "—"))}</td>
          <td>{h(row.get("dir", "—"))}</td>
          <td class="num">{fmt(row.get("line"), 1)}</td>
          <td class="num">{fmt(row.get("actual"), 1)}</td>
          <td>{outcome_badge(row.get("leg_result", "VOID"))}</td>
        </tr>
        """)
    
    legs_html = f"""
    <div class="section">
      <div class="section-title">🦵 Leg Details (Sample)</div>
      <div class="table-wrapper">
        <table>
          <thead>
            <tr>
              <th>Sheet</th>
              <th>Ticket</th>
              <th>Leg</th>
              <th>Player</th>
              <th>Prop</th>
              <th>Dir</th>
              <th>Line</th>
              <th>Actual</th>
              <th>Result</th>
            </tr>
          </thead>
          <tbody>
            {"".join(leg_rows)}
          </tbody>
        </table>
      </div>
    </div>
    """
    
    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"/>
<meta name="viewport" content="width=device-width, initial-scale=1.0"/>
<title>Ticket Eval — {display_date}</title>
<style>{CSS}</style>
</head>
<body>

<header>
  <div class="logo-title">TICKET EVALUATION</div>
  <div class="logo-sub">{display_date}</div>
  <div class="date-badge">📅 {display_date}</div>
</header>

<div class="main">
  {kpi_html}
  {tickets_html}
  {legs_html}
  <div class="footer">Generated {generated} — {graded_path.name}</div>
</div>

</body>
</html>"""
    
    return html


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--date",    type=str)
    parser.add_argument("--graded",  type=str, required=True)
    parser.add_argument("--out",     type=str, required=True)
    args = parser.parse_args()
    
    graded_path = Path(args.graded).resolve()
    if not graded_path.exists():
        print(f"ERROR: Not found: {graded_path}")
        sys.exit(1)
    
    html = build_html(graded_path)
    if not html:
        sys.exit(1)
    
    out = Path(args.out).resolve()
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(html, encoding="utf-8")
    print(f"✅ Wrote ticket eval HTML → {out}")
    print(f"   {len(html):,} bytes")


if __name__ == "__main__":
    main()
