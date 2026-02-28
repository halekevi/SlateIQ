#!/usr/bin/env python3
"""
extract_nba_slate.py

Extracts the 'NBA Slate' tab from combined_slate_tickets_YYYY-MM-DD.xlsx
and writes a normalized xlsx that slate_grader.py can consume directly.

Maps ticket columns -> slate_grader expected columns:
  Player       -> Player
  Team         -> Team
  Opp          -> Opp
  Prop         -> Prop Type        (slate_grader normalizes prop names internally)
  Pick Type    -> Pick Type
  Line         -> Line
  Dir          -> Direction
  Tier         -> Tier
  Def Tier     -> Def Tier
  Min Tier     -> Min Tier
  Shot Role    -> Shot Role
  Usage Role   -> Usage Role
  Edge         -> Edge
  Proj         -> Projection
  Rank Score   -> Rank Score
  Game Time    -> Game Time

Usage:
  py -3.14 extract_nba_slate.py --tickets combined_slate_tickets_2026-02-26.xlsx --out nba_slate_2026-02-26.xlsx
"""

import argparse
import pandas as pd
from pathlib import Path


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--tickets", required=True, help="combined_slate_tickets_YYYY-MM-DD.xlsx")
    ap.add_argument("--out",     required=True, help="Output xlsx path")
    ap.add_argument("--sheet",   default="NBA Slate", help="Sheet name to extract (default: 'NBA Slate')")
    args = ap.parse_args()

    xl = pd.ExcelFile(args.tickets)
    if args.sheet not in xl.sheet_names:
        raise RuntimeError(f"Sheet '{args.sheet}' not found. Available: {xl.sheet_names}")

    df = xl.parse(args.sheet, dtype=str).fillna("")

    # Rename ticket columns to exactly match step8 ALL sheet column names
    # that slate_grader.py expects
    col_map = {
        "Dir":        "Direction",
        "Proj":       "Projection",
        "Hit Rate":   "Hit Rate (5g)",
        "L5 Avg":     "Last 5 Avg",
        "Szn Avg":    "Season Avg",
    }
    df = df.rename(columns=col_map)

    # Drop columns slate_grader doesn't need
    df = df.drop(columns=["Sport", "cx"], errors="ignore")

    # slate_grader.py looks for sheet named 'ALL'
    out = Path(args.out)
    with pd.ExcelWriter(out, engine="openpyxl") as writer:
        df.to_excel(writer, sheet_name="ALL", index=False)

    print(f"Extracted {len(df)} rows -> {out}")
    print(f"Columns: {df.columns.tolist()}")


if __name__ == "__main__":
    main()
