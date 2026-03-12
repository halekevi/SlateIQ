#!/usr/bin/env python3
"""
step4_attach_player_stats_espn_cache.py  (DB version)

Replaces live ESPN API fetching with indexed reads from slateiq_ref.db.
The DB is populated nightly by build_boxscore_ref.py (called from run_grader.ps1).

Usage:
    py step4_attach_player_stats_espn_cache.py \
        --slate step3_with_defense.csv \
        --out   step4_with_stats.csv \
        --date  2026-03-09

Required DB column: ESPN_ATHLETE_ID (populated by step2/step1 ID attach).
If your slate uses nba_player_id instead, pass --id-col nba_player_id
and ensure it maps to ESPN IDs via the idmap (or use step5a to pre-attach).
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

# Allow running from any working directory
# Walk up from this file to find scripts/step4_db_reader.py
_here = Path(__file__).resolve().parent
for _ in range(6):
    if (_here / "scripts" / "step4_db_reader.py").exists():
        sys.path.insert(0, str(_here / "scripts"))
        break
    _here = _here.parent
from step4_db_reader import open_db, attach_stats, db_summary, DB_PATH


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate",    default="step3_with_defense.csv")
    ap.add_argument("--out",      default="step4_with_stats.csv")
    ap.add_argument("--date",     default="",   help="Slate date YYYY-MM-DD (informational)")
    ap.add_argument("--n",        type=int, default=10, help="Max games to pull per player")
    ap.add_argument("--id-col",   default="ESPN_ATHLETE_ID",
                    help="Column containing ESPN athlete ID (default: ESPN_ATHLETE_ID)")
    ap.add_argument("--db",       default="", help="Override DB path")
    ap.add_argument("--summary",  action="store_true", help="Print DB summary and exit")
    args = ap.parse_args()

    db_path = Path(args.db) if args.db else DB_PATH
    con = open_db(db_path)

    if args.summary:
        db_summary(con)
        return

    print(f"→ Loading slate: {args.slate}")
    slate = pd.read_csv(args.slate, dtype=str, encoding="utf-8-sig").fillna("")
    print(f"  {len(slate)} rows")

    # Fallback: if id_col not present but nba_player_id is, warn and use it
    id_col = args.id_col
    if id_col not in slate.columns:
        fallbacks = ["ESPN_ATHLETE_ID", "espn_athlete_id", "nba_player_id"]
        for fb in fallbacks:
            if fb in slate.columns:
                print(f"  ⚠️  '{id_col}' not found — using '{fb}' instead")
                id_col = fb
                break
        else:
            raise SystemExit(f"No ID column found. Columns: {list(slate.columns)}")

    # ── Bridge nba_player_id → ESPN_ATHLETE_ID via DB ─────────────────────────
    # The DB nba table stores espn_athlete_id per row. Build a name→espn_id map
    # so attach_stats uses ESPN IDs (primary key) instead of nba_player_ids.
    if id_col == "nba_player_id" and "ESPN_ATHLETE_ID" not in slate.columns:
        print("→ Building ESPN ID bridge from DB (player name lookup)...")
        rows = con.execute(
            "SELECT player, espn_athlete_id FROM nba "
            "WHERE espn_athlete_id IS NOT NULL "
            "GROUP BY player, espn_athlete_id"
        ).fetchall()
        name_to_espn = {r[0].strip().lower(): r[1] for r in rows if r[0] and r[1]}

        slate["ESPN_ATHLETE_ID"] = slate["player"].str.strip().str.lower().map(
            lambda n: name_to_espn.get(n, "")
        )
        bridged = (slate["ESPN_ATHLETE_ID"] != "").sum()
        print(f"  Bridged {bridged}/{len(slate)} rows to ESPN IDs")
        id_col = "ESPN_ATHLETE_ID"

    print(f"\n→ Attaching NBA stats from DB (id_col={id_col}, n={args.n})...")
    slate, counts = attach_stats(slate, "nba", con, id_col=id_col, n=args.n)

    slate.to_csv(args.out, index=False, encoding="utf-8-sig")
    print(f"\n✅ Saved → {args.out}  ({len(slate)} rows)")
    print("\nstat_status breakdown:")
    for k, v in sorted(counts.items(), key=lambda x: -x[1]):
        if v > 0:
            print(f"  {k:25s} {v:>5}")


if __name__ == "__main__":
    main()
