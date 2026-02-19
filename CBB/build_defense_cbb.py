#!/usr/bin/env python3
"""
build_defense_cbb.py  (optimized)

Builds opponent-defense context from cbb_cache.csv.

OPTIMIZATIONS vs original:
- TEAM_ALIASES imported from shared config (or redefined once at top)
- pick_col() uses a dict comprehension instead of linear scan
- norm_team() called once per column via Series.map() instead of .apply()
- Removed redundant .copy() calls after merges
- valid_games filter uses isin() on pre-computed set (no second groupby)
- Tiers computed via pd.cut() — vectorised, avoids Python-level loop
- Outputs BOTH opp_team + team_abbr (backward-compat)

Usage:
  py -3.14 build_defense_cbb.py --cache cbb_cache.csv --output cbb_defense_all.csv --n 10
"""

from __future__ import annotations

import argparse
import pandas as pd
import numpy as np

TEAM_ALIASES: dict[str, str] = {
    "GCU":   "GC",
    "NEVADA":"NEV",
    "SDST":  "SDSU",
    "MIZ":   "MIZZ",
    "NCSU":  "NCST",
    "GTECH": "GT",
    "":      "",
}


def norm_team(s: str) -> str:
    t = str(s or "").strip().upper()
    return TEAM_ALIASES.get(t, t)


def pick_col(cols: list[str], candidates: list[str]) -> str:
    # O(1) lookup via dict instead of repeated linear scan
    lower_map = {c.lower(): c for c in cols}
    return next((lower_map[c.lower()] for c in candidates if c.lower() in lower_map), "")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache",  default="cbb_cache.csv")
    ap.add_argument("--output", default="cbb_defense_all.csv")
    ap.add_argument("--n", type=int, default=10, help="Last-N games window")
    args = ap.parse_args()

    df = pd.read_csv(args.cache, dtype=str).fillna("")
    if df.empty:
        raise SystemExit(f"❌ Cache is empty: {args.cache}")

    cols = df.columns.tolist()
    team_col = pick_col(cols, ["team_abbr", "team", "abbr"])
    date_col = pick_col(cols, ["game_date", "date"])
    game_col = pick_col(cols, ["event_id", "game_id", "gameid", "id"])
    pts_col  = pick_col(cols, ["PTS", "pts", "points"])

    if not all([team_col, date_col, game_col, pts_col]):
        raise SystemExit(
            "❌ Cache missing required columns.\n"
            f"team={team_col!r} date={date_col!r} game={game_col!r} pts={pts_col!r}\n"
            f"Available: {cols}"
        )

    # Normalise team codes via vectorised map (fast for large caches)
    df[team_col] = df[team_col].str.strip().str.upper().map(
        lambda t: TEAM_ALIASES.get(t, t)
    )
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df[pts_col]  = pd.to_numeric(df[pts_col], errors="coerce").fillna(0.0)

    # Team points per game
    team_game = (
        df.groupby([game_col, team_col], as_index=False)
          .agg(team_pts=(pts_col, "sum"), game_date=(date_col, "max"))
    )

    # Keep only games with exactly 2 teams (needed for opponent calc)
    two_team_games = (
        team_game.groupby(game_col)[team_col]
                 .nunique()
                 .pipe(lambda s: s[s == 2].index)
    )
    team_game = team_game[team_game[game_col].isin(two_team_games)]

    # Self-join to get opponent points
    merged = team_game.merge(
        team_game.rename(columns={team_col: "opp_team", "team_pts": "opp_pts"})
                 [[game_col, "opp_team", "opp_pts"]],
        on=game_col
    )
    merged = merged[merged[team_col] != merged["opp_team"]]

    # One row per (game, team) — keep first after dedup
    merged = (
        merged.sort_values([game_col, team_col])
              .drop_duplicates(subset=[game_col, team_col], keep="first")
    )
    merged["pts_allowed"] = pd.to_numeric(merged["opp_pts"], errors="coerce").fillna(0.0)

    # Last-N games per team by date
    merged = merged.sort_values([team_col, "game_date"])
    merged["g_rev"] = merged.groupby(team_col).cumcount(ascending=False) + 1
    lastn = merged[merged["g_rev"] <= args.n]

    out = (
        lastn.groupby(team_col, as_index=False)
             .agg(
                 OPP_PTS_ALLOWED_LN=("pts_allowed", "mean"),
                 OPP_PTS_ALLOWED_GAMES=("pts_allowed", "count"),
             )
    )

    out["OPP_PTS_ALLOWED_RANK"] = (
        out["OPP_PTS_ALLOWED_LN"].rank(method="min", ascending=True).astype(int)
    )
    out["OPP_DEF_RANK"] = out["OPP_PTS_ALLOWED_RANK"]

    # Vectorised tier assignment via pd.cut on rank percentiles
    q25, q50, q75 = out["OPP_DEF_RANK"].quantile([0.25, 0.50, 0.75])
    n_teams = len(out)
    out["DEF_TIER"] = pd.cut(
        out["OPP_DEF_RANK"],
        bins=[0, q25, q50, q75, n_teams + 1],
        labels=["ELITE", "GOOD", "AVERAGE", "WEAK"],
        right=True,
    ).astype(str)

    out = out.rename(columns={team_col: "opp_team"})
    out["team_abbr"] = out["opp_team"]    # backward-compat alias

    out.to_csv(args.output, index=False, encoding="utf-8")
    print(
        f"✅ Wrote → {args.output} | rows={len(out)} "
        f"| window=last{args.n} | teams={out['opp_team'].nunique()}"
    )


if __name__ == "__main__":
    main()
