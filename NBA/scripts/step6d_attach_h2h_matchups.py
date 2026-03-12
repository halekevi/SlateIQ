#!/usr/bin/env python3
"""
step6d_attach_h2h_matchups.py (NBA) - FIXED dtype handling
"""

from __future__ import annotations

import sys as _sys
try:
    _sys.stdout.reconfigure(encoding="utf-8")
    _sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import argparse
import unicodedata
import numpy as np
import pandas as pd


def _normalize_team(team_str: str) -> str:
    """Normalize team abbreviation."""
    if pd.isna(team_str):
        return ""
    t = str(team_str).strip().upper()
    espn_map = {
        "NY": "NYK", "NO": "NOP", "SA": "SAS", "GS": "GSW",
        "BKN": "BRK", "PHO": "PHX", "WSH": "WAS", "UTAH": "UTA"
    }
    return espn_map.get(t, t)


def _normalize_name(name_str: str) -> str:
    """Normalize player name (remove accents)."""
    if not isinstance(name_str, str):
        return ""
    nfkd = unicodedata.normalize('NFKD', name_str)
    clean = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    return clean.strip().lower()


def get_stat_col_for_prop(prop_type: str) -> str:
    """Map prop type to stat column in cache."""
    prop_map = {
        "Points": "PTS",
        "Rebounds": "REB",
        "Assists": "AST",
        "Steals": "STL",
        "Blocks": "BLK",
        "Turnovers": "TO",
    }
    return prop_map.get(prop_type, "PTS")


def build_opponent_lookup(cache: pd.DataFrame) -> dict:
    """Build (EVENT_ID, TEAM) -> opponent_team mapping with normalized teams."""
    lookup = {}
    for event_id, group in cache.groupby('EVENT_ID'):
        teams = group['TEAM'].unique()
        if len(teams) == 2:
            team_a = _normalize_team(teams[0])
            team_b = _normalize_team(teams[1])
            lookup[(event_id, team_a)] = team_b
            lookup[(event_id, team_b)] = team_a
    return lookup


def get_last_game_vs_opponent(
    player: str,
    player_team: str,
    opp_team: str,
    prop_type: str,
    cache: pd.DataFrame,
    opponent_lookup: dict
) -> dict:
    """Find player's last game vs opponent and return stat from that game."""
    result = {
        "h2h_last_stat": np.nan,
        "h2h_last_date": "",
        "h2h_games_vs_opp": 0,
        "h2h_avg": np.nan,
        "h2h_over_rate": np.nan,
    }
    
    if not player or not opp_team:
        return result
    
    player_norm = _normalize_name(player)
    opp_norm = _normalize_team(str(opp_team))
    player_team_norm = _normalize_team(str(player_team))
    stat_col = get_stat_col_for_prop(prop_type)
    
    # Find all games where this player played on their team
    player_games = cache[
        (cache["PLAYER_NORM"] == player_norm) &
        (cache["TEAM"] == player_team_norm)
    ].copy()
    
    if len(player_games) == 0:
        return result
    
    # Filter to games vs opponent
    h2h_games = []
    for _, row in player_games.iterrows():
        event_id = row['EVENT_ID']
        team = _normalize_team(row['TEAM'])  # Normalize for lookup
        opponent = opponent_lookup.get((event_id, team))
        
        if opponent and opponent == opp_norm:
            h2h_games.append(row)
    
    if not h2h_games:
        return result
    
    # Sort by date and get the LAST game
    h2h_df = pd.DataFrame(h2h_games).sort_values('GAME_DATE', ascending=True)
    result["h2h_games_vs_opp"] = len(h2h_df)

    # Use up to last 10 H2H games for avg / over rate (line needed from caller — computed in main)
    recent = h2h_df.tail(10)
    stat_vals = pd.to_numeric(recent[stat_col], errors='coerce').dropna()
    if len(stat_vals) > 0:
        result["h2h_avg"] = round(float(stat_vals.mean()), 2)

    last_game = h2h_df.iloc[-1]
    result["h2h_last_stat"] = last_game[stat_col]
    result["h2h_last_date"] = str(last_game['GAME_DATE'])

    return result


def main():
    parser = argparse.ArgumentParser(description="Attach last game vs opponent stats")
    parser.add_argument("--input", required=True)
    parser.add_argument("--cache", default="", help="ESPN boxscore cache CSV (optional — fills NaN if missing)")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    H2H_COLS = ["h2h_last_stat", "h2h_last_date", "h2h_games_vs_opp", "h2h_avg", "h2h_over_rate"]

    print("╔════════════════════════════════════════════════════════════════════════════╗")
    print("║        SlateIQ-NBA-S6d: Last Game vs Opponent (Opponent Normalized)        ║")
    print("╚════════════════════════════════════════════════════════════════════════════╝")
    print()

    print(f"[S6d] Loading: {args.input}")
    df = pd.read_csv(args.input, dtype={'nba_player_id': str}, low_memory=False)
    print(f"  {len(df)} rows")

    # ── Load cache (graceful fallback if missing or empty) ─────────────────────
    cache = None
    if args.cache:
        try:
            cache = pd.read_csv(args.cache, dtype={'ESPN_ATHLETE_ID': str}, low_memory=False)
            print(f"[S6d] Loading cache: {args.cache}  ({len(cache)} rows)")
            if len(cache) == 0:
                print("  ⚠️  Cache is empty — filling H2H columns with NaN")
                cache = None
        except FileNotFoundError:
            print(f"  ⚠️  Cache not found: {args.cache} — filling H2H columns with NaN")
            cache = None
    else:
        print("  ⚠️  No --cache supplied — filling H2H columns with NaN")

    if cache is None:
        for col in H2H_COLS:
            df[col] = np.nan
        df.to_csv(args.output, index=False)
        print(f"[S6d] ✅ Saved (without H2H stats) → {args.output}")
        return

    print(f"  {len(cache)} rows")
    
    print("[S6d] Building opponent lookup with normalized teams...")
    opponent_lookup = build_opponent_lookup(cache)
    print(f"  {len(opponent_lookup)} (event_id, team) -> opponent mappings")
    
    print()
    print("[S6d] Computing last game vs opponent for each player prop...")

    try:
        from tqdm import tqdm as _tqdm
    except ImportError:
        import subprocess as _sp, sys as _sys
        _sp.check_call([_sys.executable, "-m", "pip", "install", "tqdm", "--break-system-packages", "-q"])
        from tqdm import tqdm as _tqdm

    # Collect results in lists to avoid pandas dtype issues
    h2h_last_stats = []
    h2h_last_dates = []
    h2h_games_vs_opps = []
    h2h_avgs = []
    h2h_over_rates = []

    matched_count = 0
    for idx, row in _tqdm(df.iterrows(), total=len(df), desc="S6d h2h lookup", unit="row"):
        stats = get_last_game_vs_opponent(
            player=row.get("player", ""),
            player_team=row.get("team", ""),
            opp_team=row.get("opp_team", ""),
            prop_type=row.get("prop_type", "Points"),
            cache=cache,
            opponent_lookup=opponent_lookup
        )

        if pd.notna(stats["h2h_last_stat"]):
            matched_count += 1

        # Compute over rate: fraction of H2H avg games where stat > line
        h2h_avg_val = stats.get("h2h_avg", np.nan)
        line_val = row.get("line", np.nan)
        over_rate = np.nan
        if pd.notna(h2h_avg_val) and pd.notna(line_val):
            try:
                line_f = float(line_val)
                # Approximate: use avg vs line as a proxy (full game-by-game needs cache pass-through)
                # Will be replaced with exact count when cache rows are available per player
                over_rate = round(float(h2h_avg_val > line_f), 2)
            except Exception:
                pass

        h2h_last_stats.append(stats['h2h_last_stat'])
        h2h_last_dates.append(stats['h2h_last_date'])
        h2h_games_vs_opps.append(stats['h2h_games_vs_opp'])
        h2h_avgs.append(h2h_avg_val)
        h2h_over_rates.append(over_rate)

    # Add columns all at once
    df['h2h_last_stat'] = h2h_last_stats
    df['h2h_last_date'] = h2h_last_dates
    df['h2h_games_vs_opp'] = h2h_games_vs_opps
    df['h2h_avg'] = h2h_avgs
    df['h2h_over_rate'] = h2h_over_rates
    
    print()
    print("[S6d] Results:")
    print(f"  ✓ Matched {matched_count}/{len(df)} player-vs-opponent combos ({100*matched_count/len(df):.1f}%)")
    print(f"  ✓ h2h_last_stat filled:  {df['h2h_last_stat'].notna().sum()}/{len(df)}")
    print(f"  ✓ h2h_avg filled:        {df['h2h_avg'].notna().sum()}/{len(df)}")
    print(f"  ✓ h2h_over_rate filled:  {df['h2h_over_rate'].notna().sum()}/{len(df)}")
    print()
    
    print(f"[S6d] ✅ Saving to {args.output}")
    df.to_csv(args.output, index=False)
    print("[S6d] Done!")
    print()


if __name__ == "__main__":
    main()
