"""
Step 4 — Pull NHL Player Game Logs + Build Stats Cache
Uses api-web.nhle.com to pull per-game stats for each player on the board.
Computes derived stats: fantasy_score, shooting_pct, save_pct.

Usage:
    py step4_attach_player_stats_nhl.py --input step3_nhl_with_defense.csv \
        --cache nhl_stats_cache.csv --output step4_nhl_with_stats.csv --season 20242025
"""

import argparse
import csv
import json
import os
import time
import urllib.request
from datetime import datetime

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json"}
NHL_WEB = "https://api-web.nhle.com/v1"


def current_nhl_season() -> str:
    """Auto-detect current NHL season string (e.g. 20252026).
    NHL season starts in October — if month >= 10 use current year, else prior year.
    """
    now = datetime.now()
    start_year = now.year if now.month >= 10 else now.year - 1
    return f"{start_year}{start_year + 1}"

# PrizePicks NHL Fantasy Score formula
# Goals×8 + Assists×5 + SOG×1.5 + Hits×1.3 + Blocks×1.3
# (Goalies: Saves×0.6 - GoalsAllowed×3 + Win bonus 6)
FS_WEIGHTS = {
    "goals": 8.0,
    "assists": 5.0,
    "shots_on_goal": 1.5,
    "hits": 1.3,
    "blocked_shots": 1.3,
}
GOALIE_WEIGHTS = {
    "saves": 0.6,
    "goals_allowed": -3.0,
}


def fetch_json(url: str, retries: int = 3) -> dict:
    for attempt in range(retries):
        try:
            req = urllib.request.Request(url, headers=HEADERS)
            with urllib.request.urlopen(req, timeout=15) as resp:
                return json.loads(resp.read().decode())
        except Exception as exc:
            if attempt == retries - 1:
                return {}
            time.sleep(1.5 ** attempt)


def get_skater_gamelogs(nhl_id: str, season: str) -> list[dict]:
    """Fetch skater game log from NHL API."""
    url = f"{NHL_WEB}/player/{nhl_id}/game-log/{season}/2"  # 2 = regular season
    data = fetch_json(url)
    games = data.get("gameLog", [])
    return games


def get_goalie_gamelogs(nhl_id: str, season: str) -> list[dict]:
    """Fetch goalie game log from NHL API."""
    url = f"{NHL_WEB}/player/{nhl_id}/game-log/{season}/2"
    data = fetch_json(url)
    games = data.get("gameLog", [])
    return games


def parse_skater_game(game: dict) -> dict:
    goals = int(game.get("goals", 0) or 0)
    assists = int(game.get("assists", 0) or 0)
    sog = int(game.get("shots", 0) or 0)
    hits = int(game.get("hits", 0) or 0)
    blocks = int(game.get("blockedShots", 0) or 0)
    pim = int(game.get("pim", 0) or 0)
    toi_str = game.get("toi", "0:00") or "0:00"
    # Convert TOI "mm:ss" to minutes
    try:
        parts = toi_str.split(":")
        toi_min = int(parts[0]) + int(parts[1]) / 60 if len(parts) == 2 else 0.0
    except Exception:
        toi_min = 0.0

    points = goals + assists
    fantasy_score = (
        goals * FS_WEIGHTS["goals"]
        + assists * FS_WEIGHTS["assists"]
        + sog * FS_WEIGHTS["shots_on_goal"]
        + hits * FS_WEIGHTS["hits"]
        + blocks * FS_WEIGHTS["blocked_shots"]
    )

    return {
        "game_date": game.get("gameDate", ""),
        "opp_abbrev": game.get("opponentAbbrev", ""),
        "home_road": game.get("homeRoadFlag", ""),
        "goals": goals,
        "assists": assists,
        "points": points,
        "shots_on_goal": sog,
        "hits": hits,
        "blocked_shots": blocks,
        "pim": pim,
        "toi_min": round(toi_min, 2),
        "fantasy_score": round(fantasy_score, 2),
    }


def parse_goalie_game(game: dict) -> dict:
    saves = int(game.get("saves", 0) or 0)
    goals_against = int(game.get("goalsAgainst", 0) or 0)
    shots_against = saves + goals_against
    decision = game.get("decision", "") or ""
    won = 1 if decision.upper() == "W" else 0
    save_pct = round(saves / max(shots_against, 1), 4)

    fantasy_score = (
        saves * GOALIE_WEIGHTS["saves"]
        + goals_against * GOALIE_WEIGHTS["goals_allowed"]
        + won * 6.0
    )

    toi_str = game.get("toi", "0:00") or "0:00"
    try:
        parts = toi_str.split(":")
        toi_min = int(parts[0]) + int(parts[1]) / 60 if len(parts) == 2 else 0.0
    except Exception:
        toi_min = 0.0

    return {
        "game_date": game.get("gameDate", ""),
        "opp_abbrev": game.get("opponentAbbrev", ""),
        "home_road": game.get("homeRoadFlag", ""),
        "saves": saves,
        "goals_allowed": goals_against,
        "shots_against": shots_against,
        "save_pct": save_pct,
        "toi_min": round(toi_min, 2),
        "decision": decision,
        "fantasy_score": round(fantasy_score, 2),
    }


def compute_rolling_stats(games: list[dict], stat_key: str, windows=(5, 10, 20)) -> dict:
    """Compute rolling averages over last N games for a given stat."""
    values = [g.get(stat_key, 0) for g in games]
    result = {}
    for w in windows:
        recent = values[:w] if len(values) >= w else values
        result[f"avg_L{w}"] = round(sum(recent) / max(len(recent), 1), 3) if recent else 0.0
    result["avg_season"] = round(sum(values) / max(len(values), 1), 3) if values else 0.0
    result["games_played"] = len(values)
    return result


def load_cache(path: str) -> dict:
    """Load existing stats cache {nhl_id: {stat: {window: avg}}}"""
    cache = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                pid = row.get("nhl_player_id", "")
                if pid:
                    cache[pid] = row
    return cache


def read_csv(path: str) -> list[dict]:
    with open(path, newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(rows: list[dict], path: str):
    if not rows:
        print("⚠️  No rows to write.")
        return
    # Union all keys across every row — some props add dynamic columns
    # (e.g. last1_faceoffs_won) that aren't present in the first row
    all_keys: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for k in row.keys():
            if k not in seen:
                all_keys.append(k)
                seen.add(k)
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=all_keys, extrasaction="ignore")
        writer.writeheader()
        writer.writerows({k: row.get(k, "") for k in all_keys} for row in rows)
    print(f"Saved {len(rows)} rows -> {path}")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="step3_nhl_with_defense.csv")
    parser.add_argument("--cache", default="nhl_stats_cache.csv")
    parser.add_argument("--output", default="step4_nhl_with_stats.csv")
    parser.add_argument("--season", default=current_nhl_season(),
                        help="NHL season string e.g. 20252026 (auto-detected by default)")
    parser.add_argument("--max-games", type=int, default=30,
                        help="Max recent games to pull per player")
    args = parser.parse_args()

    rows = read_csv(args.input)
    old_cache = load_cache(args.cache)

    # Dedupe players
    players = {}
    for row in rows:
        nhl_id = row.get("nhl_player_id", "")
        if nhl_id and nhl_id not in players:
            players[nhl_id] = row.get("player_role", "SKATER")

    print(f"Fetching stats for {len(players)} unique players (season {args.season})...")

    # Stats windows we want
    SKATER_STATS = ["goals", "assists", "points", "shots_on_goal", "hits", "blocked_shots", "fantasy_score", "toi_min"]
    GOALIE_STATS = ["saves", "goals_allowed", "shots_against", "save_pct", "fantasy_score"]

    player_stats = dict(old_cache)  # start with existing

    fetched = 0
    for nhl_id, role in players.items():
        if nhl_id in old_cache:
            continue  # already cached

        print(f"  [{fetched+1}/{len(players)}] {nhl_id} ({role})...", end=" ", flush=True)

        try:
            if role == "GOALIE":
                games_raw = get_goalie_gamelogs(nhl_id, args.season)
                games = [parse_goalie_game(g) for g in games_raw[:args.max_games]]
                stat_keys = GOALIE_STATS
            else:
                games_raw = get_skater_gamelogs(nhl_id, args.season)
                games = [parse_skater_game(g) for g in games_raw[:args.max_games]]
                stat_keys = SKATER_STATS
        except Exception as e:
            print(f"ERROR: {e} — skipping")
            fetched += 1
            continue

        print(f"{len(games)} games")

        stat_row = {
            "nhl_player_id": nhl_id,
            "player_role": role,
            "games_fetched": len(games),
        }

        for sk in stat_keys:
            rolling = compute_rolling_stats(games, sk)
            for window_key, val in rolling.items():
                stat_row[f"{sk}_{window_key}"] = val

        # Also store last 3 game raw values for context
        for i in range(min(3, len(games))):
            g = games[i]
            for sk in stat_keys:
                stat_row[f"last{i+1}_{sk}"] = g.get(sk, "")

        player_stats[nhl_id] = stat_row
        fetched += 1
        time.sleep(0.25)

    # Save cache
    if fetched > 0:
        try:
            all_keys = set()
            for v in player_stats.values():
                all_keys.update(v.keys())
            all_keys = sorted(all_keys)
            with open(args.cache, "w", newline="", encoding="utf-8") as f:
                writer = csv.DictWriter(f, fieldnames=all_keys)
                writer.writeheader()
                for stat_row in player_stats.values():
                    writer.writerow({k: stat_row.get(k, "") for k in all_keys})
        except Exception as e:
            print(f"⚠️  Cache write error: {e}")
        print(f"Stats cache updated: {len(player_stats)} players -> {args.cache}")

    # Merge stats into rows
    results = []
    for row in rows:
        nhl_id = row.get("nhl_player_id", "")
        stat_data = player_stats.get(nhl_id, {})

        # Attach relevant rolling averages for this player's prop stat
        stat_norm = row.get("stat_norm", "")
        windows = [5, 10, 20]
        for w in windows:
            key = f"{stat_norm}_avg_L{w}"
            row[f"avg_L{w}"] = stat_data.get(key, "")

        row["avg_season"] = stat_data.get(f"{stat_norm}_avg_season", "")
        # games_played: prefer "games_fetched" key (written by fresh fetch),
        # also accept "games_played" if row was loaded from an older cache version
        row["games_played"] = (
            stat_data.get("games_fetched")
            or stat_data.get("games_played")
            or ""
        )

        # Also attach fantasy score averages always
        for w in windows:
            row[f"fantasy_score_avg_L{w}"] = stat_data.get(f"fantasy_score_avg_L{w}", "")
        row["fantasy_score_avg_season"] = stat_data.get("fantasy_score_avg_season", "")

        # TOI context for skaters
        if row.get("player_role") == "SKATER":
            row["toi_avg_L10"] = stat_data.get("toi_min_avg_L10", "")
            row["toi_avg_season"] = stat_data.get("toi_min_avg_season", "")

        # Attach last 3 individual game values for the prop's stat (for direction context)
        for i in range(1, 4):
            row[f"last{i}_{stat_norm}"] = stat_data.get(f"last{i}_{stat_norm}", "")

        # Attach last 3 fantasy scores regardless of stat
        for i in range(1, 4):
            row[f"last{i}_fantasy_score"] = stat_data.get(f"last{i}_fantasy_score", "")

        results.append(row)

    write_csv(results, args.output)
    print(f"\nDone. {len(results)} props with stats attached.")


if __name__ == "__main__":
    main()
