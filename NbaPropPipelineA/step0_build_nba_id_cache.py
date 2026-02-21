#!/usr/bin/env python3
"""
step0_build_nba_id_cache.py — NBA Player ID Resolver + Cache Warmer

PURPOSE:
  Builds/updates player_id_map.csv by pulling all 30 NBA rosters from ESPN,
  matching each player to their NBA stats API ID, then pre-warming the
  _nba_cache with game logs so step4 runs cleanly with no NO_NBA_ID rows.

WHAT IT DOES:
  1. Fetches all 30 NBA team rosters from ESPN's public API
  2. Matches ESPN players → nba_api static player list via name matching
  3. Merges with existing player_id_map.csv (preserves all existing entries)
  4. Saves updated player_id_map.csv
  5. (Optional) Pre-warms _nba_cache by fetching game logs for all resolved players

USAGE:
  # Just build/update the ID map:
  py -3 step0_build_nba_id_cache.py

  # Also pre-warm the cache (fetches game logs — takes a few minutes):
  py -3 step0_build_nba_id_cache.py --warm-cache

  # Specify paths:
  py -3 step0_build_nba_id_cache.py \
    --id-map NbaPropPipelineA/player_id_map.csv \
    --cache-dir NbaPropPipelineA/_nba_cache \
    --season 2025-26 \
    --warm-cache

RUN FROM: NBA-Pipelines root directory
"""

from __future__ import annotations

# ── IPv4 patch — must happen before nba_api import ────────────────────────────
import socket as _socket
_orig_getaddrinfo = _socket.getaddrinfo
def _ipv4_only(host, port, family=0, type=0, proto=0, flags=0):
    results = _orig_getaddrinfo(host, port, family, type, proto, flags)
    ipv4 = [r for r in results if r[0] == _socket.AF_INET]
    return ipv4 if ipv4 else results
_socket.getaddrinfo = _ipv4_only
print("🔧 IPv4 mode: ON")

import argparse
import time
import re
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# ── ESPN team IDs for all 30 NBA teams ────────────────────────────────────────
ESPN_NBA_TEAMS = {
    "ATL": 1,  "BOS": 2,  "BKN": 17, "CHA": 30, "CHI": 4,
    "CLE": 5,  "DAL": 6,  "DEN": 7,  "DET": 8,  "GSW": 9,
    "HOU": 10, "IND": 11, "LAC": 12, "LAL": 13, "MEM": 29,
    "MIA": 14, "MIL": 15, "MIN": 16, "NOP": 3,  "NYK": 18,
    "OKC": 25, "ORL": 19, "PHI": 20, "PHX": 21, "POR": 22,
    "SAC": 23, "SAS": 24, "TOR": 28, "UTA": 26, "WAS": 27,
}

ESPN_ROSTER_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/nba/teams/{team_id}/roster"


def _make_session() -> requests.Session:
    session = requests.Session()
    retry = Retry(total=3, backoff_factor=1, status_forcelist=[500, 502, 503, 504])
    adapter = HTTPAdapter(max_retries=retry)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
    })
    return session


def _normalize_name(name: str) -> str:
    """Lowercase, strip diacritics/accents/punctuation for fuzzy matching."""
    import unicodedata
    # Normalize unicode: decompose accented chars (é → e + combining accent)
    name = unicodedata.normalize("NFD", name)
    # Drop combining characters (the accent marks)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().strip()
    # Remove suffixes
    name = re.sub(r'\b(jr\.?|sr\.?|ii|iii|iv)\b', '', name).strip()
    # Remove punctuation except spaces (handles apostrophes, hyphens, dots)
    name = re.sub(r"[^a-z\s]", "", name)
    return re.sub(r'\s+', ' ', name).strip()


def fetch_espn_rosters(session: requests.Session, sleep: float = 0.3) -> pd.DataFrame:
    """Pull all 30 NBA rosters from ESPN. Returns DataFrame with espn_id, name, team."""
    rows = []
    print(f"\n📋 Fetching ESPN rosters for all 30 NBA teams...\n")
    for abbr, team_id in ESPN_NBA_TEAMS.items():
        try:
            url = ESPN_ROSTER_URL.format(team_id=team_id)
            resp = session.get(url, timeout=15)
            resp.raise_for_status()
            data = resp.json()

            # ESPN roster structure: athletes array at root or under 'athletes'
            athletes = []
            if "athletes" in data:
                raw = data["athletes"]
                # Sometimes it's a list of position groups
                if isinstance(raw, list) and len(raw) > 0:
                    if isinstance(raw[0], dict) and "items" in raw[0]:
                        for grp in raw:
                            athletes.extend(grp.get("items", []))
                    else:
                        athletes = raw

            for a in athletes:
                espn_id = str(a.get("id", ""))
                full_name = a.get("fullName", "") or a.get("displayName", "")
                if espn_id and full_name:
                    rows.append({
                        "espn_id": espn_id,
                        "espn_name": full_name,
                        "team": abbr,
                        "_norm": _normalize_name(full_name),
                    })

            print(f"  ✅ {abbr}: {len(athletes)} players")
            time.sleep(sleep)

        except Exception as e:
            print(f"  ❌ {abbr} (team_id={team_id}): {e}")

    df = pd.DataFrame(rows).drop_duplicates(subset=["espn_id"])
    print(f"\n→ Total ESPN players fetched: {len(df)}")
    return df


def load_nba_api_players() -> pd.DataFrame:
    """
    Load all players from nba_api static list (no network call).
    Returns ALL players (active + inactive) so we can match players
    that nba_api incorrectly flags as inactive but are still playing.
    """
    try:
        from nba_api.stats.static import players as nba_players
        all_players = nba_players.get_players()
        df = pd.DataFrame(all_players)
        active = df[df["is_active"] == True]
        print(f"→ nba_api static players: {len(df)} total, {len(active)} active")
        # Return ALL — match_players will prefer active but fall back to inactive
        df["_norm"] = df["full_name"].apply(_normalize_name)
        return df
    except ImportError:
        print("❌ nba_api not installed. Run: pip install nba_api")
        raise


def match_players(espn_df: pd.DataFrame, nba_df: pd.DataFrame) -> pd.DataFrame:
    """
    Match ESPN players to nba_api IDs using 4-tier name matching:
      1. Exact normalized match vs ACTIVE players (handles diacritics/suffixes)
      2. First+last name only vs ACTIVE players (handles middle names)
      3. difflib fuzzy vs ACTIVE players (cutoff=0.88)
      4. All tiers repeated vs ALL players (catches ESPN-active but nba_api-inactive)
    ESPN roster is ground truth for activity status.
    """
    import difflib
    print("\n🔗 Matching ESPN players to NBA API IDs...")

    active_df = nba_df[nba_df["is_active"] == True]

    def build_lookup(df):
        lookup, norms = {}, []
        for _, row in df.iterrows():
            n = row["_norm"]
            if n not in lookup:
                lookup[n] = row
                norms.append(n)
        return lookup, norms

    active_lookup, active_norms = build_lookup(active_df)
    all_lookup,    all_norms    = build_lookup(nba_df)

    def try_match(norm, lookup, norms):
        # T1: exact normalized
        row = lookup.get(norm)
        if row is not None:
            return row, "strict"
        # T2: first + last only
        parts = norm.split()
        if len(parts) >= 2:
            row = lookup.get(f"{parts[0]} {parts[-1]}")
            if row is not None:
                return row, "loose"
        # T3: fuzzy
        close = difflib.get_close_matches(norm, norms, n=1, cutoff=0.88)
        if close:
            return lookup[close[0]], f"fuzzy({close[0]})"
        return None, None

    matched, unmatched = [], []

    for _, espn_row in espn_df.iterrows():
        norm = espn_row["_norm"]

        # Try active players first
        nba_row, res = try_match(norm, active_lookup, active_norms)
        prefix = "active"

        # Fallback to ALL (ESPN is ground truth — player is active if on their roster)
        if nba_row is None:
            nba_row, res = try_match(norm, all_lookup, all_norms)
            prefix = "inactive_override"

        if nba_row is not None:
            matched.append({
                "espn_id":         espn_row["espn_id"],
                "espn_name":       espn_row["espn_name"],
                "team":            espn_row["team"],
                "nba_player_id":   str(int(nba_row["id"])),
                "nba_player_name": nba_row["full_name"],
                "resolver":        f"espn_{prefix}_{res}",
            })
        else:
            unmatched.append(espn_row["espn_name"])

    result = pd.DataFrame(matched)
    print(f"  ✅ Matched: {len(matched)}")

    if not result.empty:
        rc = result["resolver"].value_counts()
        for r, c in rc.items():
            print(f"     {r}: {c}")

    if unmatched:
        print(f"  ⚠️  Still unmatched ({len(unmatched)}) — likely pure G-League, not in nba_api:")
        for name in unmatched:
            print(f"     • {name}")
    else:
        print(f"  🎯 100% match rate!")

    return result


def merge_with_existing(new_df: pd.DataFrame, existing_path: Path) -> pd.DataFrame:
    """
    Merge new ESPN-resolved IDs with existing player_id_map.csv.
    Existing entries are preserved. New players are added.
    """
    if not existing_path.exists():
        print(f"  ℹ️  No existing map found at {existing_path} — creating fresh")
        # Build in same format as existing player_id_map.csv
        out = new_df[["espn_id", "espn_name", "team", "nba_player_id", "nba_player_name", "resolver"]].copy()
        out.columns = ["player_id", "player_name_pp", "team_pp", "nba_player_id", "nba_player_name", "resolver"]
        return out

    existing = pd.read_csv(existing_path, dtype=str).fillna("")
    print(f"\n📂 Existing map: {len(existing)} entries")

    # Existing nba_player_ids already covered
    existing_nba_ids = set(existing["nba_player_id"].astype(str).str.strip())

    # Filter new_df to only players not already in map
    new_only = new_df[~new_df["nba_player_id"].astype(str).isin(existing_nba_ids)].copy()
    print(f"  → New players to add: {len(new_only)}")

    if len(new_only) == 0:
        print("  ✅ Map already up to date — no new players to add")
        return existing

    # Format new rows to match existing schema
    new_rows = new_only[["espn_id", "espn_name", "team", "nba_player_id", "nba_player_name", "resolver"]].copy()
    new_rows.columns = ["player_id", "player_name_pp", "team_pp", "nba_player_id", "nba_player_name", "resolver"]

    merged = pd.concat([existing, new_rows], ignore_index=True)
    print(f"  ✅ Merged map: {len(merged)} total entries ({len(new_only)} added)")
    return merged


# ── Browser-grade headers that stats.nba.com expects ──────────────────────────
# nba_api's default headers get fingerprinted and connection-reset.
# These mirror what Chrome sends when browsing stats.nba.com directly.
_NBA_HEADERS = {
    "Host":                      "stats.nba.com",
    "Connection":                "keep-alive",
    "Cache-Control":             "max-age=0",
    "Upgrade-Insecure-Requests": "1",
    "User-Agent":                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                                 "AppleWebKit/537.36 (KHTML, like Gecko) "
                                 "Chrome/121.0.0.0 Safari/537.36",
    "Accept":                    "application/json, text/plain, */*",
    "Accept-Language":           "en-US,en;q=0.9",
    "Accept-Encoding":           "gzip, deflate, br",
    "x-nba-stats-origin":        "stats",
    "x-nba-stats-token":         "true",
    "Origin":                    "https://www.nba.com",
    "Referer":                   "https://www.nba.com/",
    "Sec-Fetch-Dest":            "empty",
    "Sec-Fetch-Mode":            "cors",
    "Sec-Fetch-Site":            "same-site",
    "sec-ch-ua":                 '"Not A(Brand";v="99", "Google Chrome";v="121", "Chromium";v="121"',
    "sec-ch-ua-mobile":          "?0",
    "sec-ch-ua-platform":        '"Windows"',
}

_NBA_GAMELOG_URL = (
    "https://stats.nba.com/stats/playergamelog"
    "?PlayerID={player_id}"
    "&Season={season}"
    "&SeasonType=Regular+Season"
    "&LeagueID=00"
)


def _fetch_gamelog_direct(pid: int, season: str, session: requests.Session, timeout: float) -> pd.DataFrame:
    """
    Fetch a player game log by hitting stats.nba.com directly with browser headers,
    bypassing nba_api's internal HTTP client which gets connection-reset.
    """
    url = _NBA_GAMELOG_URL.format(player_id=pid, season=season)
    resp = session.get(url, headers=_NBA_HEADERS, timeout=timeout)
    resp.raise_for_status()
    data = resp.json()

    # stats.nba.com returns {resultSets: [{headers: [...], rowSet: [...]}]}
    rs = data["resultSets"][0]
    headers = rs["headers"]
    rows    = rs["rowSet"]
    return pd.DataFrame(rows, columns=headers)


def _make_stats_session() -> requests.Session:
    """Session tuned for stats.nba.com — keep-alive, no auto-retry on reads."""
    session = requests.Session()
    # Only retry on true server errors, not connection resets (we handle those)
    retry = Retry(
        total=0,  # we handle retries manually with jitter
        raise_on_status=False,
    )
    adapter = HTTPAdapter(
        max_retries=retry,
        pool_connections=1,
        pool_maxsize=1,
    )
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    return session


def warm_cache(
    nba_ids: list,
    cache_dir: Path,
    season: str,
    sleep: float = 1.0,
    timeout: float = 30.0,
    connect_timeout: float = 10.0,
):
    """
    Pre-fetch game logs for all resolved player IDs into _nba_cache.
    Uses direct HTTP with browser-grade headers instead of nba_api's
    internal client, which stats.nba.com fingerprints and connection-resets.
    """
    import random

    cache_dir.mkdir(parents=True, exist_ok=True)
    already_cached = 0
    fetched = 0
    failed = []

    session = _make_stats_session()

    print(f"\n🔥 Warming cache for {len(nba_ids)} players (season={season})...")
    print(f"   Cache dir: {cache_dir}")
    print(f"   Method: direct HTTP with browser headers (bypasses nba_api client)\n")

    for i, pid in enumerate(nba_ids, 1):
        cache_file = cache_dir / f"playergamelog_{season}_{pid}.csv"
        if cache_file.exists():
            already_cached += 1
            continue

        last_err = None
        for attempt in range(1, 4):  # 3 attempts max
            try:
                df = _fetch_gamelog_direct(pid, season, session, timeout)
                df.to_csv(cache_file, index=False)
                fetched += 1
                last_err = None
                break
            except Exception as e:
                last_err = e
                # Jittered backoff: 1-3s, 3-7s — avoids patterns stats.nba.com detects
                backoff = sleep * attempt + random.uniform(0.5, 2.0)
                if attempt < 3:
                    print(f"  ↩️  player_id={pid} attempt {attempt}/3: {type(e).__name__} — retry in {backoff:.1f}s")
                    time.sleep(backoff)

        if last_err is not None:
            failed.append((pid, str(last_err)))
            print(f"  ❌ player_id={pid}: {type(last_err).__name__}")
        else:
            if i % 20 == 0 or fetched % 25 == 0:
                print(f"  [{i}/{len(nba_ids)}] fetched={fetched} skipped={already_cached} failed={len(failed)}")

        # Jittered sleep between requests — avoids metronomic pattern detection
        time.sleep(sleep + random.uniform(0.2, 0.8))

    print(f"\n✅ Cache warm complete:")
    print(f"   Already cached : {already_cached}")
    print(f"   Newly fetched  : {fetched}")
    print(f"   Failed         : {len(failed)}")
    if failed:
        print(f"   Failed IDs     : {[p for p, _ in failed]}")


def main():
    ap = argparse.ArgumentParser(description="Build/update NBA player ID map from ESPN rosters")
    ap.add_argument("--id-map",    default="NbaPropPipelineA/player_id_map.csv",
                    help="Path to player_id_map.csv (default: NbaPropPipelineA/player_id_map.csv)")
    ap.add_argument("--cache-dir", default="NbaPropPipelineA/_nba_cache",
                    help="Path to _nba_cache dir (default: NbaPropPipelineA/_nba_cache)")
    ap.add_argument("--season",    default="2025-26",
                    help="NBA season string (default: 2025-26)")
    ap.add_argument("--warm-cache", action="store_true",
                    help="After building ID map, also pre-fetch game logs into cache")
    ap.add_argument("--sleep",     type=float, default=0.4,
                    help="Sleep between ESPN requests (default: 0.4s)")
    ap.add_argument("--cache-sleep", type=float, default=0.8,
                    help="Sleep between NBA API game log requests (default: 0.8s)")
    args = ap.parse_args()

    id_map_path = Path(args.id_map)
    cache_dir   = Path(args.cache_dir)

    # ── Step 1: Fetch ESPN rosters ─────────────────────────────────────────────
    session = _make_session()
    espn_df = fetch_espn_rosters(session, sleep=args.sleep)

    if espn_df.empty:
        print("❌ No ESPN data fetched — check network connectivity")
        return

    # ── Step 2: Match to NBA API IDs ───────────────────────────────────────────
    nba_df = load_nba_api_players()
    matched_df = match_players(espn_df, nba_df)

    if matched_df.empty:
        print("❌ No players matched — check nba_api installation")
        return

    # ── Step 3: Merge with existing map ───────────────────────────────────────
    merged = merge_with_existing(matched_df, id_map_path)

    # ── Step 4: Save updated map ───────────────────────────────────────────────
    id_map_path.parent.mkdir(parents=True, exist_ok=True)
    merged.to_csv(id_map_path, index=False, encoding="utf-8")
    print(f"\n✅ Saved → {id_map_path}  ({len(merged)} total entries)")

    # Print coverage summary
    print("\n📊 Coverage by team:")
    if "team_pp" in merged.columns:
        team_col = "team_pp"
    else:
        team_col = merged.columns[2]  # fallback
    team_counts = merged[team_col].value_counts().sort_index()
    for team, count in team_counts.items():
        print(f"  {team:4s}: {count} players")

    # ── Step 5: (Optional) Warm the cache ─────────────────────────────────────
    if args.warm_cache:
        all_nba_ids = merged["nba_player_id"].dropna().astype(str).str.strip()
        all_nba_ids = [int(x) for x in all_nba_ids if x.isdigit()]
        all_nba_ids = sorted(set(all_nba_ids))
        warm_cache(
            nba_ids=all_nba_ids,
            cache_dir=cache_dir,
            season=args.season,
            sleep=args.cache_sleep,
        )
    else:
        print("\n💡 Tip: Run with --warm-cache to pre-fetch all game logs into _nba_cache")
        print("   This makes step4 much faster (no live API calls during pipeline)")

    print("\n🏀 Done!")


if __name__ == "__main__":
    main()
