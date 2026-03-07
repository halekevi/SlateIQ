#!/usr/bin/env python3
"""
step4_attach_player_stats_soccer.py  (Soccer Pipeline) — OPTIMIZED

Key optimizations vs original:
  1. Concurrent ESPN player stat fetching via ThreadPoolExecutor (default 10 workers)
     → 1069 players × ~20 API calls: ~5 hours → ~30 min
  2. Fetch each unique player ONCE, then broadcast to all matching rows
     (original fetched per-row, causing duplicate network calls)
  3. Cache saved in one batch after all fetches complete
     (original saved after every single game = thousands of file writes)
  4. Concurrent event-log + match-stats fetching per player using inner pool
  5. Thread-local requests.Session to reuse HTTP connections

Run:
  py step4_attach_player_stats_soccer.py \
    --input step3_soccer_with_defense.csv \
    --cache soccer_stats_cache.csv \
    --output step4_soccer_with_stats.csv
"""

from __future__ import annotations

import argparse
import random
import re
import sys
import threading
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import pandas as pd
import requests

COMBO_SEP = "|"

ESPN_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

LEAGUE_SLUGS = {
    "EPL":        "eng.1",
    "BUNDESLIGA": "ger.1",
    "LIGUE 1":    "fra.1",
    "SERIE A":    "ita.1",
    "LA LIGA":    "esp.1",
    "MLS":        "usa.1",
    "UCL":        "uefa.champions",
    "ARG":        "arg.1",
    "ARGENTINA":  "arg.1",
    "BRASILEIRAO":"bra.1",
    "LIGA MX":    "mex.1",
    "EREDIVISIE": "ned.1",
    "PRIMEIRA LIGA": "por.1",
}
DEFAULT_SLUG = "eng.1"

# Team name → correct league slug override
# Handles players mis-tagged by PrizePicks when their league is inactive
TEAM_LEAGUE_OVERRIDE: Dict[str, str] = {
    # Argentine Primera División
    "INSTITUTO": "arg.1", "UNIÓN": "arg.1", "UNION": "arg.1",
    "ARGENTINOS": "arg.1", "BARRACAS": "arg.1", "BOCA JUNIORS": "arg.1",
    "RIVER PLATE": "arg.1", "RACING": "arg.1", "INDEPENDIENTE": "arg.1",
    "SAN LORENZO": "arg.1", "ESTUDIANTES": "arg.1", "VÉLEZ": "arg.1",
    "VELEZ": "arg.1", "TALLERES": "arg.1", "LANÚS": "arg.1", "LANUS": "arg.1",
    "HURACÁN": "arg.1", "HURACAN": "arg.1", "TIGRE": "arg.1",
    "DEFENSA": "arg.1", "GODOY CRUZ": "arg.1", "BELGRANO": "arg.1",
    "PLATENSE": "arg.1", "NEWELLS": "arg.1", "ROSARIO CENTRAL": "arg.1",
    "ATLÉTICO TUCUMÁN": "arg.1", "ATLETICO TUCUMAN": "arg.1",
    "CENTRAL CÓRDOBA": "arg.1", "CENTRAL CORDOBA": "arg.1",
    "SARMIENTO": "arg.1", "RIESTRA": "arg.1", "SAN MARTIN": "arg.1",
    # MLS
    "INTER MIAMI": "usa.1", "ORLANDO": "usa.1", "ATLANTA UNITED": "usa.1",
    "LA GALAXY": "usa.1", "LAFC": "usa.1", "SEATTLE SOUNDERS": "usa.1",
    "PORTLAND TIMBERS": "usa.1", "SPORTING KC": "usa.1", "NYC FC": "usa.1",
    "NYCFC": "usa.1", "NEW YORK RED BULLS": "usa.1", "DC UNITED": "usa.1",
    "D.C. UNITED": "usa.1", "PHILADELPHIA UNION": "usa.1", "COLUMBUS CREW": "usa.1",
    "NASHVILLE SC": "usa.1", "CHARLOTTE FC": "usa.1", "CF MONTREAL": "usa.1",
    "NEW ENGLAND": "usa.1", "CHICAGO FIRE": "usa.1", "FC DALLAS": "usa.1",
    "HOUSTON DYNAMO": "usa.1", "REAL SALT LAKE": "usa.1", "MINNESOTA UNITED": "usa.1",
    "SAN JOSE": "usa.1", "VANCOUVER WHITECAPS": "usa.1", "FC CINCINNATI": "usa.1",
    "ST. LOUIS CITY": "usa.1", "ST LOUIS CITY": "usa.1", "SAN DIEGO FC": "usa.1",
}


def resolve_league_slug(league: str, team: str) -> str:
    """Get the correct ESPN league slug, using team name to fix mis-tagged leagues."""
    team_upper = str(team).upper().strip()
    for team_key, slug in TEAM_LEAGUE_OVERRIDE.items():
        if team_key in team_upper or team_upper in team_key:
            return slug
    return LEAGUE_SLUGS.get(league.upper(), DEFAULT_SLUG)
EVENTLOG_URL_GENERIC = "https://site.api.espn.com/apis/site/v2/sports/soccer/athletes/{athlete_id}/eventlog"
SCOREBOARD_URL = (
    "https://site.api.espn.com/apis/site/v2/sports/soccer/{league_slug}/scoreboard"
    "?limit=100&dates={date_range}"
)
ATHLETE_STATS_URL = (
    "https://sports.core.api.espn.com/v2/sports/soccer/leagues/{league_slug}"
    "/events/{event_id}/competitions/{event_id}/athletes/{athlete_id}/statistics/0"
)

# Thread-local session
_local = threading.local()

def _get_session() -> requests.Session:
    if not hasattr(_local, "session"):
        s = requests.Session()
        s.headers.update(ESPN_HEADERS)
        _local.session = s
    return _local.session


def _sleep(base: float = 0.3) -> None:
    time.sleep(max(0.0, base + random.uniform(0, 0.2)))


def _get(url: str, retries: int = 3) -> Optional[dict]:
    session = _get_session()
    for attempt in range(1, retries + 1):
        try:
            _sleep(0.3)
            r = session.get(url, timeout=20)
            if r.status_code == 429:
                wait = 60 * attempt
                print(f"  ⚠️ 429 rate limit — sleeping {wait}s (attempt {attempt})")
                time.sleep(wait)
                continue
            if r.status_code == 404:
                return None
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt < retries:
                time.sleep(2.0 * attempt)
    return None


def _parse_ids(espn_player_id: str) -> List[str]:
    s = str(espn_player_id).strip()
    if not s or s == "nan":
        return []
    if COMBO_SEP in s:
        return [p.strip() for p in s.split(COMBO_SEP) if p.strip().isdigit()]
    return [s] if s.isdigit() else []


def fmt_num(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{float(x):.3f}".rstrip("0").rstrip(".")


def get_recent_event_ids(league_slug: str, n_weeks: int = 20) -> List[dict]:
    """
    Fetch recent completed game event IDs from the scoreboard for a league.
    Returns list of {event_id, date} dicts sorted newest first.
    """
    from datetime import datetime, timedelta
    events = []
    seen   = set()
    today  = datetime.utcnow()

    # Fetch week by week going backwards
    for w in range(n_weeks):
        start = today - timedelta(weeks=w + 1)
        end   = today - timedelta(weeks=w)
        date_range = f"{start.strftime('%Y%m%d')}-{end.strftime('%Y%m%d')}"
        url  = SCOREBOARD_URL.format(league_slug=league_slug, date_range=date_range)
        data = _get(url)
        if not data:
            continue
        for ev in (data.get("events") or []):
            eid    = str(ev.get("id", "")).strip()
            date   = str(ev.get("date", "")).strip()
            status = ev.get("status", {})
            stype  = status.get("type", {})
            # Accept: explicitly completed, OR state=="post", OR no status info at all
            completed = (
                stype.get("completed", False)
                or stype.get("state", "").lower() == "post"
                or stype.get("name", "").lower() in ("final", "ft", "fulltime")
                or (not stype and eid)  # no status block → include anyway
            )
            if eid and eid not in seen and completed:
                seen.add(eid)
                events.append({"event_id": eid, "date": date})

    # Sort newest first
    events.sort(key=lambda x: x["date"], reverse=True)
    return events


SUMMARY_URL = "https://site.api.espn.com/apis/site/v2/sports/soccer/{league_slug}/summary?event={event_id}"


def get_player_match_stats(athlete_id: str, event_id: str, league_slug: str) -> Dict[str, float]:
    """
    Fetch player stats from the game summary endpoint.
    Stats live in: rosters[].roster[].stats[] where entry.athlete.id matches.
    """
    data = _get_summary_cached(event_id, league_slug)
    if not data:
        return {}

    aid_str = str(athlete_id)
    stats: Dict[str, float] = {}

    for roster in (data.get("rosters") or []):
        for entry in (roster.get("roster") or []):
            ath = entry.get("athlete", {})
            if str(ath.get("id", "")).strip() != aid_str:
                continue
            for stat in (entry.get("stats") or []):
                name = str(stat.get("name", stat.get("abbreviation", ""))).lower().strip()
                try:
                    stats[name] = float(stat.get("value", stat.get("displayValue", "")))
                except (TypeError, ValueError):
                    pass
            return stats  # found the player, done

    return stats


# Module-level cache of scoreboard events per league slug so we only fetch once
_scoreboard_cache: Dict[str, List[dict]] = {}
_scoreboard_lock = threading.Lock()

# Summary cache so multiple players in same game share one API call
_summary_cache: Dict[str, Optional[dict]] = {}
_summary_lock  = threading.Lock()


def _get_summary_cached(event_id: str, league_slug: str) -> Optional[dict]:
    key = f"{league_slug}:{event_id}"
    # Check cache without blocking
    with _summary_lock:
        if key in _summary_cache:
            return _summary_cache[key]
    # Fetch outside the lock so multiple workers can fetch concurrently
    url    = SUMMARY_URL.format(league_slug=league_slug, event_id=event_id)
    result = _get(url)
    # Write result to cache
    with _summary_lock:
        _summary_cache[key] = result
    return result


def get_cached_events(league_slug: str) -> List[dict]:
    with _scoreboard_lock:
        if league_slug not in _scoreboard_cache:
            print(f"    Fetching scoreboard events for {league_slug}...")
            _scoreboard_cache[league_slug] = get_recent_event_ids(league_slug)
            print(f"    {league_slug}: {len(_scoreboard_cache[league_slug])} events found")
        return _scoreboard_cache[league_slug]


def derive_stat(stats: Dict[str, float], prop_norm: str) -> float:
    p = str(prop_norm).lower().strip()
    lookups = {
        "shots":           ["shots", "totalshots", "shotsattempted"],
        "shots_on_target": ["shotsontarget", "shots on target", "ontarget"],
        "saves":           ["saves", "goalsaves", "savesmade"],
        "passes":          ["passes", "totalpasses", "passesattempted", "passescompleted"],
        "assists":         ["assists", "goalassists"],
        "goals":           ["goals", "goalsscored"],
        "clearances":      ["clearances", "totalclearances"],
        "tackles":         ["tackles", "tacklesmade", "totalmatchwon"],
        "fouls":           ["foulscommitted", "foulsconceded", "fouls"],
        "goals_allowed":   ["goalsagainst", "goalsconceded", "goalsallowed"],
        "goal_assist":     ["goals", "assists"],
        "shots_assisted":  ["shotsassisted", "keypassescompleted", "chancescreated"],
        "minutes":         ["minutesplayed", "minutes", "minutesplayed", "minsplayed", "timeplayed"],
    }
    keys = lookups.get(p, [p])

    if p == "goal_assist":
        g = next((stats[k] for k in lookups["goals"]   if k in stats), np.nan)
        a = next((stats[k] for k in lookups["assists"]  if k in stats), np.nan)
        if not np.isnan(g) and not np.isnan(a):
            return float(g + a)
        return np.nan

    for k in keys:
        if k in stats:
            return float(stats[k])
    for k in keys:
        for sk in stats:
            if k in sk or sk in k:
                return float(stats[sk])
    return np.nan


ALL_PROP_NORMS = [
    "shots", "shots_on_target", "saves", "passes", "assists",
    "goals", "clearances", "tackles", "fouls", "goals_allowed",
    "goal_assist", "shots_assisted", "minutes",
]


ALL_LEAGUE_SLUGS = [
    "eng.1", "ger.1", "fra.1", "ita.1", "esp.1",
    "usa.1", "uefa.champions", "arg.1", "bra.1",
    "mex.1", "ned.1", "por.1", "sco.1", "tur.1",
]


def _fetch_player_stats(
    athlete_id: str,
    league: str,
    n_games: int,
    existing_event_ids: set,
) -> List[dict]:
    """
    Fetch up to n_games match stats for one player using scoreboard events.
    Tries the player's tagged league first (all events), then falls back to
    all other leagues (scanning up to 30 events each to keep it fast).
    """
    league_slug = league  # already resolved to correct slug by caller
    new_rows: List[dict] = []
    added = 0

    # Build slug order: tagged league first, then all others
    other_slugs = [s for s in ALL_LEAGUE_SLUGS if s != league_slug]
    slugs_to_try = [(league_slug, 999)] + [(s, 30) for s in other_slugs]

    for slug, max_events in slugs_to_try:
        if added >= n_games:
            break
        events = get_cached_events(slug)
        checked = 0
        for ev in events:
            if added >= n_games or checked >= max_events:
                break
            eid = ev["event_id"]
            if (str(athlete_id), str(eid)) in existing_event_ids:
                checked += 1
                continue
            stats = get_player_match_stats(athlete_id, eid, slug)
            checked += 1
            if not stats:
                continue
            date = ev.get("date", "")
            for prop_norm in ALL_PROP_NORMS:
                val = derive_stat(stats, prop_norm)
                new_rows.append({
                    "ESPN_ATHLETE_ID": str(athlete_id),
                    "EVENT_ID":        str(eid),
                    "GAME_DATE":       date,
                    "LEAGUE":          slug,   # use actual slug found, not tagged league
                    "PROP_NORM":       prop_norm,
                    "STAT_VALUE":      fmt_num(val) if not np.isnan(val) else "",
                })
            existing_event_ids.add((str(athlete_id), str(eid)))
            added += 1

    return new_rows


def load_cache(cache_path: Path) -> pd.DataFrame:
    if cache_path.exists():
        try:
            df = pd.read_csv(cache_path, dtype=str, low_memory=False).fillna("")
            print(f"  Loaded cache: {len(df)} rows from {cache_path.name}")
            return df
        except Exception as e:
            print(f"  ⚠️ Could not load cache: {e}")
    cols = ["ESPN_ATHLETE_ID", "EVENT_ID", "GAME_DATE", "LEAGUE", "PROP_NORM", "STAT_VALUE"]
    return pd.DataFrame(columns=cols)


def save_cache(cache: pd.DataFrame, cache_path: Path) -> None:
    cache.to_csv(cache_path, index=False, encoding="utf-8-sig")


def get_vals_from_cache(
    cache: pd.DataFrame,
    athlete_id: str,
    prop_norm: str,
    n: int = 10,
) -> List[float]:
    mask = (
        (cache["ESPN_ATHLETE_ID"].astype(str) == str(athlete_id)) &
        (cache["PROP_NORM"].astype(str) == str(prop_norm)) &
        (cache["STAT_VALUE"].astype(str).str.strip() != "")
    )
    sub = cache.loc[mask].copy()
    if sub.empty:
        return []
    sub["GAME_DATE"] = pd.to_datetime(sub["GAME_DATE"], errors="coerce")
    sub = sub.sort_values("GAME_DATE", ascending=False)
    vals = pd.to_numeric(sub["STAT_VALUE"], errors="coerce").dropna().tolist()
    return vals[:n]


def calc_hit_context(vals: List[float], line: float, k: int = 5):
    recent  = vals[:k] if len(vals) >= k else vals
    if not recent:
        return 0, 0, 0, np.nan, np.nan, np.nan
    over   = sum(1 for v in recent if v > line)
    under  = sum(1 for v in recent if v < line)
    push   = sum(1 for v in recent if v == line)
    played = len(recent)
    hr_all = over / played if played else np.nan
    denom  = over + under
    hr_ou  = over  / denom if denom else np.nan
    ur_ou  = under / denom if denom else np.nan
    return over, under, push, hr_all, hr_ou, ur_ou


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",   required=True)
    ap.add_argument("--cache",   default="soccer_stats_cache.csv")
    ap.add_argument("--output",  required=True)
    ap.add_argument("--n",       type=int, default=10, help="Max games per player")
    ap.add_argument("--workers", type=int, default=6,
                    help="Concurrent player fetch workers (default 6, reduced from 10 to avoid ESPN 429s)")
    ap.add_argument("--season",  default="2025")
    ap.add_argument("--debug_misses", default="")
    ap.add_argument("--debug_player", default="",
                    help="ESPN athlete ID to debug — prints raw eventlog response and exits")
    args = ap.parse_args()

    # Debug mode: inspect summary boxscore structure for player stats
    if args.debug_player:
        import json
        aid = args.debug_player.strip()
        print(f"\n🔍 DEBUG player {aid} — inspecting summary boxscore")

        data = _get_summary_cached("740860", "eng.1")
        if not data:
            print("❌ Could not fetch summary"); return

        bs = data.get("boxscore", {})
        print(f"Boxscore keys: {list(bs.keys())}")
        players_block = bs.get("players") or []
        print(f"boxscore.players: {len(players_block)} team blocks")
        for i, tb in enumerate(players_block):
            print(f"\n  Team block {i} keys: {list(tb.keys())}")
            for j, sg in enumerate((tb.get("statistics") or [])[:2]):
                print(f"  stat_group {j} keys: {list(sg.keys())}")
                keys = sg.get("keys") or sg.get("labels") or []
                print(f"    keys/labels: {[k.get('name','?') for k in keys[:10]]}")
                athletes = sg.get("athletes") or []
                print(f"    athletes: {len(athletes)}")
                if athletes:
                    print(f"    First athlete: {json.dumps(athletes[0], indent=2)[:500]}")

        print(f"\nRosters: {len(data.get('rosters',[]))} blocks")
        for i, r in enumerate((data.get("rosters") or [])[:1]):
            entries = r.get("roster") or r.get("entries") or []
            print(f"  Roster {i}: {len(entries)} entries")
            if entries:
                print(f"  First entry: {json.dumps(entries[0], indent=2)[:500]}")

        # Now try to find our specific player in the summary
        print(f"\nSearching for player {aid} in summary...")
        found_stats = get_player_match_stats(aid, "740860", "eng.1")
        print(f"Result: {found_stats}")
        return

    print(f"→ Loading Step3: {args.input}")
    slate      = pd.read_csv(args.input, low_memory=False, encoding="utf-8-sig").fillna("")

    if slate.empty:
        print("❌ [SlateIQ-Soccer-S4] Empty input from S3 — aborting.")
        sys.exit(1)
    cache_path = Path(args.cache)
    cache      = load_cache(cache_path)

    N         = int(args.n)
    stat_cols = [f"stat_g{i}" for i in range(1, N + 1)]
    out_cols  = stat_cols + [
        "stat_last5_avg", "stat_last10_avg", "stat_season_avg",
        "last5_over", "last5_under", "last5_push", "last5_hit_rate",
        "line_hit_rate_over_ou_5", "line_hit_rate_under_ou_5",
        "line_hit_rate_over_ou_10", "line_hit_rate_under_ou_10",
        "stat_status", "avg_minutes", "avg_passes",
    ]
    for c in out_cols:
        if c not in slate.columns:
            slate[c] = ""

    slate["_line_num"] = pd.to_numeric(slate.get("line", ""), errors="coerce")

    # ── Build set of unique players that need fresh data ──────────────────────
    existing_keys = set(
        zip(cache["ESPN_ATHLETE_ID"].astype(str), cache["EVENT_ID"].astype(str))
    )

    # Collect unique (athlete_id, league) pairs not yet sufficiently cached
    players_needing_fetch: Dict[str, str] = {}   # athlete_id → resolved league slug
    for _, row in slate.iterrows():
        espn_id_raw = str(row.get("espn_player_id", "")).strip()
        ids         = _parse_ids(espn_id_raw)
        league      = str(row.get("league", "")).strip().upper()
        team        = str(row.get("team",   "")).strip().upper()
        slug        = resolve_league_slug(league, team)
        for aid in ids:
            if aid not in players_needing_fetch:
                cached_count = int((cache["ESPN_ATHLETE_ID"].astype(str) == aid).sum())
                if cached_count < N * len(ALL_PROP_NORMS):
                    players_needing_fetch[aid] = slug

    print(f"\n→ Fetching stats for {len(players_needing_fetch)} players "
          f"(workers={args.workers}, n_games={N})...")

    # Pre-warm scoreboard cache for all leagues so workers don't race to fetch them
    print("  Pre-warming scoreboard cache for all leagues...")
    for slug in ALL_LEAGUE_SLUGS:
        get_cached_events(slug)
    print(f"  Scoreboard cache ready: {sum(len(v) for v in _scoreboard_cache.values())} total events across {len(_scoreboard_cache)} leagues")

    # CONCURRENT fetch: one worker per unique player
    new_rows_all: List[dict] = []
    fetched = 0

    with ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {
            pool.submit(_fetch_player_stats, aid, league, N, set(existing_keys)): aid
            for aid, league in players_needing_fetch.items()
        }
        for i, fut in enumerate(as_completed(futures), 1):
            aid = futures[fut]
            try:
                new_rows = fut.result()
                if new_rows:
                    new_rows_all.extend(new_rows)
                    fetched += 1
            except Exception as e:
                print(f"  ⚠️ {aid}: {e}")
            if i % 50 == 0 or i == len(futures):
                print(f"    {i}/{len(futures)} players done  new_rows={len(new_rows_all)}")

    # Append all new rows to cache at once, then save once
    if new_rows_all:
        cache = pd.concat([cache, pd.DataFrame(new_rows_all)], ignore_index=True)
        save_cache(cache, cache_path)
        print(f"Cache updated: +{len(new_rows_all)} rows → {cache_path}")

    # ── Attach stats to slate rows ────────────────────────────────────────────
    print(f"\n→ Attaching stats to {len(slate)} rows...")
    misses: List[dict] = []

    for idx, row in slate.iterrows():
        prop        = str(row.get("prop_norm", "")).lower().strip()
        player      = str(row.get("player",    "")).strip()
        team        = str(row.get("team",      "")).strip()
        league      = str(row.get("league",    "")).strip().upper()
        espn_id_raw = str(row.get("espn_player_id", "")).strip()
        line        = row.get("_line_num", np.nan)
        try:
            line = float(line)
        except Exception:
            line = np.nan

        ids      = _parse_ids(espn_id_raw)
        is_combo = (len(ids) > 1) or (
            str(row.get("is_combo_player", "")).strip().lower() in ("1", "true", "yes")
        )

        if not ids:
            slate.at[idx, "stat_status"] = "NO_ESPN_PLAYER"
            misses.append({"player": player, "team": team, "prop_norm": prop,
                           "line": str(row.get("line", "")), "espn_player_id": espn_id_raw})
            continue

        if not is_combo:
            vals = get_vals_from_cache(cache, ids[0], prop, n=N)
            if not vals:
                slate.at[idx, "stat_status"] = "NO_CACHE_DATA"
                continue
        else:
            per_player_vals: List[List[float]] = []
            any_empty = False
            for aid in ids:
                pv = get_vals_from_cache(cache, aid, prop, n=N)
                if not pv:
                    any_empty = True
                    break
                per_player_vals.append(pv)
            if any_empty or not per_player_vals:
                slate.at[idx, "stat_status"] = "NO_CACHE_DATA"
                continue
            min_g = min(len(pv) for pv in per_player_vals)
            vals  = [float(sum(pv[i] for pv in per_player_vals)) for i in range(min_g)]
            if not vals:
                slate.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue

        for i in range(1, N + 1):
            v = vals[i - 1] if (i - 1) < len(vals) else np.nan
            slate.at[idx, f"stat_g{i}"] = fmt_num(v)

        def avg_k(k: int) -> float:
            s = vals[:k] if len(vals) >= k else vals
            return float(np.mean(s)) if s else np.nan

        slate.at[idx, "stat_last5_avg"]  = fmt_num(avg_k(5))
        slate.at[idx, "stat_last10_avg"] = fmt_num(avg_k(10))
        slate.at[idx, "stat_season_avg"] = fmt_num(float(np.mean(vals)) if vals else np.nan)

        if not np.isnan(line):
            o5, u5, p5, hr5, hr5_ou, ur5_ou = calc_hit_context(vals, line, k=5)
            slate.at[idx, "last5_over"]               = str(o5)
            slate.at[idx, "last5_under"]              = str(u5)
            slate.at[idx, "last5_push"]               = str(p5)
            slate.at[idx, "last5_hit_rate"]           = fmt_num(hr5)
            slate.at[idx, "line_hit_rate_over_ou_5"]  = fmt_num(hr5_ou)
            slate.at[idx, "line_hit_rate_under_ou_5"] = fmt_num(ur5_ou)
            _, _, _, _, hr10_ou, ur10_ou = calc_hit_context(vals, line, k=10)
            slate.at[idx, "line_hit_rate_over_ou_10"]  = fmt_num(hr10_ou)
            slate.at[idx, "line_hit_rate_under_ou_10"] = fmt_num(ur10_ou)

        slate.at[idx, "stat_status"] = "OK"

        # ── Attach avg_minutes and avg_passes as dedicated columns ──────────
        # These are used by S6 for minutes_tier and field_involvement
        if ids:
            min_vals = get_vals_from_cache(cache, ids[0], "minutes", n=5)
            slate.at[idx, "avg_minutes"] = fmt_num(float(np.mean(min_vals)) if min_vals else np.nan)
            pass_vals = get_vals_from_cache(cache, ids[0], "passes", n=5)
            slate.at[idx, "avg_passes"] = fmt_num(float(np.mean(pass_vals)) if pass_vals else np.nan)

    if args.debug_misses and misses:
        pd.DataFrame(misses).drop_duplicates().to_csv(
            args.debug_misses, index=False, encoding="utf-8-sig"
        )
        print(f"Wrote misses → {args.debug_misses}")

    slate.drop(columns=["_line_num"], errors="ignore", inplace=True)
    slate.to_csv(args.output, index=False, encoding="utf-8-sig")

    if slate.empty:
        print("❌ [SlateIQ-Soccer-S4] Output is empty — aborting.")
        sys.exit(1)

    print(f"\n✅ Saved → {args.output}")
    print(f"Cache updates: {fetched} players / {len(new_rows_all)} rows")
    print("\nstat_status breakdown:")
    print(slate["stat_status"].astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()
