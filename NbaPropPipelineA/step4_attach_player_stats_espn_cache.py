#!/usr/bin/env python3
r"""
step4_attach_player_stats_espn_cache.py  (REVISED 2026-02-25)
--------------------------------------------------------------
ESPN-based Step4. Fixes applied vs prior version:

FIX 1 - MINUTES PARSING
    ESPN returns MIN as "23:14" (MM:SS). Prior code did replace(":",".")
    -> 23.14, which is wrong decimal minutes (23min 14sec != 23.14min).
    Now converts properly: "23:14" -> 23 + 14/60 = 23.233.

FIX 2 - TRADED PLAYER ID RESOLUTION
    Prior code tried name+team match first, then name-only as last resort.
    For traded players the team in the slate won't match the team in the
    ESPN cache (old games). Now tries name-only match FIRST as primary
    lookup, so traded players resolve correctly without needing idmap.
    idmap lookup still runs first (fastest path when populated).

FIX 3 - COMBO PLAYERS: N-PLAYER SUPPORT
    Prior code hardcoded player_1/player_2 and a1/a2. Now loops over
    all parsed IDs from nba_player_id (same approach as NBA boxscore
    version), supporting 3+ player combos via EVENT_ID intersection.

FIX 4 - DIRECTION-AWARE LINE HIT RATE COLUMNS
    step7 expects: line_hit_rate_over_ou_5, line_hit_rate_under_ou_5,
    line_hit_rate_over_ou_10, line_hit_rate_under_ou_10.
    Prior code only output last5_over/under/push + last5_hit_rate.
    step7 fell back to deriving under_rate as (1 - over_rate) which is
    inaccurate when there are pushes. Now computes all four columns
    directly from rolling game windows.

FIX 5 - EVENT SKIP TRACKING
    Prior code silently swallowed ESPN event failures. Now counts and
    surfaces events skipped vs fetched at end of cache update.

FIX 6 - SEASON FILTER ON PLAYER ROW PULLS (IMPORTANT)
    Prevents mixing seasons when selecting a player's games from the cache.
    We now filter BOTH by (SEASON == args.season) AND ESPN_ATHLETE_ID
    for:
      - single-player vals pulls
      - combo-player EVENT_ID intersection pulls
"""

from __future__ import annotations

import sys as _sys
try:
    _sys.stdout.reconfigure(encoding="utf-8")
    _sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

import argparse
import random
import re
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
import requests

COMBO_SEP = "|"

ESPN_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
}

# ── helpers ───────────────────────────────────────────────────────────────────

def _sleep(base: float, jitter: float = 0.8) -> None:
    time.sleep(max(0.0, base + random.uniform(0, jitter)))

def _clean_id(x: str) -> str:
    s = str(x).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s.replace(",", "")

def _parse_ids(nba_player_id: str) -> List[int]:
    s = _clean_id(nba_player_id)
    if not s:
        return []
    if COMBO_SEP in s:
        out: List[int] = []
        for p in s.split(COMBO_SEP):
            p = _clean_id(p)
            if p.isdigit():
                out.append(int(p))
        return sorted(list(dict.fromkeys(out)))
    return [int(s)] if s.isdigit() else []

_SUFFIXES = {"jr", "sr", "ii", "iii", "iv", "v"}

def _norm_name(name: str) -> str:
    s = (name or "").lower().strip()
    s = re.sub(r"[^a-z0-9\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    parts = [p for p in s.split(" ") if p and p not in _SUFFIXES]
    return " ".join(parts).strip()

def _to_float(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series, errors="coerce")

def _parse_made_att(s: str) -> Tuple[float, float]:
    if s is None:
        return (np.nan, np.nan)
    txt = str(s).strip()
    if not txt or txt == "--":
        return (np.nan, np.nan)
    m = re.match(r"^\s*(\d+)\s*-\s*(\d+)\s*$", txt)
    if not m:
        return (np.nan, np.nan)
    return (float(m.group(1)), float(m.group(2)))

# FIX 1: proper MM:SS -> decimal minutes conversion
def _parse_minutes(min_s: str) -> float:
    """
    Convert ESPN minutes string to decimal minutes.
    "23:14" -> 23 + 14/60 = 23.233
    "23"    -> 23.0  (already numeric)
    ""      -> nan
    """
    txt = str(min_s).strip()
    if not txt or txt == "--":
        return np.nan
    if ":" in txt:
        parts = txt.split(":")
        try:
            mins = int(parts[0]) + int(parts[1]) / 60.0
            return float(mins)
        except (ValueError, IndexError):
            return np.nan
    return pd.to_numeric(txt, errors="coerce")

def fmt_num(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{float(x):.3f}".rstrip("0").rstrip(".")

def derive_stat_series(df: pd.DataFrame, prop_norm: str) -> pd.Series:
    p = re.sub(r"\(combo\)\s*$", "", (prop_norm or "").lower().strip()).strip()

    pts  = _to_float(df.get("PTS",  pd.Series([np.nan] * len(df), index=df.index)))
    reb  = _to_float(df.get("REB",  pd.Series([np.nan] * len(df), index=df.index)))
    ast  = _to_float(df.get("AST",  pd.Series([np.nan] * len(df), index=df.index)))
    stl  = _to_float(df.get("STL",  pd.Series([np.nan] * len(df), index=df.index)))
    blk  = _to_float(df.get("BLK",  pd.Series([np.nan] * len(df), index=df.index)))
    tov  = _to_float(df.get("TO",   pd.Series([np.nan] * len(df), index=df.index)))
    fga  = _to_float(df.get("FGA",  pd.Series([np.nan] * len(df), index=df.index)))
    fgm  = _to_float(df.get("FGM",  pd.Series([np.nan] * len(df), index=df.index)))
    fg3a = _to_float(df.get("FG3A", pd.Series([np.nan] * len(df), index=df.index)))
    fg3m = _to_float(df.get("FG3M", pd.Series([np.nan] * len(df), index=df.index)))
    fta  = _to_float(df.get("FTA",  pd.Series([np.nan] * len(df), index=df.index)))
    ftm  = _to_float(df.get("FTM",  pd.Series([np.nan] * len(df), index=df.index)))
    fg2a = fga - fg3a
    fg2m = fgm - fg3m

    if p in ("pts", "points"):           return pts
    if p in ("reb", "rebounds"):         return reb
    if p in ("ast", "assists"):          return ast
    if p == "pra":                       return pts + reb + ast
    if p == "pr":                        return pts + reb
    if p == "pa":                        return pts + ast
    if p == "ra":                        return reb + ast
    if p == "stocks":                    return stl + blk
    if p in ("stl", "steals"):           return stl
    if p in ("blk", "blocks"):           return blk
    if p in ("tov", "turnovers", "to"):  return tov
    if p == "fga":                       return fga
    if p == "fgm":                       return fgm
    if p in ("fg3a", "3pa", "3-pt attempted", "3pt attempted", "3ptattempted", "three pointers attempted"):  return fg3a
    if p in ("fg3m", "3pm", "3-pt made", "3pt made", "3ptmade", "three pointers made"):                      return fg3m
    if p in ("fg2a", "2pa", "two pointers attempted", "2 pointers attempted", "twopointersattempted"):        return fg2a
    if p in ("fg2m", "2pm", "two pointers made", "2 pointers made", "twopointersmade"):                      return fg2m
    if p in ("fta", "free throws attempted", "freethrowsattempted"):                                          return fta
    if p in ("ftm", "free throws made", "freethrowsmade"):                                                    return ftm
    if p in ("fantasy", "fantasy_score"):
        return pts + 1.2 * reb + 1.5 * ast + 3.0 * stl + 3.0 * blk - tov

    return pd.Series([np.nan] * len(df), index=df.index)

# FIX 4: computes both last5 and last10 over/under/push/hit for all four columns
def calc_hit_context(
    vals_mr: List[float], line: float, k: int = 5
) -> Tuple[int, int, int, float, float, float]:
    """
    Returns (over, under, push, hit_rate_all, hit_rate_ou, under_rate_ou)
    for the first k games in vals_mr (most-recent first).
    hit_rate_all  = over / (over+under+push)   [legacy last5_hit_rate]
    hit_rate_ou   = over / (over+under)         [line_hit_rate_over_ou]
    under_rate_ou = under / (over+under)        [line_hit_rate_under_ou]
    """
    over = under = push = 0
    for v in vals_mr[:k]:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        if v > line:
            over += 1
        elif v < line:
            under += 1
        else:
            push += 1
    total_all = over + under + push
    total_ou  = over + under
    hit_rate_all  = (over / total_all) if total_all > 0 else np.nan
    hit_rate_ou   = (over / total_ou)  if total_ou  > 0 else np.nan
    under_rate_ou = (under / total_ou) if total_ou  > 0 else np.nan
    return over, under, push, hit_rate_all, hit_rate_ou, under_rate_ou

# ── ESPN pulls ────────────────────────────────────────────────────────────────

def espn_get_json(url: str, timeout: Tuple[float, float], retries: int, sleep_s: float) -> dict:
    last = None
    for attempt in range(1, retries + 1):
        try:
            _sleep(sleep_s, jitter=0.8)
            r = requests.get(url, headers=ESPN_HEADERS, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            backoff = min(30.0, (2 ** (attempt - 1)) * 2.0) + random.uniform(0.5, 3.0)
            print(f"  [WARN] ESPN GET failed attempt {attempt}/{retries}: {type(e).__name__} — cooldown {backoff:.1f}s")
            time.sleep(backoff)
    raise RuntimeError(f"ESPN GET failed after {retries} retries: {url} | last={last}")

def fetch_espn_event_ids(date_yyyymmdd: str, timeout: Tuple[float, float], retries: int, sleep_s: float) -> List[str]:
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/scoreboard?dates={date_yyyymmdd}"
    data = espn_get_json(url, timeout=timeout, retries=retries, sleep_s=sleep_s)
    events = data.get("events") or []
    seen: set = set()
    out: List[str] = []
    for ev in events:
        eid = str(ev.get("id") or "").strip()
        if eid and eid not in seen:
            seen.add(eid)
            out.append(eid)
    return out

def parse_summary_boxscore(summary: dict) -> pd.DataFrame:
    box = (summary or {}).get("boxscore") or {}
    players_blocks = box.get("players") or []
    rows = []

    game_date = ""
    header = (summary or {}).get("header") or {}
    comp = header.get("competitions") or []
    if comp:
        gd = comp[0].get("date")
        if gd:
            try:
                # FIX 11: Convert UTC -> US/Eastern before extracting date.
                # ESPN timestamps are UTC. A 9:40 PM ET game is 2:40 AM UTC
                # the NEXT day, so .date() on raw UTC flips the date forward.
                # e.g. Isaiah Joe Feb 25 9:40PM ET was stored as Feb 26.
                import pytz
                _eastern = pytz.timezone("US/Eastern")
                _utc_dt  = pd.to_datetime(gd, utc=True, errors="coerce")
                if _utc_dt is not pd.NaT:
                    game_date = _utc_dt.astimezone(_eastern).date().isoformat()
            except Exception:
                try:
                    # Fallback if pytz unavailable: games after midnight UTC
                    # (hour < 5) are actually the prior evening ET
                    from datetime import timedelta
                    _raw = pd.to_datetime(gd, utc=True, errors="coerce")
                    if _raw is not pd.NaT:
                        if _raw.hour < 5:
                            game_date = (_raw - timedelta(days=1)).date().isoformat()
                        else:
                            game_date = _raw.date().isoformat()
                except Exception:
                    game_date = ""
    event_id = str(header.get("id") or "").strip()

    for team_block in players_blocks:
        team = team_block.get("team") or {}
        team_abbr = str(team.get("abbreviation") or "").strip()

        for stat_block in (team_block.get("statistics") or []):
            labels   = stat_block.get("labels") or []
            athletes = stat_block.get("athletes") or []

            if not labels or "MIN" not in labels or "PTS" not in labels:
                continue

            idx = {lab: i for i, lab in enumerate(labels)}

            for a in athletes:
                ath   = a.get("athlete") or {}
                aid   = str(ath.get("id") or "").strip()
                name  = str(ath.get("displayName") or "").strip()
                stats = a.get("stats") or []
                if not aid or not name or not stats:
                    continue

                def getv(lab: str) -> str:
                    i = idx.get(lab)
                    if i is None or i >= len(stats):
                        return ""
                    return str(stats[i]).strip()

                mins = _parse_minutes(getv("MIN"))
                if np.isnan(mins) or mins <= 0:
                    continue

                fgm,  fga  = _parse_made_att(getv("FG"))
                fg3m, fg3a = _parse_made_att(getv("3PT"))
                ftm,  fta  = _parse_made_att(getv("FT"))

                rows.append({
                    "EVENT_ID":         event_id,
                    "GAME_DATE":        game_date,
                    "TEAM":             team_abbr,
                    "ESPN_ATHLETE_ID":  aid,
                    "PLAYER":           name,
                    "PLAYER_NORM":      _norm_name(name),
                    "MIN":              mins,
                    "PTS":  pd.to_numeric(getv("PTS"), errors="coerce"),
                    "REB":  pd.to_numeric(getv("REB"), errors="coerce"),
                    "AST":  pd.to_numeric(getv("AST"), errors="coerce"),
                    "STL":  pd.to_numeric(getv("STL"), errors="coerce"),
                    "BLK":  pd.to_numeric(getv("BLK"), errors="coerce"),
                    "TO":   pd.to_numeric(getv("TO"),  errors="coerce"),
                    "FGM":  fgm,  "FGA":  fga,
                    "FG3M": fg3m, "FG3A": fg3a,
                    "FTM":  ftm,  "FTA":  fta,
                })

    return pd.DataFrame(rows)

def _fetch_one_event_nba(
    eid: str,
    date_str: str,
    season: str,
    timeout: Tuple[float, float],
    retries: int,
    sleep_s: float,
    base_cols: List[str],
) -> Tuple[str, pd.DataFrame | None]:
    """Fetch and parse a single NBA ESPN event. Returns (eid, df | None)."""
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/nba/summary?event={eid}"
    try:
        # Spread load: each thread sleeps a random fraction so they don't all
        # hammer ESPN at the same millisecond.
        time.sleep(random.uniform(0.1, sleep_s))
        summ = espn_get_json(url, timeout=timeout, retries=retries, sleep_s=0.0)
        df = parse_summary_boxscore(summ)
        if df.empty:
            return eid, None
        df["SEASON"]    = season
        df["EVENT_ID"]  = df["EVENT_ID"].replace("", eid)
        df["GAME_DATE"] = df["GAME_DATE"].replace("", date_str)
        return eid, df[base_cols].copy()
    except Exception as e:
        print(f"  [WARN] ESPN summary failed event={eid}: {type(e).__name__}: {e}")
        return eid, None


def update_espn_cache(
    cache_path: Path,
    season: str,
    date_list: List[str],
    timeout: Tuple[float, float],
    retries: int,
    sleep_s: float,
    workers: int = 4,          # ← parallel game fetches (safe for ESPN)
) -> pd.DataFrame:
    """
    Parallelized ESPN cache updater.

    Strategy:
    - Scoreboards (one per date) are fetched sequentially — cheap, just IDs.
    - Game summaries (boxscores) are fetched in parallel with ThreadPoolExecutor.
      workers=4 is safe; ESPN rarely rate-limits at this concurrency.
    - Each worker adds a random jitter so requests don't burst simultaneously.
    """
    from concurrent.futures import ThreadPoolExecutor, as_completed

    base_cols = [
        "SEASON", "EVENT_ID", "GAME_DATE", "TEAM",
        "ESPN_ATHLETE_ID", "PLAYER", "PLAYER_NORM", "MIN",
        "PTS", "REB", "AST", "STL", "BLK", "TO",
        "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA",
    ]
    if cache_path.exists():
        cache = pd.read_csv(cache_path, dtype=str).fillna("")
        print(f"Loaded ESPN cache: {cache_path.name} | rows={len(cache)}")
    else:
        cache = pd.DataFrame(columns=base_cols)
        print(f"ESPN cache not found, creating: {cache_path.name}")

    for c in base_cols:
        if c not in cache.columns:
            cache[c] = ""

    existing_events = set(
        cache.loc[cache["SEASON"].astype(str) == season, "EVENT_ID"].astype(str).tolist()
    )

    # Phase 1: collect all event IDs that need fetching (sequential — fast)
    pending: List[Tuple[str, str]] = []   # (eid, date_str)
    skipped_scoreboards = 0
    print(f"\n-> Scanning {len(date_list)} dates for new NBA events...")
    for d in date_list:
        yyyymmdd = d.replace("-", "")
        try:
            event_ids = fetch_espn_event_ids(yyyymmdd, timeout=timeout, retries=retries, sleep_s=sleep_s)
        except Exception as e:
            print(f"  [WARN] ESPN scoreboard failed {d}: {e}")
            skipped_scoreboards += 1
            continue
        for eid in event_ids:
            if eid not in existing_events:
                pending.append((eid, d))

    if not pending:
        print(f"\nESPN cache already up to date (0 new events, {skipped_scoreboards} scoreboard errors)")
        return cache

    print(f"-> Fetching {len(pending)} new events with {workers} parallel workers...")

    # Phase 2: fetch game summaries in parallel
    new_frames: List[pd.DataFrame] = []
    new_events = 0
    skipped_events = 0

    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {
            pool.submit(
                _fetch_one_event_nba,
                eid, date_str, season, timeout, retries, sleep_s, base_cols
            ): (eid, date_str)
            for eid, date_str in pending
        }
        for future in as_completed(futures):
            eid, df = future.result()
            if df is not None:
                new_frames.append(df)
                existing_events.add(eid)
                new_events += 1
                print(f"  cached NBA event {eid} | players={len(df)}")
            else:
                skipped_events += 1

    if new_frames:
        add    = pd.concat(new_frames, ignore_index=True)
        cache2 = pd.concat([cache, add], ignore_index=True)
        cache2 = cache2.drop_duplicates(subset=["SEASON", "EVENT_ID", "ESPN_ATHLETE_ID"], keep="last")
        cache2.to_csv(cache_path, index=False, encoding="utf-8")
        print(f"\nNBA ESPN cache updated: {cache_path.name} | rows={len(cache2)} | new={new_events} | skipped={skipped_events}")
        return cache2

    print(f"\nNo new rows added (new_events={new_events}, skipped={skipped_events})")
    return cache

# ── ID MAP ────────────────────────────────────────────────────────────────────

def load_idmap(path: Path) -> pd.DataFrame:
    cols = ["nba_player_id", "team", "player", "espn_athlete_id", "updated_at"]
    if path.exists():
        df = pd.read_csv(path, dtype=str).fillna("")
        for c in cols:
            if c not in df.columns:
                df[c] = ""
        return df[cols].copy()
    return pd.DataFrame(columns=cols)

def upsert_idmap(df_map: pd.DataFrame, nba_player_id: str, team: str, player: str, espn_athlete_id: str) -> pd.DataFrame:
    nba_player_id = _clean_id(nba_player_id)
    if not nba_player_id or not espn_athlete_id:
        return df_map
    now = datetime.now().isoformat(timespec="seconds")
    key = df_map["nba_player_id"].astype(str) == nba_player_id
    if key.any():
        df_map.loc[key, ["team", "player", "espn_athlete_id", "updated_at"]] = [team, player, espn_athlete_id, now]
        return df_map
    return pd.concat([df_map, pd.DataFrame([{
        "nba_player_id": nba_player_id, "team": team,
        "player": player, "espn_athlete_id": str(espn_athlete_id), "updated_at": now,
    }])], ignore_index=True)

def build_cache_lookup(cache: pd.DataFrame) -> Tuple[Dict[Tuple[str, str], str], Dict[str, str]]:
    c = cache.copy()
    c["_dt"] = pd.to_datetime(c.get("GAME_DATE", ""), errors="coerce")
    c = c.sort_values("_dt", ascending=False)

    map_team: Dict[Tuple[str, str], str] = {}
    map_name: Dict[str, str] = {}

    for _, r in c.iterrows():
        n   = str(r.get("PLAYER_NORM", "")).strip() or _norm_name(str(r.get("PLAYER", "")))
        t   = str(r.get("TEAM", "")).strip()
        aid = str(r.get("ESPN_ATHLETE_ID", "")).strip()
        if not n or not aid:
            continue
        if n not in map_name:
            map_name[n] = aid
        if t and (n, t) not in map_team:
            map_team[(n, t)] = aid

    return map_team, map_name

def resolve_espn_id(
    player: str,
    team: str,
    map_team: Dict[Tuple[str, str], str],
    map_name: Dict[str, str],
) -> str:
    n = _norm_name(player)
    t = str(team or "").strip()
    if not n:
        return ""
    aid = map_name.get(n, "")
    if aid:
        return aid
    if t:
        return map_team.get((n, t), "")
    return ""

# ── stat attachment ───────────────────────────────────────────────────────────

# FIX 6: filter by SEASON + ESPN_ATHLETE_ID so we do NOT mix seasons
# FIX 7: cutoff_date filters to only games within the rolling window (avoids stale early-season games)
# NOTE: MIN filter removed — include all games regardless of minutes played
def get_vals_for_athlete(
    cache: pd.DataFrame, season: str, aid: str, prop: str,
    cutoff_date: "pd.Timestamp | None" = None,
) -> List[float]:
    mask = (
        (cache["SEASON"].astype(str) == str(season)) &
        (cache["ESPN_ATHLETE_ID"].astype(str) == str(aid))
    )
    if cutoff_date is not None:
        mask = mask & (cache["GAME_DATE"] >= cutoff_date)
    dfp = cache.loc[mask].copy()
    if dfp.empty:
        return []
    dfp = dfp.sort_values("GAME_DATE", ascending=False)
    dfp["STAT"] = derive_stat_series(dfp, prop)
    return pd.to_numeric(dfp["STAT"], errors="coerce").dropna().astype(float).tolist()

# ── main ──────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate",         default="step3_with_defense.csv")
    ap.add_argument("--out",           default="step4_with_stats.csv")
    ap.add_argument("--season",        default="2025-26")
    ap.add_argument("--date",          required=True, help="Slate date YYYY-MM-DD")
    ap.add_argument("--days",          type=int,   default=35)
    ap.add_argument("--cache",         default="nba_espn_boxscore_cache.csv")
    ap.add_argument("--idmap",         default="nba_to_espn_id_map.csv")
    ap.add_argument("--debug-misses",  default="",  help="CSV for NO_ESPN_PLAYER rows")
    ap.add_argument("--n",             type=int,   default=10)
    ap.add_argument("--connect-timeout", type=float, default=8.0)
    ap.add_argument("--timeout",       type=float, default=30.0)
    ap.add_argument("--sleep",         type=float, default=0.8)
    ap.add_argument("--retries",       type=int,   default=4)
    ap.add_argument("--workers",       type=int,   default=4,
                    help="Parallel workers for ESPN game summary fetches (default 4)")
    args = ap.parse_args()

    timeout = (args.connect_timeout, args.timeout)

    print(f"-> Loading slate: {args.slate}")
    slate = pd.read_csv(args.slate, dtype=str).fillna("")
    if "nba_player_id" not in slate.columns:
        raise RuntimeError("slate missing nba_player_id")
    if "prop_norm" not in slate.columns:
        if "prop_type" in slate.columns:
            slate["prop_norm"] = slate["prop_type"].astype(str).str.lower()
        else:
            raise RuntimeError("slate missing prop_norm (and prop_type)")

    slate["_line_num"] = pd.to_numeric(slate.get("line", ""), errors="coerce")

    end_dt   = datetime.strptime(args.date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=int(args.days))
    date_list = [
        (start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
        for i in range((end_dt - start_dt).days + 1)
    ]

    cache_path = Path(args.cache)
    cache = update_espn_cache(
        cache_path=cache_path,
        season=args.season,
        date_list=date_list,
        timeout=timeout,
        retries=int(args.retries),
        sleep_s=max(float(args.sleep), 0.2),
        workers=int(args.workers),
    )

    for c in ["MIN", "PTS", "REB", "AST", "STL", "BLK", "TO", "FGM", "FGA", "FG3M", "FG3A", "FTM", "FTA"]:
        cache[c] = pd.to_numeric(cache.get(c, ""), errors="coerce")
    cache["GAME_DATE"] = pd.to_datetime(cache.get("GAME_DATE", ""), errors="coerce")

    # FIX 7: derive the rolling-window cutoff so stat pulls respect --days
    cutoff_date = pd.Timestamp(start_dt)

    map_team, map_name = build_cache_lookup(cache)

    idmap_path = Path(args.idmap)
    df_map     = load_idmap(idmap_path)
    map_direct: Dict[str, str] = dict(
        zip(df_map["nba_player_id"].astype(str), df_map["espn_athlete_id"].astype(str))
    )

    N = int(args.n)
    stat_cols = [f"stat_g{i}" for i in range(1, N + 1)]
    out_cols  = stat_cols + [
        "stat_last5_avg", "stat_last10_avg", "stat_season_avg",
        "last5_over", "last5_under", "last5_push", "last5_hit_rate",
        "line_hit_rate_over_ou_5",  "line_hit_rate_under_ou_5",
        "line_hit_rate_over_ou_10", "line_hit_rate_under_ou_10",
        "stat_status",
    ]
    for c in out_cols:
        if c not in slate.columns:
            slate[c] = ""

    misses_rows: List[dict] = []
    map_updates = 0

    print(f"\n-> Attaching stats from ESPN cache -> rows={len(slate)}")

    for idx, row in slate.iterrows():
        prop       = str(row.get("prop_norm", "")).lower().strip()
        team       = str(row.get("team", "")).strip()
        player     = str(row.get("player", "")).strip()
        nba_pid_raw = str(row.get("nba_player_id", "")).strip()

        line = row.get("_line_num", np.nan)
        try:
            line = float(line)
        except Exception:
            line = np.nan

        ids      = _parse_ids(nba_pid_raw)
        is_combo = (len(ids) > 1) or (
            str(row.get("is_combo_player", "")).strip().lower() in ("1", "true", "yes")
        )

        def _resolve_one(nba_pid_str: str, pname: str, pteam: str) -> str:
            nonlocal map_updates, df_map, map_direct
            pid_key = _clean_id(nba_pid_str)
            aid = map_direct.get(pid_key, "").strip() if pid_key else ""
            if aid:
                return aid
            aid = resolve_espn_id(pname, pteam, map_team, map_name)
            if aid and pid_key:
                df_map = upsert_idmap(df_map, pid_key, pteam, pname, aid)
                map_direct[pid_key] = aid
                map_updates += 1
            return aid

        # ── single player ─────────────────────────────────────────────────────
        if not is_combo:
            nba_pid = _clean_id(nba_pid_raw)
            aid = _resolve_one(nba_pid, player, team)
            if not aid:
                slate.at[idx, "stat_status"] = "NO_ESPN_PLAYER"
                misses_rows.append({
                    "player": player, "team": team, "prop_norm": prop,
                    "line": str(row.get("line", "")), "nba_player_id": nba_pid_raw,
                    "player_1": row.get("player_1", ""), "player_2": row.get("player_2", ""),
                    "team_1": row.get("team_1", ""),     "team_2": row.get("team_2", ""),
                    "is_combo_player": row.get("is_combo_player", ""),
                })
                continue

            # FIX 6 + FIX 7 applied here
            vals = get_vals_for_athlete(cache, args.season, aid, prop, cutoff_date=cutoff_date)
            if not vals:
                slate.at[idx, "stat_status"] = "NO_CACHE_PLAYER"
                continue

        # ── FIX 3: N-player combo via EVENT_ID intersection ──────────────────
        else:
            p_names = [
                str(row.get(f"player_{i}", "")).strip() or player
                for i in range(1, len(ids) + 1)
            ]
            p_teams = [
                str(row.get(f"team_{i}", "")).strip() or team
                for i in range(1, len(ids) + 1)
            ]

            aids: List[str] = []
            for i, pid_int in enumerate(ids):
                pname = p_names[i] if i < len(p_names) else player
                pteam = p_teams[i] if i < len(p_teams) else team
                aid = _resolve_one(str(pid_int), pname, pteam)
                if not aid:
                    break
                aids.append(aid)

            if len(aids) < len(ids):
                slate.at[idx, "stat_status"] = "NO_ESPN_PLAYER"
                misses_rows.append({
                    "player": player, "team": team, "prop_norm": prop,
                    "line": str(row.get("line", "")), "nba_player_id": nba_pid_raw,
                    "player_1": row.get("player_1", ""), "player_2": row.get("player_2", ""),
                    "team_1": row.get("team_1", ""),     "team_2": row.get("team_2", ""),
                    "is_combo_player": row.get("is_combo_player", ""),
                })
                continue

            per_player_vals = []
            any_empty = False
            for aid in aids:
                dfp = cache.loc[
                    (cache["SEASON"].astype(str) == str(args.season)) &
                    (cache["ESPN_ATHLETE_ID"].astype(str) == str(aid)) &
                    (cache["GAME_DATE"] >= cutoff_date)
                ].copy()
                if dfp.empty:
                    any_empty = True
                    break
                dfp = dfp.sort_values("GAME_DATE", ascending=False)
                dfp["STAT"] = derive_stat_series(dfp, prop)
                pvals = pd.to_numeric(dfp["STAT"], errors="coerce").dropna().astype(float).tolist()
                if not pvals:
                    any_empty = True
                    break
                per_player_vals.append(pvals)

            if any_empty or not per_player_vals:
                slate.at[idx, "stat_status"] = "NO_CACHE_PLAYER"
                continue

            min_games = min(len(pv) for pv in per_player_vals)
            vals = [
                float(sum(pv[i] for pv in per_player_vals))
                for i in range(min_games)
            ]

            if not vals:
                slate.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue

        # ── fill output columns ───────────────────────────────────────────────
        for i in range(1, N + 1):
            v = vals[i - 1] if (i - 1) < len(vals) else np.nan
            slate.at[idx, f"stat_g{i}"] = fmt_num(v)

        def avg_k(k: int) -> float:
            s = vals[:k] if len(vals) >= k else vals
            return float(np.mean(s)) if s else np.nan

        slate.at[idx, "stat_last5_avg"]  = fmt_num(avg_k(5))
        slate.at[idx, "stat_last10_avg"] = fmt_num(avg_k(10))
        slate.at[idx, "stat_season_avg"] = fmt_num(float(np.mean(vals)))

        if not np.isnan(line):
            o5, u5, p5, hr5_all, hr5_ou, ur5_ou = calc_hit_context(vals, line, k=5)
            slate.at[idx, "last5_over"]               = str(o5)
            slate.at[idx, "last5_under"]              = str(u5)
            slate.at[idx, "last5_push"]               = str(p5)
            slate.at[idx, "last5_hit_rate"]           = fmt_num(hr5_all)
            slate.at[idx, "line_hit_rate_over_ou_5"]  = fmt_num(hr5_ou)
            slate.at[idx, "line_hit_rate_under_ou_5"] = fmt_num(ur5_ou)

            _, _, _, _, hr10_ou, ur10_ou = calc_hit_context(vals, line, k=10)
            slate.at[idx, "line_hit_rate_over_ou_10"]  = fmt_num(hr10_ou)
            slate.at[idx, "line_hit_rate_under_ou_10"] = fmt_num(ur10_ou)

        slate.at[idx, "stat_status"] = "OK"

    if map_updates > 0:
        df_map = df_map.drop_duplicates(subset=["nba_player_id"], keep="last")
        df_map.to_csv(idmap_path, index=False, encoding="utf-8")
        print(f"\nUpdated idmap: {idmap_path.name} | rows={len(df_map)} | updates={map_updates}")
    else:
        print("\nidmap unchanged")

    if args.debug_misses and misses_rows:
        miss_df = pd.DataFrame(misses_rows).drop_duplicates()
        miss_df.to_csv(args.debug_misses, index=False, encoding="utf-8")
        print(f"Wrote misses -> {args.debug_misses} | rows={len(miss_df)}")

    slate = slate.drop(columns=["_line_num"], errors="ignore")
    slate.to_csv(args.out, index=False, encoding="utf-8")
    print(f"\nSaved -> {args.out}")
    print("\nstat_status breakdown:")
    print(slate["stat_status"].astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()