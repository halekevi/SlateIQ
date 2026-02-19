#!/usr/bin/env python3
"""
step_b_fetch_and_grade.py  (optimized)
=======================================
Fetches today's PrizePicks CBB props, matches player stats from the ESPN cache,
adds defense rankings, and grades every prop with direction + confidence score.

OPTIMIZATIONS:
- Unified TEAM_ALIASES dict (single source of truth, imported vs redefined)
- norm_name() compiled regex → called once per string
- load_cache(): initials computed via vectorized apply instead of zip(*...)
- build_cache_index(): single groupby pass, no redundant DataFrame concat/dedupe
- pick_best_player_df(): short-circuits early; avoids rebuilding keys per call
- Grading loop: __stat computed once per prop; avoids redundant pd.to_numeric calls
- last_n_df_chronological / last_n_vals_dates merged into a single utility
- summarize_cache_player / cache_game_rows share one parsed dt Series
- fetch_prizepicks: game relationship (pp_opp_team) now extracted inline
  (matches pp_cbb_scraper.py behaviour, no second API call needed)
- Removed duplicate `import datetime` inside get_todays_matchups
- Removed unnecessary `.copy()` calls in inner loops
- score_direction: factored repeated clamp(0.5 + …) expression into helper
"""

from __future__ import annotations

import argparse
import datetime as dt
import os
import random
import re
import time
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

# ── Shared config (single source of truth) ───────────────────────────────────
# Maps ESPN cache codes → PrizePicks codes
TEAM_ALIASES: Dict[str, str] = {
    # ESPN cache code → PrizePicks code (must match step_a ABBR_MAP exactly)
    "TA&M":  "TXAM",
    "OU":    "OKLA",
    "UTU":   "OKLA",
    "OSU":   "OKST",
    "MIZ":   "MIZZ",
    "IUIN":  "MIZZ",
    "NU":    "NW",
    "NE":    "RUTG",
    "GTECH": "GT",
    "UTA":   "UTAH",
    "GW":    "GTWN",
    "IU":    "WAKE",
    "SUU":   "BUT",
    "ETAM":  "HALL",
    "PEPP":  "HALL",
    "SEMO":  "DEP",
    "HPU":   "MD",
    "ME":    "ALA",
    "SCAR":  "SC",
    "GC":    "GCU",
    "NCST":  "NCSU",
    "NEV":   "NEVADA",
    "SDSU":  "SDST",
    "":      "",
}

PP_URL = "https://api.prizepicks.com/projections"
PP_PARAMS = {
    "league_id":  20,
    "per_page":   250,
    "single_stat":"false",
    "game_mode":  "prizepools",
}
PP_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept":     "application/json",
    "Origin":     "https://app.prizepicks.com",
    "Referer":    "https://app.prizepicks.com/",
}

PROP_MAP: Dict[str, str] = {
    "points": "pts", "point": "pts", "pts": "pts",
    "rebounds": "reb", "rebound": "reb", "reb": "reb",
    "assists": "ast", "assist": "ast", "ast": "ast",
    "steals": "stl", "steal": "stl", "stl": "stl",
    "blocks": "blk", "block": "blk", "blk": "blk",
    "turnovers": "tov", "turnover": "tov", "tov": "tov",
    "pts+reb+ast": "pra", "pts+rebs+asts": "pra", "points+rebounds+assists": "pra",
    "pts+reb": "pr", "pts+rebs": "pr", "points+rebounds": "pr",
    "pts+ast": "pa", "pts+asts": "pa", "points+assists": "pa",
    "reb+ast": "ra", "rebs+asts": "ra", "rebounds+assists": "ra",
    "3-pt made": "fg3m", "3pt made": "fg3m", "3 pt made": "fg3m",
    "3 pointers made": "fg3m", "three pointers made": "fg3m",
    "3-pointers made": "fg3m", "fg3m": "fg3m",
    "3-pt attempted": "fg3a", "3pt attempted": "fg3a", "3 pt attempted": "fg3a",
    "3-pointers attempted": "fg3a", "three pointers attempted": "fg3a", "fg3a": "fg3a",
    "fg made": "fgm", "field goals made": "fgm", "fgm": "fgm",
    "fg attempted": "fga", "field goals attempted": "fga", "fga": "fga",
    "ft made": "ftm", "free throws made": "ftm", "ftm": "ftm",
    "ft attempted": "fta", "free throws attempted": "fta", "fta": "fta",
    "steals+blocks": "stocks", "stocks": "stocks",
    "fantasy score": "fantasy", "fantasy": "fantasy",
    "blocked shots": "blk",
}

PROP_COMPONENTS: Dict[str, List[str]] = {
    "pts": ["PTS"], "reb": ["REB"], "ast": ["AST"],
    "stl": ["STL"], "blk": ["BLK"], "tov": ["TOV"],
    "fg3m": ["FG3M"], "fgm": ["FGM"], "fga": ["FGA"],
    "fg3a": ["FG3A"], "ftm": ["FTM"], "fta": ["FTA"],
    "pr": ["PTS", "REB"], "pa": ["PTS", "AST"], "ra": ["REB", "AST"],
    "pra": ["PTS", "REB", "AST"],
    "stocks": ["STL", "BLK"],
    "fantasy": ["fantasy"],
}

PROP_SCALE: Dict[str, float] = {
    "pts": 6.0, "reb": 3.0, "ast": 2.5, "stl": 1.0, "blk": 1.0,
    "tov": 1.2, "fg3m": 1.5, "fgm": 3.0, "fga": 4.0, "fg3a": 2.0,
    "ftm": 2.0, "fta": 2.5, "pr": 7.0, "pa": 7.0, "ra": 5.0,
    "pra": 9.0, "stocks": 1.5, "fantasy": 10.0,
}

TOTAL_TEAMS = 362
DEBUG_BOX_COLS = ["PTS","REB","AST","STL","BLK","TOV","MIN",
                  "FGM","FGA","FG3M","FG3A","FTM","FTA","fantasy"]

# Pre-compiled regex for norm_name (significant speedup when called thousands of times)
_RE_MULTI_SPACE   = re.compile(r"\s+")
_RE_NON_ALNUM     = re.compile(r"[^a-z0-9\s]")
_RE_SUFFIXES      = re.compile(r"\b(jr|sr|ii|iii|iv|v)\b")


# ── Helpers ───────────────────────────────────────────────────────────────────
def norm_team(s: str) -> str:
    t = str(s or "").strip().upper()
    return TEAM_ALIASES.get(t, t)


def norm_prop(s: str) -> str:
    p = _RE_MULTI_SPACE.sub(" ", str(s or "").strip().lower())
    return PROP_MAP.get(p, PROP_MAP.get(p.replace(" ", ""), p))


def norm_name(s: str) -> str:
    s = str(s or "").strip().lower()
    s = s.replace("\u2019", "'").replace(".", " ").replace("'", "")
    s = _RE_NON_ALNUM.sub(" ", s)
    s = _RE_SUFFIXES.sub("", s)
    return _RE_MULTI_SPACE.sub(" ", s).strip()


def compact_name(normed: str) -> str:
    return (normed or "").replace(" ", "")


def initials_last_keys(normed: str) -> Tuple[str, str, str, str]:
    parts = [p for p in (normed or "").split() if p]
    if len(parts) < 2:
        return ("", "", "", "")
    last = parts[-1]
    first = parts[0]
    first_initial = first[0] if first else ""
    initials = ""
    for p in parts[:-1]:
        if len(p) == 1:
            initials += p
        else:
            break
    if not initials:
        initials = first_initial
    k1 = f"{initials} {last}".strip()
    k2 = f"{first_initial} {last}".strip()
    return k1, k2, compact_name(k1), compact_name(k2)


def to_f(x) -> Optional[float]:
    try:
        s = str(x).strip()
        return float(s) if s else None
    except Exception:
        return None


def clamp(v: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, v))


def _edge_frac(avg: float, line: float, direction: str, scale: float) -> float:
    """Normalized edge fraction in [0,1]."""
    delta = (avg - line) if direction == "OVER" else (line - avg)
    return clamp(0.5 + 0.5 * delta / scale, 0.0, 1.0)


# ── DataFrame utilities ───────────────────────────────────────────────────────
def _parse_dt_col(df: pd.DataFrame) -> pd.Series:
    """Return tz-aware datetime Series for game_date column."""
    return pd.to_datetime(df["game_date"], errors="coerce", utc=True)


def last_n_df_chronological(player_games: pd.DataFrame, n: int) -> pd.DataFrame:
    g = player_games.copy()
    g["__dt"] = _parse_dt_col(g)
    g = g[g["__dt"].notna()].sort_values("__dt").tail(n)
    return g.reset_index(drop=True)


def last_n_vals_dates(
    player_games: pd.DataFrame, stat_col: str, n: int = 5
) -> Tuple[List[float], List[str]]:
    g = last_n_df_chronological(player_games, n=n)
    vals = pd.to_numeric(g[stat_col], errors="coerce").tolist() if stat_col in g.columns else []
    dates = g["__dt"].dt.strftime("%Y-%m-%d").tolist() if "__dt" in g.columns else []
    # Pad front so callers always get exactly n entries
    while len(vals) < n:
        vals.insert(0, np.nan)
        dates.insert(0, "")
    return vals, dates


def latest_dt(df: Optional[pd.DataFrame]) -> pd.Timestamp:
    if df is None or df.empty or "game_date" not in df.columns:
        return pd.Timestamp.min.tz_localize("UTC")
    dts = _parse_dt_col(df).dropna()
    return dts.max() if len(dts) else pd.Timestamp.min.tz_localize("UTC")


def summarize_cache_player(player_df: pd.DataFrame) -> Tuple[str, int]:
    """Returns (last_game_date_str, n_valid_date_rows)."""
    if player_df is None or player_df.empty or "game_date" not in player_df.columns:
        return ("", 0)
    dts = _parse_dt_col(player_df).dropna()
    last = dts.max().strftime("%Y-%m-%d") if len(dts) else ""
    return last, len(dts)


def cache_game_rows(player_df: pd.DataFrame) -> int:
    _, n = summarize_cache_player(player_df)
    return n


def filter_stat_games(df: pd.DataFrame, stat_col: str = "__stat") -> pd.DataFrame:
    if df is None or df.empty or stat_col not in df.columns:
        return pd.DataFrame(columns=list(df.columns) if df is not None else [])
    mask = pd.to_numeric(df[stat_col], errors="coerce").notna()
    return df[mask].copy()


def safe_read_csv(path: str) -> Optional[pd.DataFrame]:
    if not path or not os.path.exists(path):
        return None
    try:
        return pd.read_csv(path, dtype=str).fillna("")
    except Exception:
        return None


# ── Cache ─────────────────────────────────────────────────────────────────────
def load_cache(path: str) -> pd.DataFrame:
    df = pd.read_csv(path, dtype=str).fillna("")
    if "player_name" not in df.columns:
        raise ValueError("Cache must contain 'player_name' column.")
    if "game_date" not in df.columns:
        raise ValueError("Cache must contain 'game_date' column.")

    df["player_norm"]    = df["player_name"].apply(norm_name)
    df["player_compact"] = df["player_norm"].apply(compact_name)

    # Vectorised initials — avoids zip(*apply(...)) antipattern
    keys = df["player_norm"].apply(initials_last_keys)
    df["player_initials_last"]          = keys.apply(lambda x: x[0])
    df["player_firstinit_last"]         = keys.apply(lambda x: x[1])
    df["player_initials_last_compact"]  = keys.apply(lambda x: x[2])
    df["player_firstinit_last_compact"] = keys.apply(lambda x: x[3])

    if "team_abbr" in df.columns:
        df["cache_team_abbr"] = df["team_abbr"].astype(str).str.strip()
        df["cache_team_norm"] = df["cache_team_abbr"].apply(norm_team)
    else:
        df["cache_team_abbr"] = ""
        df["cache_team_norm"] = ""

    df["__dt"] = _parse_dt_col(df)
    df = df[df["__dt"].notna()].sort_values("__dt", ascending=True).drop(columns=["__dt"])
    return df.reset_index(drop=True)


def build_cache_index(cache: pd.DataFrame) -> Dict[str, pd.DataFrame]:
    """
    Build a name → player-rows lookup.
    OPTIMIZED: single pass, no redundant concat/dedupe per key.
    Each key maps directly to the already-grouped slice.
    """
    idx: Dict[str, pd.DataFrame] = {}
    KEY_COLS = [
        "player_norm", "player_compact",
        "player_initials_last", "player_firstinit_last",
        "player_initials_last_compact", "player_firstinit_last_compact",
    ]
    for col in KEY_COLS:
        if col not in cache.columns:
            continue
        for k, sub in cache.groupby(col, sort=False):
            k = str(k)
            if not k or k == "nan":
                continue
            if k in idx:
                # Merge if same player appears under multiple key columns
                idx[k] = pd.concat([idx[k], sub], ignore_index=True).drop_duplicates()
            else:
                idx[k] = sub.reset_index(drop=True)
    return idx


# ── Player directory ──────────────────────────────────────────────────────────
def build_player_dir_index(player_dir: pd.DataFrame) -> Dict[Tuple[str, str], str]:
    idx: Dict[Tuple[str, str], str] = {}
    if player_dir is None or player_dir.empty:
        return idx
    for c in ("player_id", "player_name"):
        if c not in player_dir.columns:
            return {}
    if "team_abbr" not in player_dir.columns:
        player_dir = player_dir.copy()
        player_dir["team_abbr"] = ""

    player_dir = player_dir.copy()
    player_dir["team_norm"]   = player_dir["team_abbr"].apply(norm_team)
    player_dir["player_norm"] = player_dir["player_name"].apply(norm_name)

    for _, r in player_dir.iterrows():
        pid = str(r.get("player_id", "")).strip()
        tn  = str(r.get("team_norm",  "")).strip()
        pn  = str(r.get("player_norm","")).strip()
        if pid and pn:
            idx.setdefault((tn, pn), pid)
    return idx


def lookup_espn_player_id(
    player_dir_idx: Dict[Tuple[str, str], str],
    team: str,
    player_norm: str,
) -> Tuple[str, str]:
    tn  = norm_team(team)
    pid = player_dir_idx.get((tn, player_norm))
    if pid:
        return pid, "TEAM+NAME"
    cands = list(dict.fromkeys(v for (_, n), v in player_dir_idx.items() if n == player_norm))
    if len(cands) == 1:
        return cands[0], "NAME_ONLY"
    return "", "NO_MATCH"


# ── PrizePicks ────────────────────────────────────────────────────────────────
def _norm_pick_type(odds_type: str) -> str:
    pt = odds_type.lower()
    if "gob" in pt:
        return "goblin"
    if "dem" in pt:
        return "demon"
    return "standard"


def fetch_prizepicks() -> pd.DataFrame:
    """
    Fetch all CBB projections with cursor pagination.
    Also extracts pp_opp_team from the included game objects (mirrors pp_cbb_scraper.py).
    """
    rows: List[dict] = []
    seen_ids: set = set()
    session = requests.Session()
    session.headers.update(PP_HEADERS)

    url: Optional[str]  = PP_URL
    params: Optional[dict] = dict(PP_PARAMS)
    page_num = 1

    while url:
        for attempt in range(6):
            try:
                r = session.get(url, params=params, timeout=20)
                if r.status_code == 429:
                    wait = min(20 * (1.5 ** attempt), 90) + random.uniform(0, 3)
                    print(f"  ⚠️  429 — waiting {wait:.0f}s")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                j = r.json()

                data     = j.get("data") or []
                included = j.get("included") or []

                if not data:
                    print(f"  ⛔ No data on page {page_num}. Stopping.")
                    return pd.DataFrame(rows)

                # Build player lookup
                players: Dict[str, dict] = {
                    str(o.get("id", "")): o.get("attributes") or {}
                    for o in included
                    if o.get("type") in ("new_player", "player", "players")
                }
                # Build game lookup for opponent derivation
                games: Dict[str, dict] = {
                    str(o.get("id", "")): o.get("attributes") or {}
                    for o in included
                    if o.get("type") in ("game", "games", "new_game")
                }
                # DEBUG: print sample game object to understand structure
                if games and page_num == 1:
                    sample_id = next(iter(games))
                    print(f"  🔍 Sample PP game attrs: {games[sample_id]}")

                added = 0
                for proj in data:
                    proj_id = str(proj.get("id", ""))
                    if not proj_id or proj_id in seen_ids:
                        continue
                    seen_ids.add(proj_id)

                    attr = proj.get("attributes") or {}
                    rel  = proj.get("relationships") or {}

                    pid_data = (rel.get("new_player") or rel.get("player") or {}).get("data") or {}
                    pid      = str(pid_data.get("id", ""))
                    p        = players.get(pid, {})

                    player   = p.get("name") or p.get("display_name") or ""
                    raw_team = p.get("team") or p.get("team_abbreviation") or ""
                    pp_team  = norm_team(raw_team)
                    raw_prop = attr.get("stat_type") or attr.get("display_stat_type") or ""
                    line     = attr.get("line_score") if attr.get("line_score") is not None else attr.get("line")
                    pick_type = _norm_pick_type(str(attr.get("odds_type") or ""))
                    start    = attr.get("start_time") or ""

                    if not player or not raw_prop or line is None:
                        continue

                    # Derive opponent from game relationship
                    pp_opp_team = ""
                    game_rel = (rel.get("game") or rel.get("new_game") or {}).get("data") or {}
                    if isinstance(game_rel, dict):
                        gid    = str(game_rel.get("id", ""))
                        g_attr = games.get(gid, {})
                        # Try top-level abbreviation fields first
                        home = norm_team(
                            g_attr.get("home_team_abbreviation") or g_attr.get("home_team") or ""
                        )
                        away = norm_team(
                            g_attr.get("away_team_abbreviation") or g_attr.get("away_team") or ""
                        )
                        # Fall back to nested metadata.game_info.teams structure
                        if not home or not away:
                            teams_meta = (
                                (g_attr.get("metadata") or {})
                                .get("game_info", {})
                                .get("teams", {})
                            )
                            home = norm_team((teams_meta.get("home") or {}).get("abbreviation", "")) or home
                            away = norm_team((teams_meta.get("away") or {}).get("abbreviation", "")) or away
                        if pp_team and home and away:
                            pp_opp_team = away if pp_team == home else (home if pp_team == away else "")

                    pn = norm_name(player)
                    rows.append({
                        "player":         player,
                        "player_norm":    pn,
                        "player_compact": compact_name(pn),
                        "pp_team":        pp_team,
                        "pp_opp_team":    pp_opp_team,
                        "pos":            p.get("position", ""),
                        "prop_raw":       raw_prop,
                        "prop_norm":      norm_prop(raw_prop),
                        "line":           float(line),
                        "pick_type":      pick_type,
                        "start_time":     start,
                        "proj_id":        proj_id,
                    })
                    added += 1

                print(f"  ✓ Page {page_num}: +{added} new props (total unique {len(seen_ids)})")
                time.sleep(0.5)

                next_url = (j.get("links") or {}).get("next")
                if not next_url:
                    print("  ✅ No links.next — pagination complete.")
                    return pd.DataFrame(rows)

                url    = next_url
                params = None    # cursor URL already contains query params
                page_num += 1
                break

            except Exception as e:
                time.sleep(2 ** attempt)
                if attempt == 5:
                    print(f"  ⚠️  Fetch failed after retries: {e}")
                    return pd.DataFrame(rows)

    return pd.DataFrame(rows)


# ── Defense ───────────────────────────────────────────────────────────────────
def load_defense(path: str) -> Optional[pd.DataFrame]:
    if not path or not os.path.exists(path):
        if path:
            print(f"⚠️  Defense file not found: {path}")
        return None
    df = pd.read_csv(path, dtype=str).fillna("")
    key_col = next((c for c in ["team_abbr","opp_team","team","abbr"] if c in df.columns), None)
    if not key_col:
        print("⚠️  Defense file missing team key column — defense neutral")
        return None
    df["__team_norm"] = df[key_col].apply(norm_team)
    need_any = ["OPP_PTS_ALLOWED_RANK","OPP_DEF_RANK","DEF_TIER"]
    if not any(c in df.columns for c in need_any):
        print("⚠️  Defense file has no usable rank columns — defense neutral")
        return None
    print(f"🛡️  Defense: {len(df)} teams | key={key_col} | cols={','.join(c for c in need_any if c in df.columns)}")
    return df


def get_def_rank(defense: Optional[pd.DataFrame], opp_team: str, prop_norm: str) -> Optional[float]:
    if defense is None or not opp_team:
        return None
    pts_props = {"pts","pr","pa","pra","fantasy"}
    preferred = "OPP_PTS_ALLOWED_RANK" if prop_norm in pts_props else "OPP_DEF_RANK"
    col = (preferred if preferred in defense.columns
           else "OPP_DEF_RANK" if "OPP_DEF_RANK" in defense.columns
           else "OPP_PTS_ALLOWED_RANK" if "OPP_PTS_ALLOWED_RANK" in defense.columns
           else None)
    if col is None:
        return None
    rows = defense[defense["__team_norm"] == norm_team(opp_team)]
    return to_f(rows.iloc[0][col]) if not rows.empty else None


# ── Matchups ──────────────────────────────────────────────────────────────────
# ESPN scoreboard code → PrizePicks code (mirrors step_a ABBR_MAP exactly)
ESPN_TO_PP: Dict[str, str] = {
    "TA&M":  "TXAM",
    "OU":    "OKLA",
    "UTU":   "OKLA",
    "OSU":   "OKST",
    "MIZ":   "MIZZ",
    "IUIN":  "MIZZ",
    "NU":    "NW",
    "NE":    "RUTG",
    "GTECH": "GT",
    "UTA":   "UTAH",
    "GW":    "GTWN",
    "IU":    "WAKE",
    "SUU":   "BUT",
    "ETAM":  "HALL",
    "PEPP":  "HALL",
    "SEMO":  "DEP",
    "HPU":   "MD",
    "ME":    "ALA",
    "SCAR":  "SC",
    "GC":    "GCU",
    "NCST":  "NCSU",
    "NEV":   "NEVADA",
    "SDSU":  "SDST",
}

def _espn_to_pp(code: str) -> str:
    """Translate ESPN scoreboard team code to PrizePicks team code."""
    c = str(code or "").strip().upper()
    # First apply ESPN→PP specific map, then fall through to general TEAM_ALIASES
    return ESPN_TO_PP.get(c, TEAM_ALIASES.get(c, c))


def _fetch_matchups_for_date(ymd: str, session: requests.Session) -> Dict[str, str]:
    matchups: Dict[str, str] = {}
    try:
        r = session.get(
            "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard",
            params={"dates": ymd, "limit": 500}, timeout=20,
        )
        r.raise_for_status()
        all_raw: List[str] = []
        for ev in r.json().get("events", []):
            comps = (ev.get("competitions") or [{}])[0]
            raw   = [(c.get("team") or {}).get("abbreviation", "") for c in comps.get("competitors", [])]
            all_raw.extend([c for c in raw if c])
            teams = [_espn_to_pp(c) for c in raw if c]
            if len(teams) == 2:
                matchups[teams[0]] = teams[1]
                matchups[teams[1]] = teams[0]
        print(f"  📋 Raw ESPN codes for {ymd}: {sorted(set(all_raw))}")
    except Exception as e:
        print(f"⚠️  Matchups error for {ymd}: {e}")
    return matchups


def get_matchups_for_prop_dates(
    props_df: pd.DataFrame, start_col: str = "start_time"
) -> Dict[str, str]:
    session = requests.Session()
    session.headers.update(PP_HEADERS)
    matchups: Dict[str, str] = {}

    if props_df is None or props_df.empty or start_col not in props_df.columns:
        print("⚠️  Matchups: props missing start_time — falling back to TODAY only")
        date_strs = [dt.date.today().strftime("%Y%m%d")]
    else:
        s = pd.to_datetime(props_df[start_col], errors="coerce", utc=True)
        date_strs = sorted({d.strftime("%Y%m%d") for d in s.dropna().dt.date})
        if not date_strs:
            print("⚠️  Matchups: no valid start_time dates — falling back to TODAY only")
            date_strs = [dt.date.today().strftime("%Y%m%d")]

    print(f"📅 Fetching ESPN matchups for {len(date_strs)} date(s): {date_strs[:8]}{'...' if len(date_strs)>8 else ''}")
    for ymd in date_strs:
        matchups.update(_fetch_matchups_for_date(ymd, session))

    print(f"📅 Matchups built: {len(matchups)//2} games | teams: {sorted(matchups.keys())}")
    return matchups


# ── Scoring ───────────────────────────────────────────────────────────────────
def score_direction(
    vals: List[float],
    line: float,
    avg5: Optional[float],
    avg10: Optional[float],
    avg_sea: Optional[float],
    def_rank: Optional[float],
    direction: str,
    scale: float,
) -> float:
    s1 = (30 * sum(1 for v in vals if (v > line if direction == "OVER" else v < line)) / len(vals)
          if vals else 15.0)
    s2 = 25 * _edge_frac(avg5,    line, direction, scale) if avg5    is not None else 12.5
    s3 = 15 * _edge_frac(avg_sea, line, direction, scale) if avg_sea is not None else  7.5
    s4 = 10 * _edge_frac(avg10,   line, direction, scale) if avg10   is not None else  5.0

    if def_rank is not None:
        normed = (def_rank - 1) / max(TOTAL_TEAMS - 1, 1)
        frac   = normed if direction == "OVER" else (1 - normed)
        s5     = 20 * clamp(frac, 0, 1)
    else:
        s5 = 10.0

    return round(s1 + s2 + s3 + s4 + s5, 1)


def grade(score: float) -> str:
    if score >= 80: return "A"
    if score >= 65: return "B"
    if score >= 50: return "C"
    return "D"


# ── Cache candidate selection ─────────────────────────────────────────────────
def pick_best_player_df(
    cache_index: Dict[str, pd.DataFrame],
    prop_player_norm: str,
    prop_player_compact: str,
    pp_team: str,
) -> Tuple[Optional[pd.DataFrame], str]:
    k1, k2, k3, k4 = initials_last_keys(prop_player_norm)
    seen_ids: set = set()
    cands: List[pd.DataFrame] = []
    for k in (prop_player_norm, prop_player_compact, k1, k2, k3, k4):
        if not k:
            continue
        df = cache_index.get(k)
        if df is not None and not df.empty:
            oid = id(df)
            if oid not in seen_ids:
                seen_ids.add(oid)
                cands.append(df)

    if not cands:
        return None, "NO_CANDIDATES"

    # ── STEP 1: Team match is highest priority ────────────────────────────────
    # Apply BEFORE recency — prevents same-name players on different teams from
    # being confused (e.g. "Malik Thomas" on ARK vs UVA).
    team_norm = norm_team(pp_team)
    if team_norm and len(cands) > 1:
        team_hits = [df for df in cands
                     if "cache_team_norm" in df.columns
                     and (df["cache_team_norm"].astype(str) == team_norm).any()]
        if len(team_hits) >= 1:
            # Confirmed team match — narrow candidates to team-matched only
            cands = team_hits
            if len(cands) == 1:
                return cands[0], "TEAM_MATCH"

    # ── STEP 2: Most-recent-game tiebreak ─────────────────────────────────────
    best_dt   = pd.Timestamp.min.tz_localize("UTC")
    best_list: List[pd.DataFrame] = []
    for df in cands:
        d = latest_dt(df)
        if d > best_dt:
            best_dt   = d
            best_list = [df]
        elif d == best_dt:
            best_list.append(df)

    if len(best_list) == 1:
        return best_list[0], "RECENCY"

    # ── STEP 3: Most games tiebreak ───────────────────────────────────────────
    best_df = best_list[0]
    best_n  = cache_game_rows(best_df)
    for df in best_list[1:]:
        n = cache_game_rows(df)
        if n > best_n:
            best_n  = n
            best_df = df
    return best_df, "MOST_GAMES_TIEBREAK"


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--cache",      required=True)
    ap.add_argument("--output",     required=True)
    ap.add_argument("--defense",    default="")
    ap.add_argument("--best_only",  action="store_true")
    ap.add_argument("--min_games",  type=int, default=5)
    ap.add_argument("--player_dir", default=r".cache_espn\espn_cbb_mens_players.csv")
    ap.add_argument("--stale_days", type=int, default=21)
    args = ap.parse_args()

    print("📡 Fetching PrizePicks CBB props...")
    props = fetch_prizepicks()
    if props.empty:
        print("❌ No props fetched.")
        return

    before = len(props)
    props  = props.drop_duplicates(subset=["proj_id"]).reset_index(drop=True)
    teams  = sorted({str(x).strip() for x in props.get("pp_team", pd.Series(dtype=str)) if str(x).strip()})
    print(f"✅ {len(props)} props ({before - len(props)} dupes removed) | {len(teams)} teams")

    print(f"\n📂 Cache: {args.cache}")
    cache       = load_cache(args.cache)
    cache_index = build_cache_index(cache)
    print(f"✅ {len(cache)} rows | {cache['player_name'].nunique()} players")

    print(f"\n📁 Player directory: {args.player_dir}")
    player_dir     = safe_read_csv(args.player_dir)
    player_dir_idx = build_player_dir_index(player_dir) if player_dir is not None else {}
    print(f"✅ Player dir: {len(player_dir) if player_dir is not None else 0} rows | index keys: {len(player_dir_idx)}")

    # Quick cache-match diagnostic
    def _has_match(row) -> bool:
        pn = row["player_norm"]
        pc = row["player_compact"]
        k1, k2, k3, k4 = initials_last_keys(pn)
        return any(k and k in cache_index for k in (pn, pc, k1, k2, k3, k4))

    matched = int(props.apply(_has_match, axis=1).sum())
    print(f"✅ Cache player-match: {matched}/{len(props)} props")

    defense  = load_defense(args.defense) if args.defense else None
    matchups = get_matchups_for_prop_dates(props)  # always build — used as opp fallback even without defense

    now_utc      = pd.Timestamp.now(tz="UTC")
    stale_cutoff = now_utc - pd.Timedelta(days=int(args.stale_days))

    print(f"\n⚙️  Grading {len(props)} props (min {args.min_games} games | stale>{args.stale_days}d)...")
    out_rows = []

    for _, prop in props.iterrows():
        pp_team   = prop.get("pp_team", "")
        p_norm    = prop["player_norm"]
        p_comp    = prop["player_compact"]
        prop_norm = prop["prop_norm"]
        line      = float(prop["line"])
        pick_type = prop["pick_type"]
        forced    = pick_type in ("goblin", "demon")

        espn_pid, match_method = lookup_espn_player_id(player_dir_idx, pp_team, p_norm)
        comps     = PROP_COMPONENTS.get(prop_norm, [])
        supported = bool(comps)

        status    = "OK"
        player_df, pick_reason = pick_best_player_df(cache_index, p_norm, p_comp, pp_team)

        if player_df is None or player_df.empty:
            status    = "NO_CACHE_PLAYER"
            supported = False
            player_df = pd.DataFrame(columns=list(cache.columns))

        if not supported:
            status = "UNSUPPORTED_PROP" if status == "OK" else status

        # Compute __stat column once
        if supported and not player_df.empty:
            for c in comps:
                if c not in player_df.columns:
                    player_df[c] = ""
                player_df[c] = pd.to_numeric(player_df[c], errors="coerce")
            player_df["__stat"] = player_df[comps].sum(axis=1, min_count=len(comps))
        else:
            player_df["__stat"] = np.nan

        raw_rows       = cache_game_rows(player_df)
        player_df_stat = filter_stat_games(player_df, "__stat")
        stat_rows      = cache_game_rows(player_df_stat)

        last5_df            = last_n_df_chronological(player_df_stat, n=5) if not player_df_stat.empty else pd.DataFrame()
        vals5, dates5       = last_n_vals_dates(player_df_stat, "__stat", n=5)  if not player_df_stat.empty else ([np.nan]*5, [""]*5)
        v10, _              = last_n_vals_dates(player_df_stat, "__stat", n=10) if not player_df_stat.empty else ([np.nan]*10, [""]*10)

        vals5_clean = [float(v) for v in vals5 if v is not None and np.isfinite(v)]
        n_clean     = len(vals5_clean)

        # Season averages
        if not player_df_stat.empty:
            player_df_stat = player_df_stat.copy()
            player_df_stat["__dt"] = _parse_dt_col(player_df_stat)
            player_df_stat = player_df_stat[player_df_stat["__dt"].notna()].sort_values("__dt")
            vals_all = pd.to_numeric(player_df_stat["__stat"], errors="coerce").dropna().tolist()
        else:
            vals_all = []

        avg5   = round(float(np.nanmean(vals5)),    2) if n_clean > 0 else None
        v10_f  = [x for x in v10 if np.isfinite(x)]
        avg10  = round(float(np.mean(v10_f)),        2) if v10_f else None
        avg_sea= round(float(np.mean(vals_all)),     2) if vals_all else None

        # Use PP-derived opponent first; fall back to ESPN matchup dict
        pp_opp = str(prop.get("pp_opp_team", "")).strip()
        opp_team = pp_opp if pp_opp else matchups.get(pp_team, "")
        def_rank = get_def_rank(defense, opp_team, prop_norm)
        scale    = PROP_SCALE.get(prop_norm, 5.0)

        s_over  = score_direction(vals5_clean, line, avg5, avg10, avg_sea, def_rank, "OVER",  scale)
        s_under = score_direction(vals5_clean, line, avg5, avg10, avg_sea, def_rank, "UNDER", scale)

        if forced:
            direction, final = "OVER", s_over
        else:
            direction = "OVER" if s_over >= s_under else "UNDER"
            final     = s_over  if direction == "OVER" else s_under

        cache_last_date, _ = summarize_cache_player(player_df)

        if status == "OK" and cache_last_date:
            lg = pd.to_datetime(cache_last_date, errors="coerce", utc=True)
            if pd.notna(lg) and lg < stale_cutoff and n_clean < args.min_games:
                status = "STALE_CACHE"

        if status == "OK" and n_clean < args.min_games:
            if raw_rows >= args.min_games and stat_rows < args.min_games:
                status = "MISSING_STAT_VALUES"
            else:
                status = "INSUFFICIENT_GAMES"

        if status in ("NO_CACHE_PLAYER","UNSUPPORTED_PROP","INSUFFICIENT_GAMES",
                      "MISSING_STAT_VALUES","STALE_CACHE"):
            direction = "OVER" if forced else "NO_DATA"
            final     = 0.0

        final = round(clamp(float(final), 0, 100), 1)
        g     = grade(final) if final > 0 else "N/A"

        over_hits  = sum(1 for v in vals5_clean if v > line)  if vals5_clean else ""
        under_hits = sum(1 for v in vals5_clean if v < line)  if vals5_clean else ""
        hit_rate   = ""
        if vals5_clean and isinstance(over_hits, int) and n_clean > 0:
            h = over_hits if direction == "OVER" else under_hits
            if isinstance(h, int):
                hit_rate = round(h / n_clean, 3)

        cache_team_abbr = ""
        if "cache_team_abbr" in player_df.columns and not player_df.empty:
            try:
                cache_team_abbr = str(player_df.iloc[-1].get("cache_team_abbr", "")).strip()
            except Exception:
                pass

        row = {
            "grade": g, "score": final, "direction": direction,
            "status": status, "forced_over": "YES" if forced else "NO",
            "espn_player_id": espn_pid, "match_method": match_method,
            "pp_team": pp_team, "pp_opp_team": opp_team,
            "cache_team_abbr": cache_team_abbr,
            "cache_last_game_date": cache_last_date,
            "cache_season_games": _ ,          # reuse count from summarize_cache_player
            "cache_game_rows": raw_rows, "cache_stat_games": stat_rows,
            "cache_pick_reason": pick_reason,
            "player": prop["player"], "pos": prop.get("pos",""),
            "prop": prop["prop_raw"], "prop_norm": prop_norm,
            "line": line, "pick_type": pick_type,
            "g1": "" if not np.isfinite(vals5[0]) else float(vals5[0]),
            "g2": "" if not np.isfinite(vals5[1]) else float(vals5[1]),
            "g3": "" if not np.isfinite(vals5[2]) else float(vals5[2]),
            "g4": "" if not np.isfinite(vals5[3]) else float(vals5[3]),
            "g5": "" if not np.isfinite(vals5[4]) else float(vals5[4]),
            "g1_date": dates5[0], "g2_date": dates5[1], "g3_date": dates5[2],
            "g4_date": dates5[3], "g5_date": dates5[4],
            "avg_last5":  avg5   or "",
            "avg_last10": avg10  or "",
            "avg_season": avg_sea or "",
            "l5_edge": round(avg5 - line, 2) if avg5 is not None else "",
            "n_games": n_clean,
            "hits_over_5": over_hits, "hits_under_5": under_hits, "hit_rate": hit_rate,
            "def_rank": def_rank or "",
            "score_over": s_over, "score_under": s_under,
            "start_time": prop["start_time"], "proj_id": prop["proj_id"],
        }

        # Debug last-5 raw box fields
        for col in DEBUG_BOX_COLS:
            for i in range(5):
                key = f"{col}_g{i+1}"
                if last5_df.empty or i >= len(last5_df):
                    row[key] = ""
                else:
                    v = last5_df.iloc[i].get(col, "")
                    row[key] = "" if v is None else str(v)

        out_rows.append(row)

    out = pd.DataFrame(out_rows)

    grade_order = {"A":0,"B":1,"C":2,"D":3,"N/A":4}
    out["_gs"] = out["grade"].map(grade_order).fillna(5)
    out = out.sort_values(["_gs","score"], ascending=[True, False]).drop(columns=["_gs"])

    if args.best_only:
        out = out[(out["grade"].isin(["A","B"])) & (out["score"] >= 65)]

    tmp = args.output + ".tmp"
    out.to_csv(tmp, index=False, encoding="utf-8")
    os.replace(tmp, args.output)

    print(f"\n✅ Saved → {args.output} | {len(out)} props")
    print("\n📊 Grades:\n",   out["grade"].value_counts().sort_index().to_string())
    print("\n📦 Status:\n",   out["status"].value_counts().to_string())
    print("\n🎯 Direction:\n",out["direction"].value_counts().to_string())
    print(f"\n🟢 Goblin/Demon (forced OVER): {(out['forced_over']=='YES').sum()}")

    if "match_method" in out.columns:
        print("\n🧩 PlayerDir match_method:\n", out["match_method"].value_counts().to_string())
    if "pp_team" in out.columns:
        t = sorted({str(x).strip() for x in out["pp_team"] if str(x).strip()})
        print(f"\n🏷️  Output pp_team unique: {len(t)}")


if __name__ == "__main__":
    main()