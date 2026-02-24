#!/usr/bin/env python3
r"""
step4_attach_player_stats_boxscore_cache.py

Per-game boxscore caching with a HARD fallback when NBA endpoints are blocked (403/429):

Primary path (fast when it works):
- Pull GAME_IDs from NBA CDN scoreboard JSON for one or more dates
- Pull player stat lines from boxscoretraditionalv3 for each game
- Append to a persistent CSV cache
- Compute stat_g1..stat_gN + last5/last10/season avg for each prop row (combo-safe)

Solid workaround:
- If CDN scoreboard OR boxscore calls hit HTTP 403/429 (blocked/rate-limited),
  immediately FALL BACK to per-player PlayerGameLog (no scoreboard needed).
- PlayerGameLog rows are normalized into the SAME cache schema so downstream logic
  is unchanged.

Typical usage:
  py -3.14 .\step4_attach_player_stats_boxscore_cache.py --slate step3_with_defense.csv --out step4_with_stats.csv --season 2025-26 --date 2026-02-23 --cache nba_boxscore_cache.csv --n 10 --days 3 --sleep 3.5 --retries 10 --provider auto

Notes:
- 404 from CDN scoreboard is treated as "no games" (returns empty list) — not an error.
- 403/429 triggers immediate fallback to PlayerGameLog.
- Windows-safe: avoids emoji in prints + forces UTF-8 if available.
"""

from __future__ import annotations

# ── Windows stdout UTF-8 safety (must be before any emoji/Unicode prints) ──────
import sys as _sys
try:
    _sys.stdout.reconfigure(encoding="utf-8")
    _sys.stderr.reconfigure(encoding="utf-8")
except Exception:
    pass

# ── HARD IPv4-first (must be before nba_api imports) ───────────────────────────
import socket as _socket

if "--no-force-ipv4" not in _sys.argv:
    _orig_getaddrinfo = _socket.getaddrinfo

    def _ipv4_first_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        try:
            return _orig_getaddrinfo(host, port, _socket.AF_INET, type, proto, flags)
        except Exception:
            return _orig_getaddrinfo(host, port, family, type, proto, flags)

    _socket.getaddrinfo = _ipv4_first_getaddrinfo
    print("[IPv4] HARD IPv4-first enabled")

import argparse
import random
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

from nba_api.stats.endpoints import boxscoretraditionalv3, playergamelog

COMBO_SEP = "|"

NBA_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    ),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Origin": "https://www.nba.com",
    "Referer": "https://www.nba.com/",
    "Connection": "keep-alive",
}


# ── exceptions ────────────────────────────────────────────────────────────────
class EndpointBlocked(RuntimeError):
    """Raised when an endpoint is blocked or rate-limited (HTTP 403/429)."""


# ── helpers ───────────────────────────────────────────────────────────────────
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


def _sleep(base: float, jitter: float = 0.8) -> None:
    time.sleep(max(0.0, base + random.uniform(0, jitter)))


def _to_float_series(df: pd.DataFrame, col: str) -> pd.Series:
    return pd.to_numeric(df.get(col, pd.Series([np.nan] * len(df))), errors="coerce")


def derive_stat_series(df: pd.DataFrame, prop_norm: str) -> pd.Series:
    p = (prop_norm or "").lower().strip()

    pts = _to_float_series(df, "PTS")
    reb = _to_float_series(df, "REB")
    ast = _to_float_series(df, "AST")
    stl = _to_float_series(df, "STL")
    blk = _to_float_series(df, "BLK")
    tov = _to_float_series(df, "TO")

    fga = _to_float_series(df, "FGA")
    fgm = _to_float_series(df, "FGM")
    fg3a = _to_float_series(df, "FG3A")
    fg3m = _to_float_series(df, "FG3M")
    fta = _to_float_series(df, "FTA")
    ftm = _to_float_series(df, "FTM")

    fg2a = fga - fg3a
    fg2m = fgm - fg3m

    if p in ("pts", "points"):
        return pts
    if p in ("reb", "rebounds"):
        return reb
    if p in ("ast", "assists"):
        return ast
    if p in ("pra",):
        return pts + reb + ast
    if p in ("pr",):
        return pts + reb
    if p in ("pa",):
        return pts + ast
    if p in ("ra",):
        return reb + ast
    if p in ("stocks",):
        return stl + blk
    if p in ("stl", "steals"):
        return stl
    if p in ("blk", "blocks"):
        return blk
    if p in ("tov", "turnovers", "to"):
        return tov
    if p in ("fga",):
        return fga
    if p in ("fgm",):
        return fgm
    if p in ("fg3a", "3pa"):
        return fg3a
    if p in ("fg3m", "3pm"):
        return fg3m
    if p in ("fg2a", "2pa"):
        return fg2a
    if p in ("fg2m", "2pm"):
        return fg2m
    if p in ("fta",):
        return fta
    if p in ("ftm",):
        return ftm
    if p in ("fantasy", "fantasy_score"):
        return pts + 1.2 * reb + 1.5 * ast + 3.0 * stl + 3.0 * blk - tov

    return pd.Series([np.nan] * len(df), index=df.index)


def calc_last5_hit(vals_mr: List[float], line: float) -> Tuple[int, int, int, float]:
    over = under = push = 0
    used = 0
    for v in vals_mr[:5]:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        used += 1
        if v > line:
            over += 1
        elif v < line:
            under += 1
        else:
            push += 1
    hit = (over / used) if used else np.nan
    return over, under, push, hit


def _looks_like_10054(e: Exception) -> bool:
    s = str(e).lower()
    return ("10054" in s) or ("forcibly" in s) or ("connection reset" in s) or ("connection aborted" in s)


def _http_status_from_exc(e: Exception) -> Optional[int]:
    code = getattr(e, "code", None)
    if isinstance(code, int):
        return code
    resp = getattr(e, "response", None)
    if resp is not None:
        sc = getattr(resp, "status_code", None)
        if isinstance(sc, int):
            return sc
    return None


def nba_call_with_retries(fn, label: str, retries: int, base_sleep: float):
    """
    Retry wrapper for both CDN requests (requests.get) and nba_api calls.

    Critical behavior:
    - If HTTP 403/429 occurs: raise EndpointBlocked immediately (do NOT waste retries).
    """
    last_err: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            _sleep(base_sleep, jitter=1.2)
            return fn()

        except requests.exceptions.HTTPError as e:
            last_err = e
            code = _http_status_from_exc(e)
            if code in (403, 429):
                raise EndpointBlocked(f"{label} blocked: HTTP {code}") from e

            backoff = min(150.0, (2 ** (attempt - 1)) * 8.0) + random.uniform(2.0, 12.0)
            print(f"  [WARN] {label} HTTPError({code}) attempt {attempt}/{retries} — cooldown {backoff:.1f}s")
            time.sleep(backoff)

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            last_err = e
            backoff = min(180.0, (2 ** (attempt - 1)) * 10.0) + random.uniform(2.0, 12.0)
            print(f"  [TIMEOUT] {label} attempt {attempt}/{retries} — cooldown {backoff:.1f}s")
            time.sleep(backoff)

        except Exception as e:
            last_err = e
            code = _http_status_from_exc(e)
            if code in (403, 429):
                raise EndpointBlocked(f"{label} blocked: HTTP {code}") from e

            if _looks_like_10054(e):
                backoff = min(240.0, (2 ** (attempt - 1)) * 14.0) + random.uniform(5.0, 18.0)
                print(f"  [RESET] {label} (10054) attempt {attempt}/{retries} — cooldown {backoff:.1f}s")
                time.sleep(backoff)
            else:
                backoff = min(150.0, (2 ** (attempt - 1)) * 8.0) + random.uniform(2.0, 12.0)
                print(f"  [WARN] {label} error attempt {attempt}/{retries}: {type(e).__name__} — cooldown {backoff:.1f}s")
                time.sleep(backoff)

    raise RuntimeError(f"{label} failed after {retries} retries. Last error: {last_err}")


# ── Primary path: CDN scoreboard + boxscoretraditionalv3 ───────────────────────
def fetch_game_ids_for_date_cdn(date_str: str, timeout: Tuple[float, float], retries: int, sleep_s: float) -> List[str]:
    """
    Uses NBA CDN scoreboard JSON to fetch daily game IDs.
    Behavior:
    - 404 => treated as "no games", returns []
    - 403/429 => raises EndpointBlocked (handled by caller for fallback)
    """
    yyyymmdd = date_str.replace("-", "")
    url = f"https://cdn.nba.com/static/json/liveData/scoreboard/todaysScoreboard_{yyyymmdd}.json"

    def _call() -> List[str]:
        r = requests.get(url, headers=NBA_HEADERS, timeout=timeout)
        if r.status_code == 404:
            return []
        r.raise_for_status()
        data = r.json()
        games = (((data or {}).get("scoreboard") or {}).get("games")) or []
        out: List[str] = []
        for g in games:
            gid = str(g.get("gameId") or "").strip()
            if gid:
                out.append(gid)
        return sorted(list(dict.fromkeys(out)))

    return nba_call_with_retries(_call, f"cdn_scoreboard {date_str}", retries=retries, base_sleep=sleep_s)


def fetch_boxscore_players(game_id: str, timeout: Tuple[float, float], retries: int, sleep_s: float) -> pd.DataFrame:
    def _call() -> pd.DataFrame:
        bs = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id, timeout=timeout, headers=NBA_HEADERS)
        dfs = bs.get_data_frames()

        # Prefer a df that looks like player stats
        for d in dfs:
            cols_lower = [c.lower() for c in d.columns]
            if "playerid" in cols_lower and ("pts" in cols_lower or "points" in cols_lower):
                return d.copy()

        cand = [d for d in dfs if "playerId" in d.columns]
        if cand:
            return max(cand, key=len).copy()
        return pd.DataFrame()

    return nba_call_with_retries(_call, f"boxscore game {game_id}", retries=retries, base_sleep=sleep_s)


def normalize_boxscore_df(df: pd.DataFrame, game_id: str, game_date: str, season: str) -> pd.DataFrame:
    if df is None or len(df) == 0:
        return pd.DataFrame()

    lower = {c.lower(): c for c in df.columns}

    def pick(*names: str) -> Optional[str]:
        for n in names:
            if n.lower() in lower:
                return lower[n.lower()]
        return None

    pid = pick("playerId")
    if not pid:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["SEASON"] = season
    out["GAME_ID"] = str(game_id)
    out["GAME_DATE"] = game_date
    out["PLAYER_ID"] = df[pid].astype(str)

    out["MIN"] = df[pick("minutes", "min")] if pick("minutes", "min") else ""
    out["PTS"] = df[pick("points", "pts")] if pick("points", "pts") else np.nan
    out["REB"] = df[pick("reboundsTotal", "reb")] if pick("reboundsTotal", "reb") else np.nan
    out["AST"] = df[pick("assists", "ast")] if pick("assists", "ast") else np.nan
    out["STL"] = df[pick("steals", "stl")] if pick("steals", "stl") else np.nan
    out["BLK"] = df[pick("blocks", "blk")] if pick("blocks", "blk") else np.nan
    out["TO"] = df[pick("turnovers", "to", "tov")] if pick("turnovers", "to", "tov") else np.nan

    out["FGA"] = df[pick("fieldGoalsAttempted", "fga")] if pick("fieldGoalsAttempted", "fga") else np.nan
    out["FGM"] = df[pick("fieldGoalsMade", "fgm")] if pick("fieldGoalsMade", "fgm") else np.nan
    out["FG3A"] = df[pick("threePointersAttempted", "fg3a", "fg3Attempted")] if pick("threePointersAttempted", "fg3a", "fg3Attempted") else np.nan
    out["FG3M"] = df[pick("threePointersMade", "fg3m", "fg3Made")] if pick("threePointersMade", "fg3m", "fg3Made") else np.nan
    out["FTA"] = df[pick("freeThrowsAttempted", "fta")] if pick("freeThrowsAttempted", "fta") else np.nan
    out["FTM"] = df[pick("freeThrowsMade", "ftm")] if pick("freeThrowsMade", "ftm") else np.nan

    for c in ["PTS", "REB", "AST", "STL", "BLK", "TO", "FGA", "FGM", "FG3A", "FG3M", "FTA", "FTM"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    mins = pd.to_numeric(out["MIN"], errors="coerce")
    out = out.loc[mins.fillna(0) > 0].copy()

    return out


# ── Fallback path: PlayerGameLog (no scoreboard) ──────────────────────────────
def fetch_player_gamelog_df(pid: str, season: str, timeout: Tuple[float, float], retries: int, sleep_s: float) -> pd.DataFrame:
    def _call() -> pd.DataFrame:
        gl = playergamelog.PlayerGameLog(player_id=pid, season=season, timeout=timeout, headers=NBA_HEADERS)
        dfs = gl.get_data_frames()
        return (dfs[0].copy() if dfs else pd.DataFrame())

    return nba_call_with_retries(_call, f"PlayerGameLog {pid}", retries=retries, base_sleep=sleep_s)


def normalize_gamelog_to_cache(df: pd.DataFrame, pid: str, season: str) -> pd.DataFrame:
    if df is None or df.empty:
        return pd.DataFrame()

    cols = {c.upper(): c for c in df.columns}

    def pick(u: str) -> Optional[str]:
        return cols.get(u.upper())

    game_id = pick("GAME_ID")
    game_date = pick("GAME_DATE")
    if not game_id or not game_date:
        return pd.DataFrame()

    out = pd.DataFrame()
    out["SEASON"] = season
    out["GAME_ID"] = df[game_id].astype(str)

    gd = pd.to_datetime(df[game_date], errors="coerce")
    out["GAME_DATE"] = gd.dt.strftime("%Y-%m-%d").fillna("")
    out["PLAYER_ID"] = str(pid)

    out["MIN"] = df[pick("MIN")] if pick("MIN") else ""
    out["PTS"] = df[pick("PTS")] if pick("PTS") else np.nan
    out["REB"] = df[pick("REB")] if pick("REB") else np.nan
    out["AST"] = df[pick("AST")] if pick("AST") else np.nan
    out["STL"] = df[pick("STL")] if pick("STL") else np.nan
    out["BLK"] = df[pick("BLK")] if pick("BLK") else np.nan

    out["TO"] = df[pick("TOV")] if pick("TOV") else (df[pick("TO")] if pick("TO") else np.nan)

    out["FGM"] = df[pick("FGM")] if pick("FGM") else np.nan
    out["FGA"] = df[pick("FGA")] if pick("FGA") else np.nan
    out["FG3M"] = df[pick("FG3M")] if pick("FG3M") else np.nan
    out["FG3A"] = df[pick("FG3A")] if pick("FG3A") else np.nan
    out["FTM"] = df[pick("FTM")] if pick("FTM") else np.nan
    out["FTA"] = df[pick("FTA")] if pick("FTA") else np.nan

    for c in ["PTS", "REB", "AST", "STL", "BLK", "TO", "FGA", "FGM", "FG3A", "FG3M", "FTA", "FTM"]:
        out[c] = pd.to_numeric(out[c], errors="coerce")

    mins = pd.to_numeric(out["MIN"], errors="coerce")
    out = out.loc[mins.fillna(0) > 0].copy()

    out = out.drop_duplicates(subset=["SEASON", "GAME_ID", "PLAYER_ID"], keep="last")
    return out


def update_cache_via_playergamelog(
    cache: pd.DataFrame,
    slate: pd.DataFrame,
    season: str,
    timeout: Tuple[float, float],
    retries: int,
    sleep_s: float,
) -> Tuple[pd.DataFrame, int]:
    ids: List[str] = []
    seen = set()
    for raw in slate["nba_player_id"].astype(str).fillna(""):
        for pid_int in _parse_ids(raw):
            s = str(pid_int)
            if s not in seen:
                seen.add(s)
                ids.append(s)

    if not ids:
        return cache, 0

    print(f"→ Fallback PlayerGameLog mode: players={len(ids)}")

    if len(cache):
        key_series = (
            cache["SEASON"].astype(str)
            + "||"
            + cache["GAME_ID"].astype(str)
            + "||"
            + cache["PLAYER_ID"].astype(str)
        )
        existing = set(key_series.tolist())
    else:
        existing = set()

    new_rows: List[pd.DataFrame] = []
    added = 0

    for pid in ids:
        try:
            gl = fetch_player_gamelog_df(pid, season=season, timeout=timeout, retries=retries, sleep_s=max(sleep_s, 1.0))
            norm = normalize_gamelog_to_cache(gl, pid=pid, season=season)
            if norm.empty:
                continue

            k = norm["SEASON"].astype(str) + "||" + norm["GAME_ID"].astype(str) + "||" + norm["PLAYER_ID"].astype(str)
            mask_new = ~k.isin(existing)
            norm2 = norm.loc[mask_new].copy()
            if len(norm2):
                new_rows.append(norm2)
                for kk in k.loc[mask_new].tolist():
                    existing.add(kk)
                added += len(norm2)

        except EndpointBlocked as e:
            print(f"  [BLOCKED] PlayerGameLog blocked for {pid}: {e}")
            continue
        except Exception as e:
            print(f"  [WARN] PlayerGameLog failed for {pid}: {type(e).__name__}: {e}")
            continue

        _sleep(max(0.8, sleep_s), jitter=1.0)

    if new_rows:
        cache2 = pd.concat([cache] + new_rows, ignore_index=True)
        cache2 = cache2.drop_duplicates(subset=["SEASON", "GAME_ID", "PLAYER_ID"], keep="last")
        return cache2, added

    return cache, 0


# ── output formatting ─────────────────────────────────────────────────────────
def fmt_num(x: float) -> str:
    if x is None or (isinstance(x, float) and np.isnan(x)):
        return ""
    return f"{float(x):.3f}".rstrip("0").rstrip(".")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--slate", default="step3_with_defense.csv", help="Input slate with nba_player_id + prop_norm + line")
    ap.add_argument("--out", default="step4_with_stats.csv")
    ap.add_argument("--season", default="2025-26")
    ap.add_argument("--date", required=True, help="Slate date YYYY-MM-DD (used to fetch game IDs)")
    ap.add_argument("--days", type=int, default=35, help="How many days back to update cache (rolling)")
    ap.add_argument("--cache", default="nba_boxscore_cache.csv")
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--connect-timeout", type=float, default=12.0)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--sleep", type=float, default=3.5, help="Base sleep between NBA calls")
    ap.add_argument("--retries", type=int, default=10, help="Retries per CDN scoreboard and per boxscore call")
    ap.add_argument(
        "--provider",
        default="auto",
        choices=["auto", "cdn", "playergamelog"],
        help="auto: try CDN+boxscore, fallback to PlayerGameLog on 403/429; cdn: force CDN+boxscore only; playergamelog: force fallback mode only",
    )
    args = ap.parse_args()

    timeout = (args.connect_timeout, args.timeout)
    cache_path = Path(args.cache)

    # Load slate
    print(f"→ Loading slate: {args.slate}")
    slate = pd.read_csv(args.slate, dtype=str).fillna("")
    if "nba_player_id" not in slate.columns:
        raise RuntimeError("❌ slate missing nba_player_id")
    if "prop_norm" not in slate.columns:
        if "prop_type" in slate.columns:
            slate["prop_norm"] = slate["prop_type"].astype(str).str.lower()
        else:
            raise RuntimeError("❌ slate missing prop_norm (and prop_type)")
    slate["_line_num"] = pd.to_numeric(slate.get("line", ""), errors="coerce")

    # Determine dates to update cache
    end_dt = datetime.strptime(args.date, "%Y-%m-%d")
    start_dt = end_dt - timedelta(days=int(args.days))
    date_list = [(start_dt + timedelta(days=i)).strftime("%Y-%m-%d")
                 for i in range((end_dt - start_dt).days + 1)]

    # Load existing cache
    if cache_path.exists():
        cache = pd.read_csv(cache_path, dtype=str).fillna("")
        print(f"💾 Loaded cache: {args.cache} | rows={len(cache)}")
    else:
        cache = pd.DataFrame(columns=[
            "SEASON", "GAME_ID", "GAME_DATE", "PLAYER_ID", "MIN",
            "PTS", "REB", "AST", "STL", "BLK", "TO",
            "FGA", "FGM", "FG3A", "FG3M", "FTA", "FTM"
        ])
        print(f"🆕 Cache not found, creating: {args.cache}")

    # Ensure required cache columns exist
    required_cache_cols = [
        "SEASON", "GAME_ID", "GAME_DATE", "PLAYER_ID", "MIN",
        "PTS", "REB", "AST", "STL", "BLK", "TO",
        "FGA", "FGM", "FG3A", "FG3M", "FTA", "FTM"
    ]
    for c in required_cache_cols:
        if c not in cache.columns:
            cache[c] = ""

    cached_games = set(zip(cache.get("SEASON", pd.Series([], dtype=str)).astype(str),
                           cache.get("GAME_ID", pd.Series([], dtype=str)).astype(str)))

    def _write_cache(df: pd.DataFrame) -> None:
        df.to_csv(cache_path, index=False, encoding="utf-8")

    provider_used = None

    # ── Update cache ──────────────────────────────────────────────────────────
    if args.provider in ("auto", "cdn"):
        print(f"\n→ Updating cache via CDN+boxscore for rolling {args.days} days (dates={len(date_list)})…")
        new_rows: List[pd.DataFrame] = []
        new_games = 0

        try:
            for d in date_list:
                game_ids = fetch_game_ids_for_date_cdn(
                    d,
                    timeout=timeout,
                    retries=int(args.retries),
                    sleep_s=max(float(args.sleep), 2.0),
                )

                if not game_ids:
                    continue

                for gid in game_ids:
                    gid = str(gid)
                    if (args.season, gid) in cached_games:
                        continue

                    raw = fetch_boxscore_players(
                        gid,
                        timeout=timeout,
                        retries=int(args.retries),
                        sleep_s=max(float(args.sleep), 2.0),
                    )
                    norm = normalize_boxscore_df(raw, game_id=gid, game_date=d, season=args.season)
                    if len(norm):
                        new_rows.append(norm)
                        cached_games.add((args.season, gid))
                        new_games += 1
                        print(f"  ✅ cached game {gid} ({d}) players={len(norm)}")

            if new_rows:
                cache2 = pd.concat([cache] + new_rows, ignore_index=True)
                cache2 = cache2.drop_duplicates(subset=["SEASON", "GAME_ID", "PLAYER_ID"], keep="last")
                _write_cache(cache2)
                cache = cache2
                print(f"\n✅ Cache updated: {args.cache} | rows={len(cache)} | new_games={new_games}")
            else:
                print("\n✅ Cache already up to date (no new games added).")

            provider_used = "cdn"

        except EndpointBlocked as e:
            if args.provider == "cdn":
                print(f"\n🚫 CDN/boxscore blocked and provider=cdn forced. Error: {e}")
                raise
            print(f"\n🚧 CDN/boxscore blocked ({e}) → FALLBACK to PlayerGameLog mode")
            provider_used = "playergamelog_fallback"

    if args.provider == "playergamelog" or provider_used == "playergamelog_fallback":
        cache2, added = update_cache_via_playergamelog(
            cache=cache,
            slate=slate,
            season=args.season,
            timeout=timeout,
            retries=int(args.retries),
            sleep_s=max(float(args.sleep), 1.0),
        )
        if added > 0:
            _write_cache(cache2)
            cache = cache2
            print(f"\n✅ Cache updated via PlayerGameLog: +{added} rows | total_rows={len(cache)}")
        else:
            print("\n✅ PlayerGameLog fallback: no new cache rows added.")

        if provider_used is None:
            provider_used = "playergamelog_forced"

    print(f"\n→ Provider used: {provider_used}")

    # ── Prep cache for stat lookups ───────────────────────────────────────────
    cache["GAME_DATE"] = pd.to_datetime(cache["GAME_DATE"], errors="coerce")
    cache = cache.loc[cache["SEASON"].astype(str) == args.season].copy()

    # Output columns
    N = int(args.n)
    stat_cols = [f"stat_g{i}" for i in range(1, N + 1)]
    out_cols = stat_cols + [
        "stat_last5_avg", "stat_last10_avg", "stat_season_avg",
        "last5_over", "last5_under", "last5_push", "last5_hit_rate",
        "stat_status"
    ]
    for c in out_cols:
        if c not in slate.columns:
            slate[c] = ""

    # ── Build per-row stats ───────────────────────────────────────────────────
    print(f"\n→ Attaching stats from cache → rows={len(slate)}")
    for idx, row in slate.iterrows():
        ids = _parse_ids(row.get("nba_player_id", ""))
        prop = str(row.get("prop_norm", "")).lower().strip()

        line = row.get("_line_num", np.nan)
        try:
            line = float(line)
        except Exception:
            line = np.nan

        if not ids:
            slate.at[idx, "stat_status"] = "NO_NBA_ID"
            continue

        # Single player
        if len(ids) == 1:
            pid = str(ids[0])
            dfp = cache.loc[cache["PLAYER_ID"].astype(str) == pid].copy()
            if dfp.empty:
                slate.at[idx, "stat_status"] = "NO_CACHE_PLAYER"
                continue
            dfp = dfp.sort_values("GAME_DATE", ascending=False)
            dfp["STAT"] = derive_stat_series(dfp, prop)
            vals = pd.to_numeric(dfp["STAT"], errors="coerce").dropna().astype(float).tolist()

        # Combo: align on GAME_ID intersection then sum STAT
        else:
            dfs = []
            for pid_int in ids:
                pid = str(pid_int)
                dfp = cache.loc[cache["PLAYER_ID"].astype(str) == pid].copy()
                if dfp.empty:
                    dfs = []
                    break
                dfp = dfp.sort_values("GAME_DATE", ascending=False)
                dfp["STAT"] = derive_stat_series(dfp, prop)
                dfs.append(dfp[["GAME_ID", "GAME_DATE", "STAT"]].copy())

            if not dfs:
                slate.at[idx, "stat_status"] = "NO_CACHE_PLAYER"
                continue

            merged = dfs[0][["GAME_ID", "GAME_DATE", "STAT"]].rename(columns={"STAT": "S1"})
            for i, dfi in enumerate(dfs[1:], start=2):
                merged = merged.merge(
                    dfi[["GAME_ID", "STAT"]].rename(columns={"STAT": f"S{i}"}),
                    on="GAME_ID",
                    how="inner",
                )
            s_cols = [c for c in merged.columns if c.startswith("S")]
            merged["STAT_SUM"] = merged[s_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)
            merged = merged.sort_values("GAME_DATE", ascending=False)
            vals = pd.to_numeric(merged["STAT_SUM"], errors="coerce").dropna().astype(float).tolist()

        if not vals:
            slate.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
            continue

        # fill g1..gN (most recent first)
        for i in range(1, N + 1):
            v = vals[i - 1] if i - 1 < len(vals) else np.nan
            slate.at[idx, f"stat_g{i}"] = fmt_num(v)

        def avg_k(k: int) -> float:
            s = vals[:k] if len(vals) >= k else vals
            return float(np.mean(s)) if s else np.nan

        slate.at[idx, "stat_last5_avg"] = fmt_num(avg_k(5))
        slate.at[idx, "stat_last10_avg"] = fmt_num(avg_k(10))
        slate.at[idx, "stat_season_avg"] = fmt_num(float(np.mean(vals)))

        if not np.isnan(line):
            over, under, push, hit = calc_last5_hit(vals, line)
            slate.at[idx, "last5_over"] = str(over)
            slate.at[idx, "last5_under"] = str(under)
            slate.at[idx, "last5_push"] = str(push)
            slate.at[idx, "last5_hit_rate"] = fmt_num(hit)

        slate.at[idx, "stat_status"] = "OK"

    slate = slate.drop(columns=["_line_num"], errors="ignore")
    slate.to_csv(args.out, index=False, encoding="utf-8")
    print(f"\n✅ Saved → {args.out}")
    print("\nstat_status breakdown:")
    print(slate["stat_status"].astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()
