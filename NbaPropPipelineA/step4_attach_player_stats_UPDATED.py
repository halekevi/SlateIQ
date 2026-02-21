#!/usr/bin/env python3
"""
step4_attach_player_stats.py  (NBA) — deterministic, id-driven, combo-safe
PATCHED v2: pandas string-dtype safe (writes numbers as strings)

Why you hit the error:
- We load Step3 with dtype=str, so new columns become pandas "string" dtype.
- Assigning floats into a string-dtype column raises: "Invalid value '8.0' for dtype 'str'".

Fix:
- Always write computed numeric outputs as STRINGS (or "" for missing).
- Keeps downstream CSV deterministic and avoids dtype churn.

Recommended run:
  py -3.14 step4_attach_player_stats.py --input step3_with_defense.csv --output step4_with_stats.csv --season 2025-26 --cache-dir .\\_nba_cache --timeout 120 --retries 6 --sleep 0.8
"""

from __future__ import annotations

import argparse
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from nba_api.stats.endpoints import playergamelog

COMBO_SEP = "|"
# -----------------------------
# NBA Stats (requests) helpers
# -----------------------------
_NBA_SESSION = None

def _nba_headers() -> dict:
    # These headers make the request look like a real browser. stats.nba.com is picky.
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nba.com/",
        "Origin": "https://www.nba.com",
        "Connection": "keep-alive",
        "DNT": "1",
        "Pragma": "no-cache",
        "Cache-Control": "no-cache",
        "x-nba-stats-origin": "stats",
        "x-nba-stats-token": "true",
    }

def _get_nba_session() -> requests.Session:
    global _NBA_SESSION
    if _NBA_SESSION is not None:
        return _NBA_SESSION

    s = requests.Session()
    # Retry on transient upstream issues; do NOT blindly retry on everything.
    retry = Retry(
        total=0,  # we handle retries ourselves so we can backoff consistently
        connect=0,
        read=0,
        status=0,
        backoff_factor=0,
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    _NBA_SESSION = s
    return s

def _fetch_playergamelog_requests(player_id: str, season: str, timeout_s: float) -> pd.DataFrame:
    """Fetch player gamelog via direct requests (more controllable than nba_api)."""
    url = "https://stats.nba.com/stats/playergamelog"
    params = {
        "PlayerID": str(player_id),
        "Season": str(season),
        "SeasonType": "Regular Season",
        "LeagueID": "00",
    }
    s = _get_nba_session()
    r = s.get(url, params=params, headers=_nba_headers(), timeout=timeout_s)
    # Common failure modes:
    #  - 429: throttled
    #  - 403: blocked/fingerprinted
    #  - 5xx: upstream flake
    if r.status_code in (429, 403, 500, 502, 503, 504):
        raise requests.HTTPError(f"HTTP {r.status_code}", response=r)
    r.raise_for_status()

    payload = r.json()
    rs = payload.get("resultSets") or payload.get("resultSet")
    if isinstance(rs, dict):
        headers = rs.get("headers", [])
        rows = rs.get("rowSet", [])
    else:
        headers = (rs[0] or {}).get("headers", []) if rs else []
        rows = (rs[0] or {}).get("rowSet", []) if rs else []

    df = pd.DataFrame(rows, columns=headers) if headers else pd.DataFrame(rows)
    return df



def _to_str_num(x) -> str:
    """Convert numeric to a compact string; return '' if missing."""
    try:
        if x is None:
            return ""
        if isinstance(x, str):
            s = x.strip()
            return s
        xf = float(x)
        if np.isnan(xf):
            return ""
        # keep one decimal if needed, but avoid trailing .0 when integer-like
        if abs(xf - round(xf)) < 1e-9:
            return str(int(round(xf)))
        return f"{xf:.3f}".rstrip("0").rstrip(".")
    except Exception:
        return ""


def _compute_stat_series(df: pd.DataFrame, prop_norm: str) -> pd.Series:
    p = (prop_norm or "").lower().strip()

    def num(col: str) -> pd.Series:
        if col not in df.columns:
            return pd.Series([np.nan] * len(df), index=df.index)
        return pd.to_numeric(df[col], errors="coerce")

    pts = num("PTS")
    reb = num("REB")
    ast = num("AST")
    stl = num("STL")
    blk = num("BLK")
    tov = num("TOV")
    fga = num("FGA")
    fgm = num("FGM")
    fg3a = num("FG3A")
    fg3m = num("FG3M")
    fta = num("FTA")
    ftm = num("FTM")

    pf = num("PF")

    fg2a = fga - fg3a
    fg2m = fgm - fg3m

    if p in ("pts", "points"):
        return pts
    if p in ("reb", "rebounds"):
        return reb
    if p in ("ast", "assists"):
        return ast
    if p == "pra":
        return pts + reb + ast
    if p == "pr":
        return pts + reb
    if p == "pa":
        return pts + ast
    if p == "ra":
        return reb + ast
    if p == "stocks":
        return stl + blk
    if p in ("stl", "steals"):
        return stl
    if p in ("blk", "blocks"):
        return blk
    if p in ("tov", "turnovers"):
        return tov
    if p in ("pf", "personalfouls", "personal_fouls", "fouls"):
        return pf
    if p in ("fantasy", "fantasyscore"):
        return pts + 1.2 * reb + 1.5 * ast + 3.0 * stl + 3.0 * blk - tov

    if p == "fga":
        return fga
    if p == "fgm":
        return fgm
    if p == "fg3a":
        return fg3a
    if p == "fg3m":
        return fg3m
    if p == "fg2a":
        return fg2a
    if p == "fg2m":
        return fg2m
    if p == "fta":
        return fta
    if p == "ftm":
        return ftm

    return pd.Series([np.nan] * len(df), index=df.index)


def _clean_nba_id(s: str) -> str:
    x = str(s).strip()
    x = re.sub(r"\.0$", "", x)
    x = x.replace(",", "")
    return x


def _parse_combo_ids(nba_player_id: str) -> List[int]:
    s = _clean_nba_id(nba_player_id)
    if not s:
        return []
    if COMBO_SEP in s:
        parts = [p.strip() for p in s.split(COMBO_SEP) if p.strip()]
        ids: List[int] = []
        for p in parts:
            try:
                ids.append(int(_clean_nba_id(p)))
            except Exception:
                pass
        return sorted(list(dict.fromkeys(ids)))
    try:
        return [int(s)]
    except Exception:
        return []


def _read_player_log(
    player_id: int,
    season: str,
    cache_dir: Optional[Path],
    mem_cache: Dict[Tuple[int, str], pd.DataFrame],
    sleep_s: float,
    timeout_s: float,
    retries: int,
) -> pd.DataFrame:
    key = (player_id, season)
    if key in mem_cache:
        return mem_cache[key]

    fp = None
    if cache_dir is not None:
        cache_dir.mkdir(parents=True, exist_ok=True)
        fp = cache_dir / f"playergamelog_{season}_{player_id}.csv"
        if fp.exists():
            try:
                df = pd.read_csv(fp, dtype=str).fillna("")
                mem_cache[key] = df
                return df
            except Exception:
                pass

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if sleep_s > 0:
                time.sleep(sleep_s)

            raw = _fetch_playergamelog_requests(player_id, season, timeout_s)

            # Empty response = invalid/inactive player — skip immediately, don't retry
            if raw is None or len(raw) == 0:
                print(f"⚠️ player_id={player_id}: empty response (invalid/inactive player) — skipping")
                empty = pd.DataFrame()
                mem_cache[key] = empty
                return empty

            df = raw.copy()
            for c in df.columns:
                df[c] = df[c].astype(str)

            if "MIN" in df.columns:
                mins = pd.to_numeric(df["MIN"], errors="coerce")
                df = df.loc[mins.fillna(0) > 0].copy()

            if "GAME_DATE" in df.columns:
                df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
                df = df.sort_values("GAME_DATE", ascending=True)

            if fp is not None:
                try:
                    df.to_csv(fp, index=False, encoding="utf-8")
                except Exception:
                    pass

            mem_cache[key] = df
            return df

        except (requests.exceptions.ReadTimeout, requests.exceptions.ConnectTimeout) as e:
            last_err = e
            # Use longer backoff to let the API recover from rate-limiting — never short-circuit
            backoff = min(90.0, (2 ** (attempt - 1)) * 3.0) + random.uniform(1.0, 5.0)
            print(f"⚠️ playergamelog error for player_id={player_id} attempt {attempt}/{retries}: {type(e).__name__} — retrying in {backoff:.1f}s")
            time.sleep(backoff)

        except (requests.exceptions.RequestException, Exception) as e:
            last_err = e
            backoff = min(60.0, (2 ** (attempt - 1)) * 1.5) + random.random()
            print(f"⚠️ playergamelog error for player_id={player_id} attempt {attempt}/{retries}: {type(e).__name__} — retrying in {backoff:.1f}s")
            time.sleep(backoff)

    # All retries exhausted — skip instead of crashing the pipeline
    print(f"⚠️ Skipping player_id={player_id} after {retries} retries. Last error: {last_err}")
    empty = pd.DataFrame()
    mem_cache[key] = empty
    return empty


def _combo_aligned_sum(logs: List[pd.DataFrame], prop_norm: str) -> pd.DataFrame:
    if not logs:
        return pd.DataFrame(columns=["GAME_ID", "GAME_DATE", "STAT"])

    prepared = []
    for df in logs:
        if df is None or len(df) == 0 or "GAME_ID" not in df.columns:
            continue
        tmp = df.copy()
        tmp["STAT"] = _compute_stat_series(tmp, prop_norm)
        cols = ["GAME_ID", "STAT"]
        if "GAME_DATE" in tmp.columns:
            cols.append("GAME_DATE")
        prepared.append(tmp[cols].copy())

    if not prepared:
        return pd.DataFrame(columns=["GAME_ID", "GAME_DATE", "STAT"])

    merged = prepared[0][["GAME_ID", "STAT"]].rename(columns={"STAT": "STAT_1"})
    for i, p in enumerate(prepared[1:], start=2):
        merged = merged.merge(p[["GAME_ID", "STAT"]].rename(columns={"STAT": f"STAT_{i}"}), on="GAME_ID", how="inner")

    stat_cols = [c for c in merged.columns if c.startswith("STAT_")]
    merged["STAT"] = merged[stat_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)

    if "GAME_DATE" in prepared[0].columns:
        dates = prepared[0][["GAME_ID", "GAME_DATE"]].copy()
        merged = merged.merge(dates, on="GAME_ID", how="left")
        merged["GAME_DATE"] = pd.to_datetime(merged["GAME_DATE"], errors="coerce")
        merged = merged.sort_values("GAME_DATE", ascending=True)

    keep = ["GAME_ID"] + (["GAME_DATE"] if "GAME_DATE" in merged.columns else []) + ["STAT"]
    return merged[keep].copy()


def _calc_last5_hit(stat_g: List[float], line_val: float) -> Tuple[int, int, int, float]:
    over = under = push = 0
    vals = []
    for v in stat_g[:5]:
        if v is None or (isinstance(v, float) and np.isnan(v)):
            continue
        vals.append(float(v))
        if v > line_val:
            over += 1
        elif v < line_val:
            under += 1
        else:
            push += 1
    denom = len(vals)
    hit_rate = (over / denom) if denom > 0 else np.nan
    return over, under, push, hit_rate


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="step3_with_defense.csv")
    ap.add_argument("--output", default="step4_with_stats.csv")
    ap.add_argument("--season", default="2025-26")
    ap.add_argument("--n", type=int, default=10)
    ap.add_argument("--cache-dir", default="")
    ap.add_argument("--sleep", type=float, default=0.8)
    ap.add_argument("--timeout", type=float, default=120.0)
    ap.add_argument("--retries", type=int, default=6)
    args = ap.parse_args()

    cache_dir = Path(args.cache_dir) if args.cache_dir.strip() else None

    print(f"→ Loading Step3: {args.input}")
    df = pd.read_csv(args.input, dtype=str).fillna("")

    if "nba_player_id" not in df.columns:
        raise RuntimeError("❌ Input missing nba_player_id")
    if "prop_norm" not in df.columns:
        if "prop_type" in df.columns:
            df["prop_norm"] = df["prop_type"].astype(str).str.lower()
        else:
            raise RuntimeError("❌ Input missing prop_norm (and prop_type)")

    df["_line_num"] = pd.to_numeric(df["line"], errors="coerce") if "line" in df.columns else np.nan

    mem_cache: Dict[Tuple[int, str], pd.DataFrame] = {}

    all_ids: List[int] = []
    for s in df["nba_player_id"].astype(str).tolist():
        all_ids.extend(_parse_combo_ids(s))
    unique_ids = sorted(list(dict.fromkeys([i for i in all_ids if isinstance(i, int)])))

    print(f"→ Unique NBA player ids to fetch: {len(unique_ids)}")
    failed_ids = set()
    for i, pid in enumerate(unique_ids, start=1):
        try:
            _read_player_log(pid, args.season, cache_dir, mem_cache, args.sleep, args.timeout, args.retries)
        except Exception as e:
            print(f"⚠️ Skipping player_id={pid} after all retries failed: {e}")
            failed_ids.add(pid)
        if i % 25 == 0:
            print(f"  fetched {i}/{len(unique_ids)} ({len(failed_ids)} skipped)")

    N = int(args.n)
    stat_cols = [f"stat_g{i}" for i in range(1, N + 1)]
    new_cols = stat_cols + [
        "stat_last5_avg",
        "stat_last10_avg",
        "stat_season_avg",
        "last5_over",
        "last5_under",
        "last5_push",
        "last5_hit_rate",
        "stat_status",
    ]
    for c in new_cols:
        if c not in df.columns:
            df[c] = ""

    for idx, row in df.iterrows():
        nba_id_raw = str(row.get("nba_player_id", "")).strip()
        prop_norm = str(row.get("prop_norm", "")).strip().lower()
        ids = _parse_combo_ids(nba_id_raw)

        if not ids:
            df.at[idx, "stat_status"] = "NO_NBA_ID"
            continue

        # Skip players that failed all retries during prefetch
        if any(i in failed_ids for i in ids):
            df.at[idx, "stat_status"] = "FETCH_FAILED"
            continue

        if len(ids) == 1:
            log = mem_cache.get((ids[0], args.season))
            if log is None or len(log) == 0:
                df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue
            series = _compute_stat_series(log, prop_norm)
            if series.isna().all():
                df.at[idx, "stat_status"] = "UNSUPPORTED_PROP"
                continue
            season_vals = series.dropna().astype(float).tolist()   # asc order
        else:
            logs = [mem_cache.get((pid, args.season)) for pid in ids]
            aligned = _combo_aligned_sum([l for l in logs if l is not None], prop_norm)
            if aligned is None or len(aligned) == 0:
                df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue
            season_vals = pd.to_numeric(aligned["STAT"], errors="coerce").dropna().astype(float).tolist()  # asc

        if not season_vals:
            df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
            continue

        vals_mr = list(reversed(season_vals))  # most recent first

        # stat_g1..stat_gN as strings
        for i in range(1, N + 1):
            v = vals_mr[i - 1] if i - 1 < len(vals_mr) else np.nan
            df.at[idx, f"stat_g{i}"] = _to_str_num(v)

        def avg_last(k: int) -> float:
            v = season_vals[-k:] if len(season_vals) >= k else season_vals
            return float(np.mean(v)) if v else np.nan

        df.at[idx, "stat_last5_avg"] = _to_str_num(avg_last(5))
        df.at[idx, "stat_last10_avg"] = _to_str_num(avg_last(10))
        df.at[idx, "stat_season_avg"] = _to_str_num(float(np.mean(season_vals)))

        # hit vs line
        line_val = row.get("_line_num", np.nan)
        try:
            line_val = float(line_val)
        except Exception:
            line_val = np.nan

        if np.isnan(line_val):
            df.at[idx, "last5_over"] = ""
            df.at[idx, "last5_under"] = ""
            df.at[idx, "last5_push"] = ""
            df.at[idx, "last5_hit_rate"] = ""
        else:
            last5_vals = vals_mr[:5]
            over, under, push, hit_rate = _calc_last5_hit(last5_vals, line_val)
            df.at[idx, "last5_over"] = _to_str_num(over)
            df.at[idx, "last5_under"] = _to_str_num(under)
            df.at[idx, "last5_push"] = _to_str_num(push)
            df.at[idx, "last5_hit_rate"] = _to_str_num(hit_rate)

        df.at[idx, "stat_status"] = "OK"

    df = df.drop(columns=["_line_num"], errors="ignore")

    desired_front = ["nba_player_id", "player", "pos", "team", "opp_team", "line", "prop_type", "prop_norm", "pick_type"]
    front = [c for c in desired_front if c in df.columns]
    stats_block = [c for c in new_cols if c in df.columns]
    tail = [c for c in df.columns if c not in set(front + stats_block)]
    out = df[front + tail + stats_block].copy()

    out.to_csv(args.output, index=False, encoding="utf-8")
    print(f"✅ Saved → {args.output} | rows={len(out)}")
    print("stat_status breakdown:")
    print(out["stat_status"].astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()
