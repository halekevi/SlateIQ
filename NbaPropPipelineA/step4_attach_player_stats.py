#!/usr/bin/env python3
"""
step4_attach_player_stats.py  (NBA) — deterministic, id-driven, combo-safe
PATCHED v4: FIXED last-5 ordering (most recent first, guaranteed)

Key Fix:
- Sort game logs by GAME_DATE DESC (most recent -> oldest)
- Do NOT reverse arrays later
- last5 averages and last5 over/under are now computed from the true most recent 5 games

RECOMMENDED RUN:
  py -3.14 step4_attach_player_stats.py \
    --input step3_with_defense.csv \
    --output step4_with_stats.csv \
    --season 2025-26 \
    --cache-dir ./_nba_cache \
    --timeout 120 \
    --connect-timeout 10 \
    --retries 3 \
    --sleep 0.8 \
    --force-ipv4
"""

from __future__ import annotations

# ── IPv4 monkey-patch — must happen BEFORE any nba_api import ──────────────────
import argparse as _ap_early
import sys as _sys
import socket as _socket

_force_ipv4 = "--force-ipv4" in _sys.argv or "--no-force-ipv4" not in _sys.argv

if _force_ipv4:
    _orig_getaddrinfo = _socket.getaddrinfo

    def _ipv4_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
        results = _orig_getaddrinfo(host, port, family, type, proto, flags)
        ipv4 = [r for r in results if r[0] == _socket.AF_INET]
        return ipv4 if ipv4 else results

    _socket.getaddrinfo = _ipv4_getaddrinfo
    print("🔧 IPv4 mode: socket.getaddrinfo patched to prefer AF_INET")

# ── Now safe to import nba_api ─────────────────────────────────────────────────
import argparse
import random
import re
import time
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from nba_api.stats.endpoints import playergamelog

COMBO_SEP = "|"


# ── IPv4-forced requests session ───────────────────────────────────────────────

def _make_nba_session(connect_timeout: float = 10.0) -> requests.Session:
    session = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=0,
        backoff_factor=2,
        status_forcelist=[500, 502, 503, 504],
        allowed_methods=["GET"],
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


def _inject_session_into_nba_api(session: requests.Session) -> None:
    # Socket patch handles routing; session kept for future direct calls if needed.
    pass


# ── Stat computation ───────────────────────────────────────────────────────────

def _to_str_num(x) -> str:
    try:
        if x is None:
            return ""
        if isinstance(x, str):
            return x.strip()
        xf = float(x)
        if np.isnan(xf):
            return ""
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

    pts  = num("PTS");  reb  = num("REB");  ast  = num("AST")
    stl  = num("STL");  blk  = num("BLK");  tov  = num("TOV")
    fga  = num("FGA");  fgm  = num("FGM")
    fg3a = num("FG3A"); fg3m = num("FG3M")
    fta  = num("FTA");  ftm  = num("FTM")
    pf   = num("PF")
    fg2a = fga - fg3a;  fg2m = fgm - fg3m

    dreb = num("DREB")
    oreb = num("OREB")

    MAP = {
        ("pts", "points"):                          pts,
        ("reb", "rebounds"):                        reb,
        ("ast", "assists"):                         ast,
        ("pra",):                                   pts + reb + ast,
        ("pr",):                                    pts + reb,
        ("pa",):                                    pts + ast,
        ("ra",):                                    reb + ast,
        ("stocks",):                                stl + blk,
        ("stl", "steals"):                          stl,
        ("blk", "blocks"):                          blk,
        ("tov", "turnovers"):                       tov,
        ("pf", "personalfouls", "personal_fouls", "fouls"): pf,
        ("fantasy", "fantasyscore"):                pts + 1.2*reb + 1.5*ast + 3.0*stl + 3.0*blk - tov,
        ("fga",): fga, ("fgm",): fgm,
        ("fg3a", "3ptattempted"):                   fg3a,
        ("fg3m", "3ptmade"):                        fg3m,
        ("fg2a", "twopointersattempted"):           fg2a,
        ("fg2m", "twopointersmade"):                fg2m,
        ("fta", "freethrowsattempted"):             fta,
        ("ftm", "freethrowsmade"):                  ftm,
        ("dreb", "defensiverebounds"):              dreb,
        ("oreb", "offensiverebounds"):              oreb,
    }
    for keys, series in MAP.items():
        if p in keys:
            return series
    return pd.Series([np.nan] * len(df), index=df.index)


# ── NBA ID helpers ─────────────────────────────────────────────────────────────

def _clean_nba_id(s: str) -> str:
    x = str(s).strip()
    x = re.sub(r"\.0$", "", x)
    return x.replace(",", "")


def _parse_combo_ids(nba_player_id: str) -> List[int]:
    s = _clean_nba_id(nba_player_id)
    if not s:
        return []
    if COMBO_SEP in s:
        ids: List[int] = []
        for p in s.split(COMBO_SEP):
            try:
                ids.append(int(_clean_nba_id(p.strip())))
            except Exception:
                pass
        return sorted(list(dict.fromkeys(ids)))
    try:
        return [int(s)]
    except Exception:
        return []


# ── Game log fetcher with IPv4 fix ────────────────────────────────────────────

def _read_player_log(
    player_id: int,
    season: str,
    cache_dir: Optional[Path],
    mem_cache: Dict[Tuple[int, str], pd.DataFrame],
    sleep_s: float,
    timeout_s: float,
    connect_timeout_s: float,
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
                print(f"  💾 cache hit: player_id={player_id}")
                return df
            except Exception:
                pass

    last_err = None
    for attempt in range(1, retries + 1):
        try:
            if sleep_s > 0:
                jitter = random.uniform(0, min(0.5, sleep_s * 0.3))
                time.sleep(sleep_s + jitter)

            t0 = time.time()
            gl = playergamelog.PlayerGameLog(
                player_id=player_id,
                season=season,
                timeout=(connect_timeout_s, timeout_s),
            )
            raw = gl.get_data_frames()[0]
            elapsed = time.time() - t0

            if raw is None or len(raw) == 0:
                print(f"  ⚠️  player_id={player_id}: empty response — skipping [{elapsed:.1f}s]")
                empty = pd.DataFrame()
                mem_cache[key] = empty
                return empty

            df = raw.copy()
            for c in df.columns:
                df[c] = df[c].astype(str)

            # Remove DNP / MIN=0
            if "MIN" in df.columns:
                mins = pd.to_numeric(df["MIN"], errors="coerce")
                df = df.loc[mins.fillna(0) > 0].copy()

            # ✅ FIX: sort MOST RECENT FIRST
            if "GAME_DATE" in df.columns:
                df["GAME_DATE"] = pd.to_datetime(df["GAME_DATE"], errors="coerce")
                df = df.sort_values("GAME_DATE", ascending=False)

            if fp is not None:
                try:
                    df.to_csv(fp, index=False, encoding="utf-8")
                except Exception:
                    pass

            mem_cache[key] = df
            print(f"  ✅ player_id={player_id}: {len(df)} games [{elapsed:.1f}s]")
            return df

        except (requests.exceptions.ConnectTimeout,) as e:
            last_err = e
            backoff = min(15.0, (2 ** (attempt - 1)) * 2.0) + random.uniform(0.5, 2.0)
            print(f"  🔌 CONNECT TIMEOUT player_id={player_id} attempt {attempt}/{retries} — retry in {backoff:.1f}s")
            time.sleep(backoff)

        except (requests.exceptions.ReadTimeout,) as e:
            last_err = e
            backoff = min(30.0, (2 ** (attempt - 1)) * 3.0) + random.uniform(1.0, 5.0)
            print(f"  ⏱️  READ TIMEOUT player_id={player_id} attempt {attempt}/{retries} — retry in {backoff:.1f}s")
            time.sleep(backoff)

        except Exception as e:
            last_err = e
            backoff = min(20.0, (2 ** (attempt - 1)) * 1.5) + random.random()
            print(f"  ⚠️  ERROR player_id={player_id} attempt {attempt}/{retries}: {type(e).__name__}: {e} — retry in {backoff:.1f}s")
            time.sleep(backoff)

    print(f"  ❌ SKIPPING player_id={player_id} after {retries} retries. Last: {type(last_err).__name__}")
    empty = pd.DataFrame()
    mem_cache[key] = empty
    return empty


# ── Combo game log alignment ───────────────────────────────────────────────────

def _combo_aligned_sum(logs: List[pd.DataFrame], prop_norm: str) -> pd.DataFrame:
    if not logs:
        return pd.DataFrame(columns=["GAME_ID", "GAME_DATE", "STAT"])

    prepared = []
    for df in logs:
        if df is None or len(df) == 0 or "GAME_ID" not in df.columns:
            continue
        tmp = df.copy()
        tmp["STAT"] = _compute_stat_series(tmp, prop_norm)
        cols = ["GAME_ID", "STAT"] + (["GAME_DATE"] if "GAME_DATE" in tmp.columns else [])
        prepared.append(tmp[cols].copy())

    if not prepared:
        return pd.DataFrame(columns=["GAME_ID", "GAME_DATE", "STAT"])

    merged = prepared[0][["GAME_ID", "STAT"]].rename(columns={"STAT": "STAT_1"})
    for i, p in enumerate(prepared[1:], start=2):
        merged = merged.merge(
            p[["GAME_ID", "STAT"]].rename(columns={"STAT": f"STAT_{i}"}),
            on="GAME_ID", how="inner"
        )

    stat_cols = [c for c in merged.columns if c.startswith("STAT_")]
    merged["STAT"] = merged[stat_cols].apply(pd.to_numeric, errors="coerce").sum(axis=1)

    if "GAME_DATE" in prepared[0].columns:
        merged = merged.merge(prepared[0][["GAME_ID", "GAME_DATE"]], on="GAME_ID", how="left")
        merged["GAME_DATE"] = pd.to_datetime(merged["GAME_DATE"], errors="coerce")
        # ✅ FIX: most recent first
        merged = merged.sort_values("GAME_DATE", ascending=False)

    keep = ["GAME_ID"] + (["GAME_DATE"] if "GAME_DATE" in merged.columns else []) + ["STAT"]
    return merged[keep].copy()


# ── Hit rate calculator ────────────────────────────────────────────────────────

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
    return over, under, push, (over / denom if denom > 0 else np.nan)


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> None:
    ap = argparse.ArgumentParser(description="Step4: Attach player stats (IPv4-safe, fixed last-5 ordering)")
    ap.add_argument("--input",           default="step3_with_defense.csv")
    ap.add_argument("--output",          default="step4_with_stats.csv")
    ap.add_argument("--season",          default="2025-26")
    ap.add_argument("--n",               type=int,   default=10)
    ap.add_argument("--cache-dir",       default="")
    ap.add_argument("--sleep",           type=float, default=0.8)
    ap.add_argument("--timeout",         type=float, default=120.0)
    ap.add_argument("--connect-timeout", type=float, default=10.0)
    ap.add_argument("--retries",         type=int,   default=3)
    ap.add_argument("--force-ipv4",      action="store_true", default=True)
    ap.add_argument("--no-force-ipv4",   action="store_true", default=False)
    ap.add_argument("--dry-run",         action="store_true")
    args = ap.parse_args()

    if args.no_force_ipv4:
        print("⚠️  IPv4 forcing DISABLED (--no-force-ipv4). Note: socket patch may already be active.")
    else:
        print(f"🔧 IPv4 mode: ON (connect timeout: {args.connect_timeout}s, read timeout: {args.timeout}s)")

    cache_dir = Path(args.cache_dir) if args.cache_dir.strip() else None

    print(f"\n→ Loading Step3: {args.input}")
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

    print(f"→ Unique NBA player IDs to fetch: {len(unique_ids)}")

    if args.dry_run:
        print("\n🔍 DRY RUN — would fetch these player IDs:")
        for pid in unique_ids:
            cached = ""
            if cache_dir:
                fp = cache_dir / f"playergamelog_{args.season}_{pid}.csv"
                cached = " [CACHED]" if fp.exists() else " [API]"
            print(f"  {pid}{cached}")
        print(f"\nTotal: {len(unique_ids)} players")
        return

    print(f"\n→ Fetching game logs (season={args.season})...\n")
    failed_ids = set()
    for i, pid in enumerate(unique_ids, start=1):
        try:
            _read_player_log(
                pid, args.season, cache_dir, mem_cache,
                args.sleep, args.timeout, args.connect_timeout, args.retries
            )
        except Exception as e:
            print(f"  ❌ player_id={pid} failed unexpectedly: {e}")
            failed_ids.add(pid)
        if i % 25 == 0:
            print(f"\n  [{i}/{len(unique_ids)} fetched | {len(failed_ids)} skipped]\n")

    print(f"\n→ Fetch complete: {len(unique_ids) - len(failed_ids)}/{len(unique_ids)} players OK")
    if failed_ids:
        print(f"  Skipped IDs: {sorted(failed_ids)}")

    N = int(args.n)
    stat_cols = [f"stat_g{i}" for i in range(1, N + 1)]
    new_cols = stat_cols + [
        "stat_last5_avg", "stat_last10_avg", "stat_season_avg",
        "last5_over", "last5_under", "last5_push", "last5_hit_rate",
        "stat_status",
    ]
    for c in new_cols:
        if c not in df.columns:
            df[c] = ""

    print(f"\n→ Attaching stats to {len(df)} rows...\n")
    for idx, row in df.iterrows():
        nba_id_raw = str(row.get("nba_player_id", "")).strip()
        prop_norm  = str(row.get("prop_norm", "")).strip().lower()
        ids = _parse_combo_ids(nba_id_raw)

        if not ids:
            df.at[idx, "stat_status"] = "NO_NBA_ID"
            continue
        if any(i in failed_ids for i in ids):
            df.at[idx, "stat_status"] = "FETCH_FAILED"
            continue

        # season_vals will now always be MOST RECENT FIRST
        if len(ids) == 1:
            log = mem_cache.get((ids[0], args.season))
            if log is None or len(log) == 0:
                df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue

            series = _compute_stat_series(log, prop_norm)
            if series.isna().all():
                df.at[idx, "stat_status"] = "UNSUPPORTED_PROP"
                continue

            season_vals = series.dropna().astype(float).tolist()
        else:
            logs = [mem_cache.get((pid, args.season)) for pid in ids]
            aligned = _combo_aligned_sum([l for l in logs if l is not None], prop_norm)
            if aligned is None or len(aligned) == 0:
                df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
                continue
            season_vals = pd.to_numeric(aligned["STAT"], errors="coerce").dropna().astype(float).tolist()

        if not season_vals:
            df.at[idx, "stat_status"] = "INSUFFICIENT_GAMES"
            continue

        vals_mr = season_vals  # ✅ already most recent first

        for i in range(1, N + 1):
            v = vals_mr[i - 1] if i - 1 < len(vals_mr) else np.nan
            df.at[idx, f"stat_g{i}"] = _to_str_num(v)

        def avg_last(k: int) -> float:
            v = vals_mr[:k] if len(vals_mr) >= k else vals_mr
            return float(np.mean(v)) if v else np.nan

        df.at[idx, "stat_last5_avg"]  = _to_str_num(avg_last(5))
        df.at[idx, "stat_last10_avg"] = _to_str_num(avg_last(10))
        df.at[idx, "stat_season_avg"] = _to_str_num(float(np.mean(vals_mr)))

        line_val = row.get("_line_num", np.nan)
        try:
            line_val = float(line_val)
        except Exception:
            line_val = np.nan

        if not np.isnan(line_val):
            over, under, push, hit_rate = _calc_last5_hit(vals_mr[:5], line_val)
            df.at[idx, "last5_over"]     = _to_str_num(over)
            df.at[idx, "last5_under"]    = _to_str_num(under)
            df.at[idx, "last5_push"]     = _to_str_num(push)
            df.at[idx, "last5_hit_rate"] = _to_str_num(hit_rate)

        df.at[idx, "stat_status"] = "OK"

    df = df.drop(columns=["_line_num"], errors="ignore")

    desired_front = ["nba_player_id", "player", "pos", "team", "opp_team",
                     "line", "prop_type", "prop_norm", "pick_type"]
    front       = [c for c in desired_front if c in df.columns]
    stats_block = [c for c in new_cols if c in df.columns]
    tail        = [c for c in df.columns if c not in set(front + stats_block)]
    out         = df[front + tail + stats_block].copy()

    out.to_csv(args.output, index=False, encoding="utf-8")
    print(f"\n✅ Saved → {args.output} | rows={len(out)}")
    print("\nstat_status breakdown:")
    print(out["stat_status"].astype(str).value_counts().to_string())


if __name__ == "__main__":
    main()