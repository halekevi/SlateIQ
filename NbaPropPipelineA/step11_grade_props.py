#!/usr/bin/env python3
r"""
step10_grade_props.py  (Pipeline A - Step 10)

Grades props using nba_api + NBA CDN liveData boxscore:
- scoreboardv2 -> games for date (and optional +/- window)
- NBA CDN liveData boxscore -> player boxscores (preferred, reliable in 2025-26)
- boxscoretraditionalv3 -> fallback if CDN fails

Designed to grade *eligible props only* (if `eligible` column exists),
and produce breakdowns by:
- minutes_tier / minutes_bucket
- DEF_TIER / def_bucket
- abs_edge_bucket
- tier / pick_type / direction

INPUTS:
- Primary input (recommended): Step 9 formatted output (xlsx) OR Step 8 output (csv/xlsx)
    --infile step9_formatted_ranked.xlsx
    --infile step8_all_direction.csv
- Optional enrichment: Step 7 ranked props (xlsx) to pull missing raw columns
    --step7 step7_ranked_props.xlsx

OUTPUTS:
- step10_results_<date>.xlsx  (tabs: GRADED, SUMMARY, BY_MINUTES, BY_DEF, BY_EDGE)
- step10_results_<date>_graded.csv

Run:
  py -3.14 step10_grade_props.py --date 2026-02-12 --infile step9_formatted_ranked.xlsx --out step10_results_2026-02-12.xlsx

If your props are for games that actually played a day earlier/later than you think,
use a window to search nearby dates and merge actuals:
  py -3.14 step10_grade_props.py --date 2026-02-12 --window_days 2 --infile step9_formatted_ranked.xlsx --out step10_results_2026-02-12.xlsx

Optional merge from Step 7:
  py -3.14 step10_grade_props.py --date 2026-02-12 --window_days 2 --infile step9_formatted_ranked.xlsx --step7 step7_ranked_props.xlsx --out step10_results_2026-02-12.xlsx
"""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple, List, Any

import numpy as np
import pandas as pd
import requests

from nba_api.stats.endpoints import scoreboardv2, boxscoretraditionalv3


# ----------------------------
# Helpers: IO
# ----------------------------
def _read_any(path: Path, sheet: Optional[str] = None) -> pd.DataFrame:
    suf = path.suffix.lower()
    if suf == ".csv":
        try:
            return pd.read_csv(path, low_memory=False)
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="latin-1", low_memory=False)
    if suf in {".xlsx", ".xlsm", ".xls"}:
        if sheet:
            return pd.read_excel(path, sheet_name=sheet)
        # prefer ALL if exists, else first sheet
        xls = pd.ExcelFile(path)
        if "ALL" in xls.sheet_names:
            return pd.read_excel(path, sheet_name="ALL")
        return pd.read_excel(path, sheet_name=xls.sheet_names[0])
    raise SystemExit(f"Unsupported input type: {suf}. Use .csv or .xlsx")


def _coalesce_col(df: pd.DataFrame, *cands: str) -> Optional[str]:
    cols_l = {c.lower(): c for c in df.columns}
    for c in cands:
        if c.lower() in cols_l:
            return cols_l[c.lower()]
    return None


def _ensure_unique_col(df: pd.DataFrame, col: str) -> None:
    """If df[col] is a DataFrame due to duplicate columns, keep the first."""
    if col not in df.columns:
        return
    obj = df[col]
    if isinstance(obj, pd.DataFrame):
        first = obj.iloc[:, 0]
        keep_cols = []
        seen = False
        for c in df.columns:
            if c == col:
                if not seen:
                    keep_cols.append(c)
                    seen = True
                else:
                    continue
            else:
                keep_cols.append(c)
        df2 = df.loc[:, keep_cols].copy()
        df2[col] = first
        df.drop(df.index, inplace=True)
        for c in df2.columns:
            df[c] = df2[c]


# ----------------------------
# Helpers: normalization
# ----------------------------
def norm_prop_type(x: Any) -> str:
    s = str(x).strip().lower()

    s = s.replace("points (combo)", "points")
    s = s.replace("rebounds (combo)", "rebounds")
    s = s.replace("assists (combo)", "assists")
    s = s.replace("3-pt", "3pt")
    s = s.replace("3 pt", "3pt")
    s = s.replace("two pointers", "2pt")
    s = s.replace("free throws", "ft")

    if "fantasy" in s:
        return "fantasy"
    if s in {"points", "pts"} or "points" in s:
        return "pts"
    if "rebounds" in s or s == "reb":
        return "reb"
    if "assists" in s or s == "ast":
        return "ast"
    if "pra" in s:
        return "pra"
    if s == "pr" or "points + rebounds" in s:
        return "pr"
    if s == "pa" or "points + assists" in s:
        return "pa"
    if s == "ra" or "rebounds + assists" in s:
        return "ra"
    if "stocks" in s:
        return "stocks"
    if "steals" in s:
        return "stl"
    if "blocks" in s:
        return "blk"
    if "turnovers" in s:
        return "tov"

    if "3pt made" in s or "3 pointers made" in s:
        return "fg3m"
    if "3pt attempted" in s or "3 pointers attempted" in s:
        return "fg3a"
    if "2pt made" in s:
        return "fg2m"
    if "2pt attempted" in s:
        return "fg2a"
    if s in {"fga", "field goals attempted"} or "field goals attempted" in s:
        return "fga"
    if s in {"fgm", "field goals made"} or "field goals made" in s:
        return "fgm"
    if "ft made" in s:
        return "ftm"
    if "ft attempted" in s:
        return "fta"

    if "defensive rebounds" in s:
        return "dreb"
    if "offensive rebounds" in s:
        return "oreb"

    return ""


def pick_type_key(x: Any) -> str:
    s = str(x).strip().lower()
    if "gob" in s:
        return "goblin"
    if "dem" in s:
        return "demon"
    return "standard"


# ----------------------------
# Buckets for analysis
# ----------------------------
def abs_edge_bucket(x: float) -> str:
    if pd.isna(x):
        return "NA"
    x = float(abs(x))
    if x >= 3:
        return "3+"
    if x >= 2:
        return "2-2.99"
    if x >= 1:
        return "1-1.99"
    return "0-0.99"


def minutes_bucket_from_min(mins: float) -> str:
    if pd.isna(mins):
        return "NA"
    if mins >= 34:
        return "34+"
    if mins >= 28:
        return "28-33.99"
    if mins >= 22:
        return "22-27.99"
    return "<22"


def def_bucket_from_rank(r: float) -> str:
    if pd.isna(r):
        return "NA"
    r = float(r)
    if r <= 10:
        return "1-10 Elite"
    if r <= 20:
        return "11-20 Avg"
    return "21-30 Weak"


# ----------------------------
# Actual stat extraction
# ----------------------------
def _safe_float(x) -> float:
    try:
        if pd.isna(x):
            return np.nan
        return float(x)
    except Exception:
        return np.nan


def _parse_min_to_float(x) -> float:
    try:
        if pd.isna(x):
            return np.nan
    except Exception:
        pass
    s = str(x).strip()
    if s == "" or s.lower() == "nan":
        return np.nan
    if ":" in s:
        try:
            mm, ss = s.split(":")
            return float(mm) + float(ss) / 60.0
        except Exception:
            return np.nan
    return _safe_float(x)


def compute_actual_from_row(row: pd.Series, actual_map: Dict[int, Dict[str, float]]) -> Tuple[float, str]:
    pid_raw = row.get("nba_player_id", None)
    if pd.isna(pid_raw):
        return (np.nan, "MISSING_PLAYER_ID")

    pid_s = str(pid_raw).strip()

    if isinstance(row.get("player", ""), str) and "+" in str(row.get("player", "")):
        return (np.nan, "COMBO_PROP_NAME")

    pn = str(row.get("prop_norm", "")).strip().lower()
    if not pn:
        pn = str(row.get("prop_type_norm", "")).strip().lower()
    pn = pn.strip()

    prop_type_raw = str(row.get("prop_type", "")).lower()
    if "1st" in prop_type_raw or "quarters" in prop_type_raw:
        return (np.nan, "UNSUPPORTED_TIME_WINDOW")

    def stat_for_player(pid: int) -> Dict[str, float]:
        return actual_map.get(pid, {})

    def get(pid: int, key: str) -> float:
        return _safe_float(stat_for_player(pid).get(key, np.nan))

    def compute_for_pid(pid: int) -> Tuple[float, str]:
        if pid not in actual_map:
            return (np.nan, "NO_ACTUAL_FOUND")

        pts = get(pid, "PTS")
        reb = get(pid, "REB")
        ast = get(pid, "AST")
        stl = get(pid, "STL")
        blk = get(pid, "BLK")
        tov = get(pid, "TO")
        fga = get(pid, "FGA")
        fgm = get(pid, "FGM")
        fg3a = get(pid, "FG3A")
        fg3m = get(pid, "FG3M")
        fta = get(pid, "FTA")
        ftm = get(pid, "FTM")
        oreb = get(pid, "OREB")
        dreb = get(pid, "DREB")

        fg2a = np.nan
        fg2m = np.nan
        if pd.notna(fga) and pd.notna(fg3a):
            fg2a = fga - fg3a
        if pd.notna(fgm) and pd.notna(fg3m):
            fg2m = fgm - fg3m

        if pn == "pts":
            return (pts, "")
        if pn == "reb":
            return (reb, "")
        if pn == "ast":
            return (ast, "")
        if pn == "stl":
            return (stl, "")
        if pn == "blk":
            return (blk, "")
        if pn == "stocks":
            if pd.isna(stl) and pd.isna(blk):
                return (np.nan, "MISSING_STL_BLK")
            return (np.nan_to_num(stl) + np.nan_to_num(blk), "")
        if pn == "tov":
            return (tov, "")
        if pn == "fga":
            return (fga, "")
        if pn == "fgm":
            return (fgm, "")
        if pn == "fg3a":
            return (fg3a, "")
        if pn == "fg3m":
            return (fg3m, "")
        if pn == "fg2a":
            return (fg2a, "")
        if pn == "fg2m":
            return (fg2m, "")
        if pn == "fta":
            return (fta, "")
        if pn == "ftm":
            return (ftm, "")
        if pn == "oreb":
            return (oreb, "")
        if pn == "dreb":
            return (dreb, "")
        if pn == "pr":
            return (np.nan_to_num(pts) + np.nan_to_num(reb), "")
        if pn == "pa":
            return (np.nan_to_num(pts) + np.nan_to_num(ast), "")
        if pn == "ra":
            return (np.nan_to_num(reb) + np.nan_to_num(ast), "")
        if pn == "pra":
            return (np.nan_to_num(pts) + np.nan_to_num(reb) + np.nan_to_num(ast), "")
        if pn == "fantasy":
            if pd.isna(pts) and pd.isna(reb) and pd.isna(ast):
                return (np.nan, "MISSING_CORE_STATS")
            v = (
                np.nan_to_num(pts)
                + 1.2 * np.nan_to_num(reb)
                + 1.5 * np.nan_to_num(ast)
                + 3.0 * np.nan_to_num(stl)
                + 3.0 * np.nan_to_num(blk)
                - 1.0 * np.nan_to_num(tov)
            )
            return (v, "")
        return (np.nan, "UNSUPPORTED_PROP")

    if "|" in pid_s:
        parts = [p.strip() for p in pid_s.split("|") if p.strip()]
        if len(parts) != 2:
            return (np.nan, "BAD_COMBO_ID")
        try:
            p1 = int(float(parts[0]))
            p2 = int(float(parts[1]))
        except Exception:
            return (np.nan, "BAD_COMBO_ID")

        a1, r1 = compute_for_pid(p1)
        a2, r2 = compute_for_pid(p2)

        if pd.isna(a1) or r1:
            return (np.nan, f"COMBO_P1_{r1 or 'NO_ACTUAL'}")
        if pd.isna(a2) or r2:
            return (np.nan, f"COMBO_P2_{r2 or 'NO_ACTUAL'}")

        return (float(a1) + float(a2), "")

    try:
        pid = int(float(pid_s))
    except Exception:
        return (np.nan, "BAD_PLAYER_ID")

    return compute_for_pid(pid)


# ----------------------------
# Boxscore fetching (CDN + V3 fallback) with date window support
# ----------------------------
def _scoreboard_game_ids(date_yyyy_mm_dd: str) -> List[str]:
    dt = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
    date_nba = dt.strftime("%m/%d/%Y")
    sb = scoreboardv2.ScoreboardV2(game_date=date_nba)
    games = sb.get_data_frames()[0]
    if games is None or games.empty:
        return []
    return games["GAME_ID"].astype(str).tolist()


def _fetch_cdn_boxscore(game_id: str) -> Tuple[Dict[int, Dict[str, float]], bool]:
    url = f"https://cdn.nba.com/static/json/liveData/boxscore/boxscore_{game_id}.json"
    try:
        r = requests.get(url, timeout=12)
        if r.status_code != 200:
            return {}, False
        data = r.json()
        game = data.get("game", {})
        home_players = (game.get("homeTeam", {}) or {}).get("players", []) or []
        away_players = (game.get("awayTeam", {}) or {}).get("players", []) or []
        players = list(home_players) + list(away_players)
        if not players:
            return {}, False

        actual_map: Dict[int, Dict[str, float]] = {}
        for p in players:
            pid = p.get("personId")
            if not pid:
                continue
            stats = p.get("statistics", {}) or {}
            # minutes sometimes comes as "PTxxMxx.xxS" or "mm:ss" or numeric-ish; keep raw and parse later
            actual_map[int(pid)] = {
                "PTS": _safe_float(stats.get("points")),
                "REB": _safe_float(stats.get("reboundsTotal")),
                "AST": _safe_float(stats.get("assists")),
                "STL": _safe_float(stats.get("steals")),
                "BLK": _safe_float(stats.get("blocks")),
                "TO": _safe_float(stats.get("turnovers")),
                "FGA": _safe_float(stats.get("fieldGoalsAttempted")),
                "FGM": _safe_float(stats.get("fieldGoalsMade")),
                "FG3A": _safe_float(stats.get("threePointersAttempted")),
                "FG3M": _safe_float(stats.get("threePointersMade")),
                "FTA": _safe_float(stats.get("freeThrowsAttempted")),
                "FTM": _safe_float(stats.get("freeThrowsMade")),
                "OREB": _safe_float(stats.get("reboundsOffensive")),
                "DREB": _safe_float(stats.get("reboundsDefensive")),
                "MIN": _parse_min_to_float(stats.get("minutes")),
            }
        return actual_map, True
    except Exception:
        return {}, False


def _fetch_v3_boxscore(game_id: str) -> Tuple[Dict[int, Dict[str, float]], bool]:
    try:
        bs3 = boxscoretraditionalv3.BoxScoreTraditionalV3(game_id=game_id)
        dfs = bs3.get_data_frames() or []
        players_df: Optional[pd.DataFrame] = None
        for d in dfs:
            if isinstance(d, pd.DataFrame) and (not d.empty) and ("PLAYER_ID" in [str(c).upper() for c in d.columns]):
                players_df = d.copy()
                break
        if players_df is None or players_df.empty:
            return {}, False

        players_df.columns = [str(c).upper() for c in players_df.columns]
        if "PLAYER_ID" not in players_df.columns:
            return {}, False

        players_df["PLAYER_ID"] = pd.to_numeric(players_df["PLAYER_ID"], errors="coerce")
        players_df = players_df.dropna(subset=["PLAYER_ID"])
        if players_df.empty:
            return {}, False

        actual_map: Dict[int, Dict[str, float]] = {}
        for _, r in players_df.iterrows():
            pid = int(r["PLAYER_ID"])
            tov_val = r.get("TO")
            if tov_val is None:
                tov_val = r.get("TOV")
            actual_map[pid] = {
                "PTS": _safe_float(r.get("PTS")),
                "REB": _safe_float(r.get("REB")),
                "AST": _safe_float(r.get("AST")),
                "STL": _safe_float(r.get("STL")),
                "BLK": _safe_float(r.get("BLK")),
                "TO": _safe_float(tov_val),
                "FGA": _safe_float(r.get("FGA")),
                "FGM": _safe_float(r.get("FGM")),
                "FG3A": _safe_float(r.get("FG3A")),
                "FG3M": _safe_float(r.get("FG3M")),
                "FTA": _safe_float(r.get("FTA")),
                "FTM": _safe_float(r.get("FTM")),
                "MIN": _parse_min_to_float(r.get("MIN")),
                "OREB": _safe_float(r.get("OREB")),
                "DREB": _safe_float(r.get("DREB")),
            }
        return actual_map, True
    except Exception:
        return {}, False


def fetch_boxscores_for_date(date_yyyy_mm_dd: str, sleep_s: float = 0.6, window_days: int = 0) -> Dict[int, Dict[str, float]]:
    """
    Fetch actual_map across date_yyyy_mm_dd +/- window_days (inclusive).
    This solves the common issue where your props slate date != the game date you’re grading.
    """
    base = datetime.strptime(date_yyyy_mm_dd, "%Y-%m-%d")
    offsets = list(range(-abs(window_days), abs(window_days) + 1)) if window_days else [0]
    dates = [(base + timedelta(days=o)).strftime("%Y-%m-%d") for o in offsets]

    all_game_ids: List[str] = []
    per_date_counts: Dict[str, int] = {}
    for d in dates:
        gids = _scoreboard_game_ids(d)
        per_date_counts[d] = len(gids)
        all_game_ids.extend(gids)

    # de-dupe keep order
    seen = set()
    game_ids = []
    for g in all_game_ids:
        if g not in seen:
            seen.add(g)
            game_ids.append(g)

    total_games = len(game_ids)
    print(f"→ Games found on scoreboard: {total_games} (dates: " + ", ".join([f"{k}={v}" for k,v in per_date_counts.items()]) + ")")
    if total_games == 0:
        return {}

    actual_map: Dict[int, Dict[str, float]] = {}
    cdn_ok = 0
    v3_ok = 0

    for gid in game_ids:
        time.sleep(sleep_s)

        m, ok = _fetch_cdn_boxscore(gid)
        if ok and m:
            cdn_ok += 1
            actual_map.update(m)
            continue

        m2, ok2 = _fetch_v3_boxscore(gid)
        if ok2 and m2:
            v3_ok += 1
            actual_map.update(m2)

    print(f"→ Boxscore sources: CDN_ok={cdn_ok}/{total_games}, V3_fallback_ok={v3_ok}/{total_games}")
    return actual_map


# ----------------------------
# Summary utilities
# ----------------------------
def summarize(df: pd.DataFrame, group_cols: List[str]) -> pd.DataFrame:
    work = df.copy()
    for c in group_cols:
        if c not in work.columns:
            work[c] = "NA"

    g = work.groupby(group_cols, dropna=False)
    out = g["result"].agg(
        rows="count",
        hits=lambda s: int((s == "HIT").sum()),
        misses=lambda s: int((s == "MISS").sum()),
        pushes=lambda s: int((s == "PUSH").sum()),
        voids=lambda s: int((s == "VOID").sum()),
    ).reset_index()

    denom = (out["rows"] - out["voids"]).replace(0, np.nan)
    out["hit_rate_ex_void"] = (out["hits"] / denom).round(4)
    return out.sort_values(["rows"], ascending=False).reset_index(drop=True)


# ----------------------------
# Main
# ----------------------------
def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--date", required=True, help="Anchor date YYYY-MM-DD (we can also search +/- window_days).")
    ap.add_argument("--window_days", type=int, default=0, help="Search +/- N days around --date for game IDs (default 0).")
    ap.add_argument("--infile", required=True, help="Input: Step9 formatted XLSX OR Step8 output CSV/XLSX OR Step7 ranked XLSX")
    ap.add_argument("--sheet", default=None, help="(XLSX only) sheet name to read. Default: ALL if present else first.")
    ap.add_argument("--step7", default=None, help="Optional Step7 ranked XLSX to enrich missing columns.")
    ap.add_argument("--step7_sheet", default="ALL", help="Step7 sheet name (default ALL).")
    ap.add_argument("--out", default=None, help="Output XLSX path. Default: step10_results_<date>.xlsx")
    ap.add_argument("--sleep", type=float, default=0.6, help="Sleep seconds between game calls (default 0.6)")
    ap.add_argument("--grade_all", action="store_true", help="Grade all rows even if eligible column exists. Default grades eligible only.")
    args = ap.parse_args()

    date = args.date.strip()
    in_path = Path(args.infile)
    if not in_path.exists():
        raise FileNotFoundError(f"Input not found: {in_path}")

    out_xlsx = Path(args.out) if args.out else Path(f"step10_results_{date}.xlsx")
    out_csv = out_xlsx.with_suffix("").as_posix() + "_graded.csv"

    print(f"→ Loading: {in_path}")
    df = _read_any(in_path, sheet=args.sheet).copy()

    pid_col = _coalesce_col(df, "nba_player_id", "player_id", "playerid")
    if not pid_col:
        raise RuntimeError("Missing player id column (expected nba_player_id/player_id).")
    if pid_col != "nba_player_id":
        df["nba_player_id"] = df[pid_col]

    pcol = _coalesce_col(df, "player", "name", "player_name")
    if pcol and pcol != "player":
        df["player"] = df[pcol]

    if "line" not in df.columns:
        raise RuntimeError("Missing required column: line")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")

    if "prop_norm" not in df.columns:
        src = _coalesce_col(df, "prop_norm", "prop_type_norm", "prop_type", "prop")
        df["prop_norm"] = df[src].apply(norm_prop_type) if src else ""
    else:
        df["prop_norm"] = df["prop_norm"].astype(str).str.lower().str.strip()
        allowed = {
            "pts","reb","ast","pr","pa","ra","pra","tov","stl","blk","stocks","fantasy",
            "fga","fgm","fg3a","fg3m","fg2a","fg2m","fta","ftm","oreb","dreb"
        }
        df["prop_norm"] = df["prop_norm"].apply(lambda x: x if x in allowed else norm_prop_type(x))

    if "final_bet_direction" in df.columns and "bet_direction" in df.columns:
        df.rename(columns={"bet_direction": "bet_direction_raw"}, inplace=True)
    if "final_bet_direction" in df.columns:
        df["bet_direction"] = df["final_bet_direction"]
    _ensure_unique_col(df, "bet_direction")

    if "pick_type" not in df.columns:
        src = _coalesce_col(df, "picktype", "pick_type_key")
        df["pick_type"] = df[src] if src else "Standard"
    df["pick_type_key"] = df["pick_type"].apply(pick_type_key)

    if "tier" not in df.columns:
        df["tier"] = "NA"

    if ("eligible" in df.columns) and (not args.grade_all):
        elig = df["eligible"]
        if isinstance(elig, pd.DataFrame):
            elig = elig.iloc[:, 0]
        elig = elig.astype(str).str.lower().isin(["true", "1", "yes"])
        before = len(df)
        df = df.loc[elig].copy()
        print(f"→ Filtering eligible only: {len(df)}/{before}")

    if args.step7:
        s7 = Path(args.step7)
        if s7.exists():
            print(f"→ Enriching from Step7: {s7} (sheet={args.step7_sheet})")
            s7df = _read_any(s7, sheet=args.step7_sheet).copy()

            for col in ["team", "opp_team"]:
                if col not in df.columns and col in s7df.columns:
                    df[col] = pd.NA

            s7df["line"] = pd.to_numeric(s7df.get("line", np.nan), errors="coerce")
            if "prop_norm" not in s7df.columns:
                src = _coalesce_col(s7df, "prop_norm", "prop_type_norm", "prop_type")
                s7df["prop_norm"] = s7df[src].apply(norm_prop_type) if src else ""
            else:
                s7df["prop_norm"] = s7df["prop_norm"].apply(norm_prop_type)

            s7_pid_col = _coalesce_col(s7df, "nba_player_id", "player_id", "playerid")
            if s7_pid_col and s7_pid_col != "nba_player_id":
                s7df["nba_player_id"] = s7df[s7_pid_col]

            def make_key(x: pd.DataFrame) -> pd.Series:
                return (
                    x["nba_player_id"].astype(str).str.strip()
                    + "|" + x.get("team", "").astype(str).str.strip()
                    + "|" + x.get("opp_team", "").astype(str).str.strip()
                    + "|" + x["prop_norm"].astype(str).str.strip()
                    + "|" + x["line"].astype(str)
                )

            df["_k"] = make_key(df)
            s7df["_k"] = make_key(s7df)

            enrich_cols = [
                "projection","edge","abs_edge","edge_dr","rank_score","minutes_tier","shot_role","usage_role",
                "min_player_season_avg","OVERALL_DEF_RANK","DEF_TIER","OVERALL_DEF_SCORE","DEF_RATING",
                "last5_over","last5_under","last5_push","last5_hit_rate",
                "line_hits_over_5","line_hits_under_5","line_hits_push_5",
                "line_hit_rate_over_5","line_hit_rate_under_5","line_hit_rate_over_ou_5","line_hit_rate_under_ou_5",
            ]
            avail = [c for c in enrich_cols if c in s7df.columns]
            s7small = s7df[["_k"] + avail].drop_duplicates("_k", keep="last").set_index("_k")

            for c in avail:
                if c not in df.columns or df[c].isna().all():
                    df[c] = df["_k"].map(s7small[c])

            df.drop(columns=["_k"], inplace=True, errors="ignore")
        else:
            print(f"⚠️ Step7 file not found: {s7}")

    if "abs_edge" in df.columns:
        df["abs_edge"] = pd.to_numeric(df["abs_edge"], errors="coerce")
        df["abs_edge_bucket"] = df["abs_edge"].apply(abs_edge_bucket)
    else:
        df["abs_edge_bucket"] = "NA"

    min_src = _coalesce_col(df, "min_player_last10_avg", "min_player_last5_avg", "min_player_season_avg", "min_player_avg", "min", "MIN")
    if min_src:
        df["minutes_bucket"] = pd.to_numeric(df[min_src], errors="coerce").apply(minutes_bucket_from_min)
    else:
        df["minutes_bucket"] = "NA"

    if "OVERALL_DEF_RANK" in df.columns:
        df["def_bucket"] = pd.to_numeric(df["OVERALL_DEF_RANK"], errors="coerce").apply(def_bucket_from_rank)
    else:
        df["def_bucket"] = "NA"

    print(f"→ Fetching NBA actuals for {date} (sleep={args.sleep}s, window_days={args.window_days})")
    actual_map = fetch_boxscores_for_date(date, sleep_s=args.sleep, window_days=args.window_days)
    print(f"→ Actuals found for players: {len(actual_map)}")

    df["actual"] = np.nan
    df["void_reason"] = ""
    for i in range(len(df)):
        a, vr = compute_actual_from_row(df.iloc[i], actual_map)
        df.iat[i, df.columns.get_loc("actual")] = a
        df.iat[i, df.columns.get_loc("void_reason")] = vr

    if "bet_direction" not in df.columns or df["bet_direction"].isna().all():
        if "edge" in df.columns:
            df["bet_direction"] = np.where(pd.to_numeric(df["edge"], errors="coerce") >= 0, "OVER", "UNDER")
        else:
            df["bet_direction"] = "NA"

    df["result"] = "VOID"
    df["result_sign"] = 0
    df["dir_ok"] = np.nan

    has_math = df["actual"].notna() & df["line"].notna() & (df["void_reason"].astype(str).str.strip() == "")
    over = df["bet_direction"].astype(str).str.upper().eq("OVER")
    under = df["bet_direction"].astype(str).str.upper().eq("UNDER")

    push = has_math & (np.isclose(df["actual"], df["line"], atol=1e-9))
    df.loc[push, "result"] = "PUSH"

    hit_over = has_math & over & (df["actual"] > df["line"])
    miss_over = has_math & over & (df["actual"] < df["line"])
    df.loc[hit_over, "result"] = "HIT"
    df.loc[miss_over, "result"] = "MISS"

    hit_under = has_math & under & (df["actual"] < df["line"])
    miss_under = has_math & under & (df["actual"] > df["line"])
    df.loc[hit_under, "result"] = "HIT"
    df.loc[miss_under, "result"] = "MISS"

    df.loc[df["result"].eq("HIT"), "result_sign"] = 1
    df.loc[df["result"].eq("MISS"), "result_sign"] = -1
    df.loc[df["result"].eq("PUSH"), "result_sign"] = 0

    df["dir_ok"] = np.where(df["result"].eq("HIT"), 1,
                            np.where(df["result"].eq("MISS"), 0, np.nan))

    graded = df.copy()

    summary_main = summarize(graded, ["pick_type_key", "tier", "bet_direction"])
    by_minutes = summarize(
        graded,
        [
            "minutes_tier" if "minutes_tier" in graded.columns else "minutes_bucket",
            "tier",
            "bet_direction",
        ],
    )
    by_def = summarize(graded, ["DEF_TIER" if "DEF_TIER" in graded.columns else "def_bucket", "tier", "bet_direction"])
    by_edge = summarize(graded, ["abs_edge_bucket", "tier", "bet_direction"])

    # ----------------------------
    # NEW summaries requested:
    # 1) OVER vs UNDER only
    # 2) By prop type
    # 3) By last-5 directional hits (OVER uses last5_over, UNDER uses last5_under)
    # ----------------------------
    by_dir_only = summarize(graded, ["bet_direction"])

    # By prop type (normalized)
    if "prop_norm" not in graded.columns:
        graded["prop_norm"] = "NA"
    by_prop_type = summarize(graded, ["prop_norm", "bet_direction"])

    # Last-5 directional hits bucket
    if "last5_over" in graded.columns or "last5_under" in graded.columns:
        o = pd.to_numeric(graded.get("last5_over", np.nan), errors="coerce")
        u = pd.to_numeric(graded.get("last5_under", np.nan), errors="coerce")
        is_over = graded["bet_direction"].astype(str).str.upper().eq("OVER")
        is_under = graded["bet_direction"].astype(str).str.upper().eq("UNDER")
        graded["last5_hits_dir"] = np.where(is_over, o, np.where(is_under, u, np.nan))
        graded["last5_hits_dir_bucket"] = graded["last5_hits_dir"].apply(
            lambda v: "NA" if pd.isna(v) else str(int(float(v)))
        )
    else:
        graded["last5_hits_dir"] = np.nan
        graded["last5_hits_dir_bucket"] = "NA"

    by_last5_dir_hits = summarize(graded, ["bet_direction", "last5_hits_dir_bucket"])


    with pd.ExcelWriter(out_xlsx, engine="openpyxl") as writer:
        graded.to_excel(writer, index=False, sheet_name="GRADED")
        summary_main.to_excel(writer, index=False, sheet_name="SUMMARY")
        by_minutes.to_excel(writer, index=False, sheet_name="BY_MINUTES")
        by_def.to_excel(writer, index=False, sheet_name="BY_DEF")
        by_edge.to_excel(writer, index=False, sheet_name="BY_EDGE")

        # NEW sheets
        by_dir_only.to_excel(writer, index=False, sheet_name="BY_DIR")
        by_prop_type.to_excel(writer, index=False, sheet_name="BY_PROP")
        by_last5_dir_hits.to_excel(writer, index=False, sheet_name="BY_LAST5")


    graded.to_csv(out_csv, index=False)

    print(f"\n✅ Saved → {out_xlsx}")
    print(f"✅ Saved → {out_csv}")
    print("\nResult counts:")
    print(graded["result"].value_counts(dropna=False).to_string())


if __name__ == "__main__":
    main()
