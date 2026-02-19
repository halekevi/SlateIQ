#!/usr/bin/env python3
import argparse
import time
import unicodedata
from typing import Dict, Optional, Tuple, List

import pandas as pd
from nba_api.stats.static import players
from nba_api.stats.endpoints import playergamelog, commonplayerinfo


STAT_MAP = {
    "pts": "PTS",
    "reb": "REB",
    "ast": "AST",
    "pra": None,
    "fantasy": None,
    "fga": "FGA",
    "fgm": "FGM",
    "fg3a": "FG3A",
    "fg3m": "FG3M",
    "fta": "FTA",
    "ftm": "FTM",
    "stocks": None,
    "tov": "TOV",
    "min": "MIN",
}


def _fix_mojibake(s: str) -> str:
    """
    Tries to repair common UTF-8/Latin-1 mojibake like 'DÃ«min' -> 'Dëmin'.
    Safe: if it can't decode, returns original.
    """
    s = (s or "").strip()
    if not s:
        return s
    try:
        repaired = s.encode("latin1").decode("utf-8")
        if repaired and repaired != s:
            return repaired
    except Exception:
        pass
    return s


def _norm_name(s: str) -> str:
    s = _fix_mojibake(s)
    s = unicodedata.normalize("NFKC", s)
    return " ".join(s.split()).strip()


def find_player_id(name: str) -> Optional[int]:
    name = _norm_name(name)
    try:
        m = players.find_players_by_full_name(name)
        if m:
            return m[0]["id"]
    except Exception:
        pass
    return None


def resolve_player_name(player_id: int, timeout: int) -> str:
    try:
        ep = commonplayerinfo.CommonPlayerInfo(player_id=player_id, timeout=timeout)
        df = ep.get_data_frames()[0]
        return str(df.iloc[0].get("DISPLAY_FIRST_LAST", "")).strip()
    except Exception:
        return ""


def get_logs(pid: int, season: str, timeout: int) -> pd.DataFrame:
    gl = playergamelog.PlayerGameLog(player_id=pid, season=season, timeout=timeout)
    gdf = gl.get_data_frames()[0]

    if gdf is None or gdf.empty:
        return pd.DataFrame()

    gdf.columns = [c.upper() for c in gdf.columns]
    return gdf


def safe(df: pd.DataFrame, c: str) -> pd.Series:
    if df is None or df.empty or c not in df.columns:
        return pd.Series(dtype=float)
    return pd.to_numeric(df[c], errors="coerce").fillna(0)


def split_combo_player(name: str) -> Optional[Tuple[str, str]]:
    """
    Detect PrizePicks combo name format: 'Player A + Player B'
    Returns (a, b) or None.
    """
    if not name:
        return None
    if "+" not in name:
        return None
    parts = [p.strip() for p in name.split("+")]
    parts = [p for p in parts if p]
    if len(parts) != 2:
        return None
    return (_norm_name(parts[0]), _norm_name(parts[1]))


def compute_vectors_for_player(gdf: pd.DataFrame, prefix: str, col: Optional[str]) -> Tuple[pd.Series, pd.Series, pd.Series]:
    """
    ORIGINAL LOGIC preserved:
      - g5 = head(5), g10 = head(10)
      - PRA = PTS+REB+AST
      - stocks = STL+BLK
      - fantasy formula unchanged
    """
    g5 = gdf.head(5)
    g10 = gdf.head(10)

    if prefix == "pra":
        v5 = safe(g5, "PTS") + safe(g5, "REB") + safe(g5, "AST")
        v10 = safe(g10, "PTS") + safe(g10, "REB") + safe(g10, "AST")
        vs = safe(gdf, "PTS") + safe(gdf, "REB") + safe(gdf, "AST")

    elif prefix == "stocks":
        v5 = safe(g5, "STL") + safe(g5, "BLK")
        v10 = safe(g10, "STL") + safe(g10, "BLK")
        vs = safe(gdf, "STL") + safe(gdf, "BLK")

    elif prefix == "fantasy":
        pts5, reb5, ast5 = safe(g5, "PTS"), safe(g5, "REB"), safe(g5, "AST")
        st5, tov5 = safe(g5, "STL") + safe(g5, "BLK"), safe(g5, "TOV")

        pts10, reb10, ast10 = safe(g10, "PTS"), safe(g10, "REB"), safe(g10, "AST")
        st10, tov10 = safe(g10, "STL") + safe(g10, "BLK"), safe(g10, "TOV")

        ptss, rebs, asts = safe(gdf, "PTS"), safe(gdf, "REB"), safe(gdf, "AST")
        sts, tovs = safe(gdf, "STL") + safe(gdf, "BLK"), safe(gdf, "TOV")

        v5 = pts5 + 1.2 * reb5 + 1.5 * ast5 + 3 * st5 - tov5
        v10 = pts10 + 1.2 * reb10 + 1.5 * ast10 + 3 * st10 - tov10
        vs = ptss + 1.2 * rebs + 1.5 * asts + 3 * sts - tovs

    else:
        v5 = safe(g5, col)
        v10 = safe(g10, col)
        vs = safe(gdf, col)

    return v5, v10, vs


def add_stats_into_row(r: pd.Series, v5: pd.Series, v10: pd.Series, vs: pd.Series, prefix: str) -> pd.Series:
    r[f"{prefix}_last5_avg"] = round(float(v5.mean()) if len(v5) else 0.0, 2)
    r[f"{prefix}_last10_avg"] = round(float(v10.mean()) if len(v10) else 0.0, 2)
    r[f"{prefix}_season_avg"] = round(float(vs.mean()) if len(vs) else 0.0, 2)

    # ORIGINAL behavior: write g1..g5 from v5 list
    for i, v in enumerate(v5.tolist(), 1):
        r[f"{prefix}_g{i}"] = v
    return r


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    ap.add_argument("--season", default="2025-26")
    ap.add_argument("--timeout", type=int, default=30)
    ap.add_argument("--lastn", type=int, default=10, help="compat flag")
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    pid_cache: Dict[str, Optional[int]] = {}
    name_cache: Dict[int, str] = {}
    gamelog_cache: Dict[int, pd.DataFrame] = {}

    out_rows = []

    for _, r0 in df.iterrows():
        r = r0.copy()
        raw_name = str(r.get("player", "")).strip()
        name = _norm_name(raw_name)

        combo = split_combo_player(name)

        # ---------------------------
        # COMBO PLAYER SUPPORT (NEW)
        # ---------------------------
        if combo:
            a, b = combo

            # resolve pids (cached)
            if a not in pid_cache:
                pid_cache[a] = find_player_id(a)
            if b not in pid_cache:
                pid_cache[b] = find_player_id(b)

            pid_a = pid_cache[a]
            pid_b = pid_cache[b]

            # store something useful without breaking existing schema
            r["nba_player_id"] = None
            r["nba_player_ids"] = f"{pid_a or ''}+{pid_b or ''}".strip("+")

            # If either PID missing, keep row but no stats (same skip behavior as original)
            if not pid_a or not pid_b:
                out_rows.append(r)
                continue

            # fetch logs (cached) - keep same sleep behavior per player fetch
            for pid in [pid_a, pid_b]:
                if pid not in gamelog_cache:
                    try:
                        gamelog_cache[pid] = get_logs(pid, args.season, args.timeout)
                        time.sleep(0.35)
                    except Exception:
                        gamelog_cache[pid] = pd.DataFrame()

            gdf_a = gamelog_cache.get(pid_a, pd.DataFrame())
            gdf_b = gamelog_cache.get(pid_b, pd.DataFrame())

            # compute each stat vector and SUM them
            for prefix, col in STAT_MAP.items():
                v5a, v10a, vsa = compute_vectors_for_player(gdf_a, prefix, col)
                v5b, v10b, vsb = compute_vectors_for_player(gdf_b, prefix, col)

                # align indexes to allow safe addition even if lengths differ
                v5 = v5a.reset_index(drop=True).add(v5b.reset_index(drop=True), fill_value=0)
                v10 = v10a.reset_index(drop=True).add(v10b.reset_index(drop=True), fill_value=0)
                vs = vsa.reset_index(drop=True).add(vsb.reset_index(drop=True), fill_value=0)

                r = add_stats_into_row(r, v5, v10, vs, prefix)

            out_rows.append(r)
            continue

        # ---------------------------
        # ORIGINAL SINGLE-PLAYER LOGIC
        # ---------------------------
        if name not in pid_cache:
            pid_cache[name] = find_player_id(name)

        pid = pid_cache[name]
        r["nba_player_id"] = pid

        if pid:
            if pid not in name_cache:
                name_cache[pid] = resolve_player_name(pid, args.timeout)
            if name_cache[pid]:
                r["player"] = name_cache[pid]

        if not pid:
            out_rows.append(r)
            continue

        if pid not in gamelog_cache:
            try:
                gamelog_cache[pid] = get_logs(pid, args.season, args.timeout)
                time.sleep(0.35)
            except Exception:
                gamelog_cache[pid] = pd.DataFrame()

        gdf = gamelog_cache[pid]
        g5 = gdf.head(5)
        g10 = gdf.head(10)

        for prefix, col in STAT_MAP.items():
            if prefix == "pra":
                v5 = safe(g5, "PTS") + safe(g5, "REB") + safe(g5, "AST")
                v10 = safe(g10, "PTS") + safe(g10, "REB") + safe(g10, "AST")
                vs = safe(gdf, "PTS") + safe(gdf, "REB") + safe(gdf, "AST")

            elif prefix == "stocks":
                v5 = safe(g5, "STL") + safe(g5, "BLK")
                v10 = safe(g10, "STL") + safe(g10, "BLK")
                vs = safe(gdf, "STL") + safe(gdf, "BLK")

            elif prefix == "fantasy":
                pts5, reb5, ast5 = safe(g5, "PTS"), safe(g5, "REB"), safe(g5, "AST")
                st5, tov5 = safe(g5, "STL") + safe(g5, "BLK"), safe(g5, "TOV")

                pts10, reb10, ast10 = safe(g10, "PTS"), safe(g10, "REB"), safe(g10, "AST")
                st10, tov10 = safe(g10, "STL") + safe(g10, "BLK"), safe(g10, "TOV")

                ptss, rebs, asts = safe(gdf, "PTS"), safe(gdf, "REB"), safe(gdf, "AST")
                sts, tovs = safe(gdf, "STL") + safe(gdf, "BLK"), safe(gdf, "TOV")

                v5 = pts5 + 1.2 * reb5 + 1.5 * ast5 + 3 * st5 - tov5
                v10 = pts10 + 1.2 * reb10 + 1.5 * ast10 + 3 * st10 - tov10
                vs = ptss + 1.2 * rebs + 1.5 * asts + 3 * sts - tovs

            else:
                v5 = safe(g5, col)
                v10 = safe(g10, col)
                vs = safe(gdf, col)

            r[f"{prefix}_last5_avg"] = round(v5.mean(), 2)
            r[f"{prefix}_last10_avg"] = round(v10.mean(), 2)
            r[f"{prefix}_season_avg"] = round(vs.mean(), 2)

            for i, v in enumerate(v5.tolist(), 1):
                r[f"{prefix}_g{i}"] = v

        out_rows.append(r)

    out = pd.DataFrame(out_rows)

    # ---- DERIVED FG2 ---- (ORIGINAL)
    for w in ["last5", "last10", "season"]:
        if f"fga_{w}_avg" in out and f"fg3a_{w}_avg" in out:
            out[f"fg2a_{w}_avg"] = (out[f"fga_{w}_avg"] - out[f"fg3a_{w}_avg"]).clip(lower=0)

        if f"fgm_{w}_avg" in out and f"fg3m_{w}_avg" in out:
            out[f"fg2m_{w}_avg"] = (out[f"fgm_{w}_avg"] - out[f"fg3m_{w}_avg"]).clip(lower=0)

    for i in range(1, 6):
        if f"fga_g{i}" in out and f"fg3a_g{i}" in out:
            out[f"fg2a_g{i}"] = (out[f"fga_g{i}"] - out[f"fg3a_g{i}"]).clip(lower=0)

        if f"fgm_g{i}" in out and f"fg3m_g{i}" in out:
            out[f"fg2m_g{i}"] = (out[f"fgm_g{i}"] - out[f"fg3m_g{i}"]).clip(lower=0)

    out.to_csv(args.output, index=False)
    print(f"Saved: {args.output} ({len(out)} rows)")


if __name__ == "__main__":
    main()
