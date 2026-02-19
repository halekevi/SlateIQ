#!/usr/bin/env python3
"""
step2_attach_picktypes.py (FINAL - PP schema + robust opp_team)

Step2 depends ONLY on Step1:
- Keeps ALL Step1 columns intact
- Adds nba_player_id:
    Singles: "<nba_id>"
    Combos : "<nba_id1>|<nba_id2>" (sorted asc)
- Adds combo helper columns:
    player_1, player_2, team_1, team_2
- Adds opp_team for singles using pp_game_id inference:
    * Primary: build mapping from singles only (ignore team strings like CHA/HOU)
    * Fallback: if only one single team exists in game, infer opponent from combo pairs
- Normalizes pick_type and prop_norm
- Adds id_status

Run:
  py -3.14 step2_attach_picktypes.py --input step1_fetch_prizepicks_api.csv --output step2_attach_picktypes.csv
"""

from __future__ import annotations

import argparse
import re
import unicodedata
from typing import Dict, Optional, Tuple

import pandas as pd
from nba_api.stats.static import players

COMBO_SEP = "|"

# ---------------- NORMALIZERS ---------------- #

def norm_name_strict(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    s = unicodedata.normalize("NFKD", str(s))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = re.sub(r"[^\w\s]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def norm_name_loose(s: str) -> str:
    x = norm_name_strict(s)
    x = re.sub(r"\b(jr|sr|ii|iii|iv|v)\b", "", x)
    x = re.sub(r"\s+", " ", x).strip()
    return x


def norm_pick_type(s: str) -> str:
    if s is None or str(s).strip() == "":
        return "Standard"
    t = str(s).strip().lower()
    if "gob" in t:
        return "Goblin"
    if "dem" in t:
        return "Demon"
    if t in {"standard", "classic", "normal"}:
        return "Standard"
    return str(s).strip().title()


def norm_prop(s: str) -> str:
    if s is None or (isinstance(s, float) and pd.isna(s)):
        return ""
    raw = str(s).lower()
    clean = raw.replace(" ", "").replace("-", "").replace("_", "")

    exact_map = {
        "points": "pts",
        "rebounds": "reb",
        "assists": "ast",
        "blocks": "blk",
        "blockedshots": "blk",
        "steals": "stl",
        "turnovers": "tov",
        "blks+stls": "stocks",
        "fantasyscore": "fantasy",
        "pts+rebs+asts": "pra",
        "points+rebounds+assists": "pra",
        "pts+rebs": "pr",
        "points+rebounds": "pr",
        "pts+asts": "pa",
        "points+assists": "pa",
        "rebs+asts": "ra",
        "rebounds+assists": "ra",
        "fgm": "fgm",
        "fgmade": "fgm",
        "fga": "fga",
        "fgattempted": "fga",
        "3ptfgattempted": "fg3a",
        "3ptfgmade": "fg3m",
        "fg3a": "fg3a",
        "fg3m": "fg3m",
        "2ptfgattempted": "fg2a",
        "2ptfgmade": "fg2m",
        "fg2a": "fg2a",
        "fg2m": "fg2m",
        "fta": "fta",
        "ftattempted": "fta",
        "ftm": "ftm",
        "ftmade": "ftm",
    }
    return exact_map.get(clean, clean)


def detect_combo_player(player_str: str) -> int:
    if player_str is None or (isinstance(player_str, float) and pd.isna(player_str)):
        return 0
    return 1 if "+" in str(player_str) else 0


def split_combo_player(player_str: str) -> Tuple[str, str]:
    s = str(player_str or "")
    parts = [p.strip() for p in s.split("+")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return s.strip(), ""


def split_combo_team(team_str: str) -> Tuple[str, str]:
    s = str(team_str or "")
    parts = [p.strip() for p in s.split("/")]
    if len(parts) >= 2:
        return parts[0], parts[1]
    return s.strip(), ""


# ---------------- OPP TEAM (pp_game_id inference) ---------------- #

def build_opp_team_from_gameid(df: pd.DataFrame) -> pd.Series:
    """
    Build opponent using pp_game_id + team values.
    Robust to combo rows like CHA/HOU by:
      - building map from singles only (no '/')
      - fallback: if only 1 single team exists, infer opponent from combo pairs
      - ignore opponent assignment for combos
    """
    df2 = df.copy()
    df2["pp_game_id"] = df2["pp_game_id"].astype(str).fillna("")
    df2["team"] = df2["team"].astype(str).fillna("")
    df2["is_combo_player"] = pd.to_numeric(df2.get("is_combo_player", 0), errors="coerce").fillna(0).astype(int)

    opp_map: Dict[tuple, str] = {}

    for gid, g in df2.groupby("pp_game_id", dropna=False):
        gid = str(gid)
        if not gid or gid.lower() == "nan":
            continue

        # Primary: singles teams only (exclude combo team strings like CHA/HOU)
        singles = g[(g["is_combo_player"] == 0) & (g["team"].str.strip() != "") & (~g["team"].str.contains("/"))]
        single_teams = list(singles["team"].dropna().unique())

        # Fallback: combo pairs (CHA/HOU -> ("CHA","HOU"))
        combos = g[(g["is_combo_player"] == 1) & (g["team"].str.contains("/"))]
        combo_pairs = []
        for t in combos["team"].dropna().unique():
            parts = [p.strip() for p in str(t).split("/") if p.strip()]
            if len(parts) >= 2:
                combo_pairs.append((parts[0], parts[1]))

        # Case A: clean 2-team game from singles
        if len(single_teams) == 2:
            t1, t2 = single_teams
            opp_map[(gid, t1)] = t2
            opp_map[(gid, t2)] = t1
            continue

        # Case B: only 1 single team seen -> infer from combos containing that team
        if len(single_teams) == 1 and combo_pairs:
            t = single_teams[0]
            candidates = set()
            for a, b in combo_pairs:
                if a == t:
                    candidates.add(b)
                elif b == t:
                    candidates.add(a)
            if len(candidates) == 1:
                other = next(iter(candidates))
                opp_map[(gid, t)] = other

        # Case C: >2 single teams (rare/messy) -> take top 2 most frequent singles
        if len(single_teams) > 2:
            top2 = singles["team"].value_counts().head(2).index.tolist()
            if len(top2) == 2:
                t1, t2 = top2
                opp_map[(gid, t1)] = t2
                opp_map[(gid, t2)] = t1

    # Apply map row-by-row
    out = []
    for _, row in df2.iterrows():
        gid = str(row["pp_game_id"])
        team = str(row["team"]).strip()

        # do not assign opp_team for combo rows or combo team strings
        if int(row["is_combo_player"]) == 1 or "/" in team or team == "":
            out.append("")
            continue

        out.append(opp_map.get((gid, team), ""))

    return pd.Series(out, index=df2.index)


# ---------------- NBA DIRECTORY + RESOLUTION ---------------- #

def build_nba_directory() -> pd.DataFrame:
    nba_players = players.get_players()
    pldf = pd.DataFrame(nba_players)
    if "full_name" not in pldf.columns or "id" not in pldf.columns:
        raise RuntimeError("❌ nba_api players directory missing full_name/id")

    pldf["norm_strict"] = pldf["full_name"].apply(norm_name_strict)
    pldf["norm_loose"] = pldf["full_name"].apply(norm_name_loose)
    # is_active sometimes exists in nba_api; handle if not
    if "is_active" not in pldf.columns:
        pldf["is_active"] = False

    return pldf[["id", "full_name", "is_active", "norm_strict", "norm_loose"]].copy()


def resolve_nba_id_by_name(pldf: pd.DataFrame, name: str) -> Tuple[Optional[int], Optional[str], str]:
    strict = norm_name_strict(name)
    loose = norm_name_loose(name)

    if strict:
        hit = pldf.loc[pldf["norm_strict"] == strict]
        if len(hit) == 1:
            r = hit.iloc[0]
            return int(r["id"]), str(r["full_name"]), "name_strict"

    if loose:
        hit = pldf.loc[pldf["norm_loose"] == loose]
        if len(hit) == 1:
            r = hit.iloc[0]
            return int(r["id"]), str(r["full_name"]), "name_loose"

        if len(hit) > 1:
            active = hit[hit["is_active"] == True]
            if len(active) == 1:
                r = active.iloc[0]
                return int(r["id"]), str(r["full_name"]), "name_loose_active_tiebreak"

    return None, None, "unresolved"


# ---------------- MAIN ---------------- #

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="step1_fetch_prizepicks_api.csv")
    ap.add_argument("--output", default="step2_attach_picktypes.csv")
    args = ap.parse_args()

    print(f"→ Loading Step1: {args.input}")
    df = pd.read_csv(args.input, dtype=str).fillna("")

    # Required for PP schema
    required = ["pp_projection_id", "pp_game_id", "player", "team", "prop_type", "line"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise RuntimeError(f"❌ Step1 missing required columns: {missing}")

    # Optional columns we want to exist
    for c in ["pos", "opp_team", "pick_type", "start_time"]:
        if c not in df.columns:
            df[c] = ""

    # Normalize pick type + prop norm
    df["pick_type"] = df["pick_type"].apply(norm_pick_type)
    df["prop_norm"] = df["prop_type"].apply(norm_prop)

    # Combo marker
    df["is_combo_player"] = df["player"].apply(detect_combo_player).astype(int)

    # Combo helper cols
    for c in ["player_1", "player_2", "team_1", "team_2"]:
        if c not in df.columns:
            df[c] = ""

    # Fill combo helper cols
    combos = df["is_combo_player"] == 1
    if combos.any():
        for idx, row in df.loc[combos, ["player", "team"]].iterrows():
            p1, p2 = split_combo_player(row["player"])
            t1, t2 = split_combo_team(row["team"])
            df.at[idx, "player_1"] = p1
            df.at[idx, "player_2"] = p2
            df.at[idx, "team_1"] = t1
            df.at[idx, "team_2"] = t2

    # Build opp_team (singles only)
    df["opp_team"] = build_opp_team_from_gameid(df)

    # NBA ID resolution
    pldf = build_nba_directory()
    df["nba_player_id"] = ""
    df["id_status"] = "OK"

    # Singles
    singles = df["is_combo_player"] == 0
    for idx, row in df.loc[singles, ["player"]].iterrows():
        pid, _, how = resolve_nba_id_by_name(pldf, row["player"])
        if pid is not None:
            df.at[idx, "nba_player_id"] = str(int(pid))
        else:
            df.at[idx, "nba_player_id"] = ""
            df.at[idx, "id_status"] = "UNRESOLVED_SINGLE"

    # Combos
    if combos.any():
        print(f"→ Processing {int(combos.sum())} combo rows (writing nba_player_id as id1|id2)...")
        for idx, row in df.loc[combos, ["player_1", "player_2"]].iterrows():
            id1, _, _ = resolve_nba_id_by_name(pldf, row["player_1"])
            id2, _, _ = resolve_nba_id_by_name(pldf, row["player_2"])
            if id1 is not None and id2 is not None:
                ids = sorted([int(id1), int(id2)])
                df.at[idx, "nba_player_id"] = f"{ids[0]}{COMBO_SEP}{ids[1]}"
            else:
                df.at[idx, "nba_player_id"] = ""
                df.at[idx, "id_status"] = "UNRESOLVED_COMBO"

    # Output ordering (keep all Step1 columns in middle)
    desired_front = [
        "nba_player_id",
        "player",
        "pos",
        "team",
        "opp_team",
        "line",
        "prop_type",
        "prop_norm",
        "pick_type",
    ]
    front = [c for c in desired_front if c in df.columns]
    tail = ["is_combo_player"]
    middle = [c for c in df.columns if c not in set(front + tail)]
    out = df[front + middle + tail].copy()

    out.to_csv(args.output, index=False, encoding="utf-8")
    print(f"✅ Saved → {args.output} | rows={len(out)}")


if __name__ == "__main__":
    main()
