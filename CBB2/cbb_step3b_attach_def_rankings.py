#!/usr/bin/env python3
import argparse
import pandas as pd

def norm_key(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def build_def_lookup(def_df: pd.DataFrame):
    # Try to discover columns that look like:
    # - team name: "team", "school", "team_name"
    # - abbreviation: "abbr", "team_abbr", "TEAM_ABBR"
    # - rank: "rank", "def_rank", "OVERALL_DEF_RANK"
    # - ppg allowed: "ppg", "def_ppg", "opp_ppg"
    # - tier: "tier", "def_tier", "opp_def_tier"
    name_col = pick_col(def_df, ["team", "school", "team_name", "Team", "School"])
    abbr_col = pick_col(def_df, ["abbr", "team_abbr", "TEAM_ABBR", "Abbr", "TEAM"])
    rank_col = pick_col(def_df, ["OVERALL_DEF_RANK", "def_rank", "rank", "DEF_RANK", "Overall Rank"])
    ppg_col  = pick_col(def_df, ["opp_def_ppg", "def_ppg", "ppg", "PPG", "points_allowed", "Points Allowed"])
    tier_col = pick_col(def_df, ["def_tier", "tier", "DEF_TIER", "Def Tier", "opp_def_tier"])

    # Normalize helper keys
    if abbr_col:
        def_df["_abbr_key"] = def_df[abbr_col].map(norm_key)
    else:
        def_df["_abbr_key"] = ""

    if name_col:
        def_df["_name_key"] = def_df[name_col].map(norm_key)
    else:
        def_df["_name_key"] = ""

    # Build dict lookups
    by_abbr = {}
    by_name = {}

    for _, r in def_df.iterrows():
        payload = {
            "rank": r[rank_col] if rank_col else None,
            "ppg":  r[ppg_col] if ppg_col else None,
            "tier": r[tier_col] if tier_col else None,
        }
        ak = r.get("_abbr_key", "")
        nk = r.get("_name_key", "")

        if ak and ak not in by_abbr:
            by_abbr[ak] = payload
        if nk and nk not in by_name:
            by_name[nk] = payload

    return by_abbr, by_name

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True, help="Input CBB slate CSV (expects opp_team_abbr and/or pp_opp_team)")
    ap.add_argument("--defense", required=True, help="Defense rankings CSV")
    ap.add_argument("--output", required=True, help="Output CSV with defense columns attached")
    args = ap.parse_args()

    df = pd.read_csv(args.input)
    def_df = pd.read_csv(args.defense)

    # Normalize keys from slate
    if "opp_team_abbr" in df.columns:
        df["_opp_abbr_key"] = df["opp_team_abbr"].map(norm_key)
    else:
        df["_opp_abbr_key"] = ""

    # Some slates have opponent name in pp_opp_team / opp_team / opponent
    opp_name_source = None
    for c in ["pp_opp_team", "opp_team", "opponent", "opp"]:
        if c in df.columns:
            opp_name_source = c
            break

    if opp_name_source:
        df["_opp_name_key"] = df[opp_name_source].map(norm_key)
    else:
        df["_opp_name_key"] = ""

    by_abbr, by_name = build_def_lookup(def_df)

    opp_ranks = []
    opp_ppg   = []
    opp_tiers = []
    misses = 0

    for _, r in df.iterrows():
        ak = r["_opp_abbr_key"]
        nk = r["_opp_name_key"]

        payload = None
        if ak and ak in by_abbr:
            payload = by_abbr[ak]
        elif nk and nk in by_name:
            payload = by_name[nk]

        if payload is None:
            misses += 1
            opp_ranks.append(None)
            opp_ppg.append(None)
            opp_tiers.append(None)
        else:
            opp_ranks.append(payload.get("rank"))
            opp_ppg.append(payload.get("ppg"))
            opp_tiers.append(payload.get("tier"))

    # Attach columns your downstream expects
    df["opp_def_rank"]     = opp_ranks
    df["opp_def_ppg"]      = opp_ppg
    df["opp_def_tier"]     = opp_tiers
    df["def_tier"]         = opp_tiers
    df["OVERALL_DEF_RANK"] = opp_ranks

    # Cleanup
    df.drop(columns=[c for c in ["_opp_abbr_key", "_opp_name_key"] if c in df.columns], inplace=True)

    df.to_csv(args.output, index=False)
    print(f"✅ Defense attached. Output={args.output} | rows={len(df)} | missing_def_rows={misses}")

if __name__ == "__main__":
    main()