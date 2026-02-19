import pandas as pd
from nba_api.stats.endpoints import leaguedashteamstats

SEASON = "2025-26"

print("Pulling NBA defense stats...")

df = leaguedashteamstats.LeagueDashTeamStats(
    season=SEASON,
    per_mode_detailed="PerGame",
    measure_type_detailed_defense="Defense",
    timeout=60
).get_data_frames()[0]

print("\nColumns from API:\n")
print(df.columns.tolist())

# --- Use real available columns ---
keep = [
    "TEAM_NAME",
    "DEF_RATING",
    "DREB",
    "DREB_PCT",
    "STL",
    "BLK",
    "OPP_PTS_OFF_TOV",
    "OPP_PTS_2ND_CHANCE",
    "OPP_PTS_FB",
    "OPP_PTS_PAINT",
]

keep = [c for c in keep if c in df.columns]

df = df[keep].copy()

# --- Build ranks (lower DEF_RATING is better) ---
for col in keep:
    if col == "TEAM_NAME":
        continue

    ascending = True
    if col in ["DREB", "DREB_PCT", "STL", "BLK"]:
        ascending = False  # higher is better

    df[f"{col}_RANK"] = df[col].rank(method="min", ascending=ascending)

rank_cols = [c for c in df.columns if c.endswith("_RANK")]

df["OVERALL_DEF_SCORE"] = df[rank_cols].mean(axis=1)
df["OVERALL_DEF_RANK"] = df["OVERALL_DEF_SCORE"].rank(method="min")

df["DEF_TIER"] = pd.qcut(
    df["OVERALL_DEF_SCORE"],
    5,
    labels=["Elite", "Good", "Avg", "Bad", "Target"]
)

df = df.sort_values("OVERALL_DEF_RANK")

df.to_csv("defense_team_summary.csv", index=False)

print("\nSaved defense_team_summary.csv successfully.")


#python build_defense_team_summary.py
