import argparse
import pandas as pd
import requests
import time

TEAM_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/teams"
SEARCH_URL = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/athletes"
SLEEP_SECONDS = 0.2

def pick_col(df, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None

def load_espn_teams():
    resp = requests.get(TEAM_URL, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    teams = {}
    for entry in data.get("sports", [])[0].get("leagues", [])[0].get("teams", []):
        t = entry.get("team", {})
        name = (t.get("displayName") or "").strip().lower()
        abbrev = (t.get("abbreviation") or "").strip().lower()
        tid = t.get("id")
        if name and tid:
            teams[name] = tid
        if abbrev and tid:
            teams[abbrev] = tid
    return teams

def normalize_team(s: str) -> str:
    if pd.isna(s):
        return ""
    return str(s).strip().lower()

def find_athlete_id(player_name: str, team_id: str | None):
    if not player_name or pd.isna(player_name):
        return None
    params = {"search": str(player_name).strip()}
    try:
        r = requests.get(SEARCH_URL, params=params, timeout=20)
        r.raise_for_status()
        js = r.json()
        for it in js.get("items", []):
            athlete = it.get("athlete", {})
            aid = athlete.get("id")
            # If ESPN returns team info, prefer matching team_id (when available)
            if team_id:
                teams = athlete.get("teams", [])
                for t in teams:
                    if str(t.get("id")) == str(team_id):
                        return aid
            # fallback: first hit
            if aid:
                return aid
    except Exception:
        return None
    return None

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", required=True)
    ap.add_argument("--output", required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input)

    player_col = pick_col(df, ["player_name", "Player", "player", "name"])
    team_col   = pick_col(df, ["pp_team", "Team", "team", "TEAM"])
    opp_col    = pick_col(df, ["pp_opp_team", "Opp", "opp", "OPP"])

    if not player_col:
        raise SystemExit(f"Missing player column. Found columns: {list(df.columns)}")
    if not team_col:
        # Still write outputs with status so you can debug downstream
        df["team_id"] = None
        df["espn_athlete_id"] = None
        df["attach_status"] = "NO_TEAM_COL"
        df.to_csv(args.output, index=False)
        print("Saved but NO_TEAM_COL — fix normalize step to include Team/pp_team.")
        return

    teams = load_espn_teams()

    team_ids = []
    statuses = []
    for t in df[team_col].fillna("").astype(str):
        key = normalize_team(t)
        tid = teams.get(key)
        team_ids.append(tid)
        statuses.append("OK" if tid else "NO_TEAM_MATCH")

    df["team_id"] = team_ids
    df["attach_status"] = statuses

    athlete_ids = []
    for i, row in df.iterrows():
        tid = row.get("team_id")
        pid = find_athlete_id(row[player_col], tid)
        athlete_ids.append(pid)
        time.sleep(SLEEP_SECONDS)

    df["espn_athlete_id"] = athlete_ids

    df.to_csv(args.output, index=False)
    print(f"✅ Saved → {args.output} | rows={len(df)}")
    print(df["attach_status"].value_counts(dropna=False).head(10))

if __name__ == "__main__":
    main()