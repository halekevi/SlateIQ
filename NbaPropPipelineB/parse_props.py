import argparse
import requests
import pandas as pd
import os


def fetch_board(url: str):
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept": "application/json",
        "Origin": "https://app.prizepicks.com",
        "Referer": "https://app.prizepicks.com/",
    }

    r = requests.get(url, headers=headers, timeout=30)
    r.raise_for_status()
    return r.json()


def parse_board(data: dict):
    players = {}
    games = {}

    # ---------------- BUILD LOOKUPS ---------------- #

    for inc in data.get("included", []):
        t = inc.get("type")
        iid = inc.get("id")
        attr = inc.get("attributes", {})

        if t == "new_player":
            players[iid] = {
                "player": attr.get("name"),
                "team": attr.get("team"),
                "pos": attr.get("position"),
            }

        elif t == "game":
            games[iid] = attr

    # ---------------- PARSE PROJECTIONS ---------------- #

    rows = []

    for proj in data.get("data", []):
        if proj.get("type") != "projection":
            continue

        attr = proj.get("attributes", {})
        rel = proj.get("relationships", {})

        line = attr.get("line_score")
        prop = attr.get("stat_type")
        odds_type = attr.get("odds_type", "standard")

        # --- player ---
        player_rel = rel.get("new_player", {}).get("data", {})
        pid = player_rel.get("id")
        pinfo = players.get(pid, {})

        player = pinfo.get("player")
        team = pinfo.get("team")
        pos = pinfo.get("pos")

        # --- game / opponent ---
        game_rel = rel.get("game", {}).get("data", {})
        gid = game_rel.get("id")
        ginfo = games.get(gid, {})

        opp = None
        meta = ginfo.get("metadata", {})
        teams = meta.get("game_info", {}).get("teams", {})

        if teams and team:
            home = teams.get("home", {}).get("abbreviation")
            away = teams.get("away", {}).get("abbreviation")

            if home == team:
                opp = away
            else:
                opp = home

        # --- safety ---
        if not player or not team or not prop or line is None:
            continue

        row = {
            "player": player,
            "pos": pos,
            "team": team,
            "opp_team": opp,
            "prop_type": prop,
            "line": line,
            "pick_type": odds_type.title(),
        }

        rows.append(row)

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df.dropna(subset=["player", "line", "team"])

    return df


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True, help="PrizePicks DevTools Request URL")
    ap.add_argument("--output", default=None, help="Optional output CSV name")
    args = ap.parse_args()

    print("→ Fetching PrizePicks board...")
    data = fetch_board(args.url)

    print("→ Parsing projections...")
    df = parse_board(data)

    print(f"✅ Rows parsed: {len(df)}")

    # ---------------- AUTO FILENAME ---------------- #

    script_name = os.path.splitext(os.path.basename(__file__))[0]
    default_output = f"{script_name}.csv"
    output_path = args.output or default_output

    df.to_csv(output_path, index=False)
    print(f"✅ Saved → {output_path}")


if __name__ == "__main__":
    main()
