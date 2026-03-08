"""
py -3 debug_soccer_actuals.py --date 2026-03-06
Dumps ALL stat names available from the rosters endpoint
"""
import requests, argparse

HEADERS = {"User-Agent": "Mozilla/5.0"}
SUMMARY = "https://site.api.espn.com/apis/site/v2/sports/soccer/{league}/summary?event={event_id}"

ap = argparse.ArgumentParser()
ap.add_argument("--date", required=True)
args = ap.parse_args()

url = SUMMARY.format(league="esp.1", event_id="748411")
box = requests.get(url, headers=HEADERS, timeout=20).json()

all_stat_names = set()
for roster_block in box.get("rosters", []):
    team = roster_block.get("team", {}).get("abbreviation", "?")
    players = roster_block.get("roster", roster_block.get("entries", roster_block.get("athletes", [])))
    print(f"\nTeam: {team} — {len(players)} players")
    for p in players[:3]:  # first 3 players per team
        athlete = p.get("athlete", {})
        name = athlete.get("displayName", "?")
        stats = p.get("stats", [])
        stat_dict = {s["name"]: s["value"] for s in stats if isinstance(s, dict) and "name" in s}
        all_stat_names.update(stat_dict.keys())
        print(f"  {name}: {stat_dict}")

print(f"\n\nALL STAT NAMES AVAILABLE:")
for n in sorted(all_stat_names):
    print(f"  {n}")
