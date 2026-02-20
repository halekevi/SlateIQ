#!/usr/bin/env python3
"""
fetch_actuals.py — pulls NBA/CBB box scores from ESPN and outputs
actuals CSV for the slate grader.

Usage:
  py -3.14 fetch_actuals.py --sport NBA --date 2026-02-20
  py -3.14 fetch_actuals.py --sport CBB --date 2026-02-20
  py -3.14 fetch_actuals.py --sport NBA   # defaults to yesterday
"""

import argparse
import requests
import pandas as pd
import time
from datetime import date, timedelta

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json",
}

# ── Build all prop rows from a stat_map ──────────────────────────────────────
def parse_stats(player_name, t_abbr, stat_map):
    pts   = stat_map.get('PTS')
    reb   = stat_map.get('REB')
    ast   = stat_map.get('AST')
    blk   = stat_map.get('BLK')
    stl   = stat_map.get('STL')
    tov   = stat_map.get('TO')
    fgm   = stat_map.get('FGM')
    fga   = stat_map.get('FGA')
    fg3m  = stat_map.get('3PM')
    fg3a  = stat_map.get('3PA')
    fg2m  = stat_map.get('2PM')   # may not always exist
    fg2a  = stat_map.get('2PA')
    ftm   = stat_map.get('FTM')
    fta   = stat_map.get('FTA')
    oreb  = stat_map.get('OREB')
    dreb  = stat_map.get('DREB')
    pf    = stat_map.get('PF')
    mins  = stat_map.get('MIN')

    # Derive two-pointers from fg - 3pt if not directly provided
    if fg2m is None and fgm is not None and fg3m is not None:
        fg2m = fgm - fg3m
    if fg2a is None and fga is not None and fg3a is not None:
        fg2a = fga - fg3a

    # Combos
    pra  = pts + reb + ast           if all(x is not None for x in [pts, reb, ast])  else None
    pr   = pts + reb                 if all(x is not None for x in [pts, reb])        else None
    pa   = pts + ast                 if all(x is not None for x in [pts, ast])        else None
    ra   = reb + ast                 if all(x is not None for x in [reb, ast])        else None
    bs   = blk + stl                 if all(x is not None for x in [blk, stl])        else None

    # Fantasy score (PrizePicks DK-style)
    fs = None
    if all(x is not None for x in [pts, reb, ast, stl, blk, tov, fg3m]):
        fs = (pts * 1.0 + reb * 1.25 + ast * 1.5 + stl * 2.0 + blk * 2.0
              - tov * 0.5 + fg3m * 0.5
              + (4.5 if pts >= 10 else 0)
              + (6.0 if reb >= 10 else 0)
              + (6.0 if ast >= 10 else 0))

    # Map exactly to slate prop names
    prop_map = {
        # Basic
        'Points':               pts,
        'Rebounds':             reb,
        'Assists':              ast,
        'Blocked Shots':        blk,
        'Steals':               stl,
        'Turnovers':            tov,

        # Shooting — exact slate names
        'FG Made':              fgm,
        'FG Attempted':         fga,
        '3-PT Made':            fg3m,
        '3-PT Attempted':       fg3a,
        'Two Pointers Made':    fg2m,
        'Two Pointers Attempted': fg2a,
        'Free Throws Made':     ftm,
        'Free Throws Attempted': fta,

        # Rebound splits
        'Offensive Rebounds':   oreb,
        'Defensive Rebounds':   dreb,

        # Misc
        'Personal Fouls':       pf,
        'Fantasy Score':        fs,

        # Combos
        'Pts+Rebs+Asts':        pra,
        'PRA':                  pra,
        'Pts+Rebs':             pr,
        'Pts+Asts':             pa,
        'Rebs+Asts':            ra,
        'Blks+Stls':            bs,
    }

    rows = []
    for prop_type, actual in prop_map.items():
        if actual is not None:
            rows.append({
                'player':    player_name,
                'team':      t_abbr,
                'prop_type': prop_type,
                'actual':    round(float(actual), 1),
            })
    return rows

# ── Parse ESPN box score JSON ─────────────────────────────────────────────────
def parse_boxscore(box):
    rows = []
    for bteam in box.get('boxscore', {}).get('players', []):
        t_abbr = bteam.get('team', {}).get('abbreviation', '')
        for stat_group in bteam.get('statistics', []):
            labels = stat_group.get('labels', [])
            for athlete in stat_group.get('athletes', []):
                player_name = athlete.get('athlete', {}).get('displayName', '')
                stats = athlete.get('stats', [])
                if not stats or all(s in ('--', '', None) for s in stats):
                    continue

                stat_map = {}
                for label, val in zip(labels, stats):
                    try:
                        stat_map[label] = float(val)
                    except (ValueError, TypeError):
                        pass

                # ESPN uses different label names — normalize them
                label_aliases = {
                    '3PM':  ['3PM', 'FG3M', '3FGM'],
                    '3PA':  ['3PA', 'FG3A', '3FGA'],
                    'FGM':  ['FGM'],
                    'FGA':  ['FGA'],
                    'FTM':  ['FTM'],
                    'FTA':  ['FTA'],
                    'PTS':  ['PTS'],
                    'REB':  ['REB', 'TREB'],
                    'AST':  ['AST'],
                    'BLK':  ['BLK'],
                    'STL':  ['STL'],
                    'TO':   ['TO', 'TOV'],
                    'PF':   ['PF', 'FOULS'],
                    'OREB': ['OREB'],
                    'DREB': ['DREB'],
                    'MIN':  ['MIN'],
                }
                normalized = {}
                for canon, aliases in label_aliases.items():
                    for alias in aliases:
                        if alias in stat_map:
                            normalized[canon] = stat_map[alias]
                            break

                if not normalized:
                    continue
                rows.extend(parse_stats(player_name, t_abbr, normalized))
    return rows

# ── Fetch scoreboard + box scores ────────────────────────────────────────────
def fetch_sport(sport_path, date_str):
    date_espn = date_str.replace('-', '')
    url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{sport_path}/scoreboard?dates={date_espn}&limit=200"

    print(f"Fetching scoreboard...")
    try:
        r = requests.get(url, headers=HEADERS, timeout=20)
        r.raise_for_status()
        data = r.json()
    except Exception as e:
        print(f"ERROR: {e}")
        return pd.DataFrame()

    events = data.get('events', [])
    print(f"  Found {len(events)} games")

    all_rows = []
    seen = set()

    for event in events:
        event_id  = event.get('id', '')
        game_name = event.get('shortName', event.get('name', ''))
        if event_id in seen:
            continue
        seen.add(event_id)

        status_type = event.get('status', {}).get('type', {})
        state       = status_type.get('state', '')
        completed   = status_type.get('completed', False)

        if state != 'post' and not completed:
            print(f"  Skipping {game_name} — not final yet (state={state})")
            continue

        print(f"  Grading: {game_name}")
        box_url = f"https://site.api.espn.com/apis/site/v2/sports/basketball/{sport_path}/summary?event={event_id}"
        try:
            br = requests.get(box_url, headers=HEADERS, timeout=20)
            br.raise_for_status()
            box = br.json()
            time.sleep(0.25)
        except Exception as e:
            print(f"    ERROR: {e}")
            continue

        rows = parse_boxscore(box)
        all_rows.extend(rows)
        print(f"    -> {len(rows)} stat rows")

    if not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows)
    df = df.drop_duplicates(subset=['player', 'prop_type'], keep='first')
    print(f"\n  Total: {len(df)} player-prop actuals")
    return df

# ── Main ─────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sport',  default='NBA', choices=['NBA', 'CBB'])
    ap.add_argument('--date',   default='', help='YYYY-MM-DD (default: yesterday)')
    ap.add_argument('--output', default='')
    args = ap.parse_args()

    if not args.date:
        args.date = (date.today() - timedelta(days=1)).strftime('%Y-%m-%d')
    if not args.output:
        args.output = f'actuals_{args.sport.lower()}_{args.date}.csv'

    print(f"\n=== {args.sport} actuals for {args.date} ===\n")

    sport_path = 'nba' if args.sport == 'NBA' else 'mens-college-basketball'
    df = fetch_sport(sport_path, args.date)

    if df.empty:
        print("\nNo actuals fetched — games may not be final yet.")
        print("Try again after all games have finished (usually safe by 1am ET).")
        return

    df.to_csv(args.output, index=False)
    print(f"\nSaved -> {args.output}  ({len(df)} rows)")
    print(f"\nProp types extracted: {sorted(df['prop_type'].unique().tolist())}")
    print(f"\nNext step:")
    if args.sport == 'NBA':
        print(f"  py -3.14 slate_grader.py --sport NBA --slate NbaPropPipelineA\\step8_all_direction_clean.xlsx --actuals {args.output} --output nba_graded_{args.date}.xlsx")
    else:
        print(f"  py -3.14 slate_grader.py --sport CBB --slate CBB2\\step6_ranked_cbb.xlsx --actuals {args.output} --output cbb_graded_{args.date}.xlsx")

if __name__ == '__main__':
    main()
