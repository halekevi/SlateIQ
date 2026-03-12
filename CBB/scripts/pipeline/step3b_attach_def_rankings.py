#!/usr/bin/env python3
import argparse
import pandas as pd

# Mapping from PrizePicks team abbreviations -> sr_name in cbb_def_rankings.csv
ABBR_TO_SR = {
    # ── Power conferences ─────────────────────────────────────────────
    'ALA':  'Alabama',           'ARK':  'Arkansas',
    'BC':   'Boston College',    'BUT':  'Butler',
    'CAL':  'California',        'COLO': 'Colorado',
    'CONN': 'Connecticut',       'CREI': 'Creighton',
    'DEP':  'DePaul',            'FLA':  'Florida',
    'GC':   'Grand Canyon',      'GMU':  'George Mason',
    'GONZ': 'Gonzaga',           'IOWA': 'Iowa',
    'JOES': "Saint Joseph's",    'KSU':  'Kansas State',
    'LSU':  'Louisiana State',   'MD':   'Maryland',
    'MISS': 'Mississippi',       'MSST': 'Mississippi State',
    'MSU':  'Michigan State',    'NEB':  'Nebraska',
    'ORE':  'Oregon',            'ORST': 'Oregon State',
    'OSU':  'Ohio State',        'PEPP': 'Pepperdine',
    'PITT': 'Pittsburgh',        'PORT': 'Portland',
    'PROV': 'Providence',        'PUR':  'Purdue',
    'SCU':  'Santa Clara',       'SDSU': 'San Diego State',
    'SEA':  'Seattle',           'SJU':  "St. John's (NY)",
    'SMC':  "Saint Mary's (CA)", 'SMU':  'Southern Methodist',
    'STAN': 'Stanford',          'TEX':  'Texas',
    'TXAM': "Texas A&M",         'UGA':  'Georgia',
    'UNLV': 'Nevada-Las Vegas',  'USD':  'San Diego',
    'USU':  'Utah State',        'VAN':  'Vanderbilt',
    'VILL': 'Villanova',         'WAKE': 'Wake Forest',
    'WIS':  'Wisconsin',         'XAV':  'Xavier',
    # ── Short aliases ─────────────────────────────────────────────────
    'UK':   'Kentucky',          'UNC':  'North Carolina',
    'KU':   'Kansas',            'IU':   'Indiana',
    'OU':   'Oklahoma',          'UVA':  'Virginia',
    'USC':  'Southern California','UCLA': 'UCLA',
    'DUKE': 'Duke',              'SYR':  'Syracuse',
    'MARQ': 'Marquette',         'NOVA': 'Villanova',
    'UT':   'Utah',              'MICH': 'Michigan',
    'PSU':  'Penn State',        'MSS':  'Mississippi State',
    'RUTG': 'Rutgers',           'NW':   'Northwestern',
    'MN':   'Minnesota',         'ILL':  'Illinois',
    # ── Previously missing (caused 259 miss rows) ─────────────────────
    'FAU':  'Florida Atlantic',  'WICH': 'Wichita State',
    'TEM':  'Temple',            'MEM':  'Memphis',
    'BRY':  'Bryant',            'UMBC': 'Maryland-Baltimore County',
    # ── Additional common PrizePicks abbrs ────────────────────────────
    'AFA':  'Air Force',         'AKR':  'Akron',
    'APP':  'Appalachian State', 'ARIZ': 'Arizona',
    'ARST': 'Arizona State',     'ASU':  'Arizona State',
    'AUB':  'Auburn',            'BALL': 'Ball State',
    'BAY':  'Baylor',            'BELM': 'Belmont',
    'BGSU': 'Bowling Green',     'BRAD': 'Bradley',
    'BYU':  'Brigham Young',     'BUFF': 'Buffalo',
    'CHAR': 'Charlotte',         'CIN':  'Cincinnati',
    'CLEM': 'Clemson',           'CLT':  'Charlotte',
    'COLST':'Colorado State',    'DAV':  'Davidson',
    'DAY':  'Dayton',            'DRK':  'Drake',
    'DRX':  'Drexel',            'DUQ':  'Duquesne',
    'ECU':  'East Carolina',     'ETSU': 'East Tennessee State',
    'FLA':  'Florida',           'FLST': 'Florida State',
    'FOR':  'Fordham',           'FRES': 'Fresno State',
    'FUR':  'Furman',            'GTWN': 'Georgetown',
    'GASO': 'Georgia Southern',  'GAST': 'Georgia State',
    'GT':   'Georgia Tech',      'HAW':  'Hawaii',
    'HOU':  'Houston',           'IDHO': 'Idaho',
    'ILST': 'Illinois State',    'INST': 'Indiana State',
    'IONA': 'Iona',              'IAST': 'Iowa State',
    'JKST': 'Jacksonville State','JMU':  'James Madison',
    'KENT': 'Kent State',        'LA':   'Louisiana',
    'LBST': 'Long Beach State',  'LIB':  'Liberty',
    'LOU':  'Louisville',        'LOY':  'Loyola (IL)',
    'LMU':  'Loyola Marymount',  'MRST': 'Marist',
    'MRSH': 'Marshall',          'MASS': 'Massachusetts',
    'MTSU': 'Middle Tennessee',  'MIZ':  'Missouri',
    'MIST': 'Missouri State',    'MON':  'Montana',
    'MOST': 'Montana State',     'MUR':  'Murray State',
    'NCST': 'NC State',          'NAU':  'Northern Arizona',
    'NEV':  'Nevada',            'NH':   'New Hampshire',
    'NM':   'New Mexico',        'NMST': 'New Mexico State',
    'NIU':  'Northern Illinois', 'NIU':  'Northern Illinois',
    'ND':   'Notre Dame',        'OAK':  'Oakland',
    'OHIO': 'Ohio',              'OKST': 'Oklahoma State',
    'ODU':  'Old Dominion',      'ORL':  'Oral Roberts',
    'PAC':  'Pacific',           'PENN': 'Pennsylvania',
    'RICE': 'Rice',              'RICH': 'Richmond',
    'RMR':  'Robert Morris',     'SAC':  'Sacramento State',
    'SAML': 'Sam Houston',       'SAMF': 'Samford',
    'SFU':  'San Francisco',     'SJST': 'San Jose State',
    'HALL': 'Seton Hall',        'SIEN': 'Siena',
    'SAL':  'South Alabama',     'SC':   'South Carolina',
    'SDAK': 'South Dakota',      'SDST': 'South Dakota State',
    'USF':  'South Florida',     'SOU':  'Southern',
    'SIU':  'Southern Illinois', 'STBN': 'St. Bonaventure',
    'STTH': 'St. Thomas',        'SFA':  'Stephen F. Austin',
    'STET': 'Stetson',           'TCU':  'TCU',
    'TENN': 'Tennessee',         'TNST': 'Tennessee State',
    'TNTC': 'Tennessee Tech',    'TLDO': 'Toledo',
    'TOWN': 'Towson',            'TROY': 'Troy',
    'TUL':  'Tulane',            'TLSA': 'Tulsa',
    'UAB':  'UAB',               'UCF':  'UCF',
    'UCSD': 'UC San Diego',      'UNO':  'New Orleans',
    'UTEP': 'UTEP',              'UTSA': 'UTSA',
    'UTAH': 'Utah',              'UTV':  'Utah Valley',
    'VCU':  'Virginia Commonwealth','VT': 'Virginia Tech',
    'WAG':  'Wagner',            'WASH': 'Washington',
    'WAST': 'Washington State',  'WEB':  'Weber State',
    'WVU':  'West Virginia',     'WKU':  'Western Kentucky',
    'WMU':  'Western Michigan',  'WYO':  'Wyoming',
    'YALE': 'Yale',
}


def norm_key(x) -> str:
    if pd.isna(x):
        return ""
    return str(x).strip().upper()


def rank_to_tier(rank) -> str:
    """
    Derive a defensive tier from a numeric rank.
    CBB has ~360 teams; lower rank = better defense (rank 1 = best).
    Buckets mirror the NBA pipeline tiers: Elite / Above Avg / Avg / Weak.
    """
    try:
        r = float(rank)
    except (TypeError, ValueError):
        return ""
    if r <= 72:
        return "Elite"
    elif r <= 144:
        return "Above Avg"
    elif r <= 252:
        return "Avg"
    else:
        return "Weak"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",   required=True)
    ap.add_argument("--defense", default="", help="Path to CBB defense rankings CSV (optional if DB is populated)")
    ap.add_argument("--output",  required=True)
    args = ap.parse_args()

    df = pd.read_csv(args.input, dtype=str).fillna("")

    # ── Load defense: DB first, CSV fallback ─────────────────────────────────
    import sys as _sys
    from pathlib import Path as _Path
    def_df = None
    try:
        _here = _Path(__file__).resolve().parent
        for _ in range(6):
            if (_here / "scripts" / "defense_db.py").exists():
                _sys.path.insert(0, str(_here / "scripts"))
                break
            _here = _here.parent
        from defense_db import load_defense_from_db, defense_freshness
        db_df = load_defense_from_db("cbb")
        if db_df is not None and len(db_df) >= 20:
            fresh = defense_freshness("cbb")
            print(f"→ CBB defense loaded from DB ({len(db_df)} teams, updated {fresh})")
            def_df = db_df
        else:
            print("→ CBB defense DB empty — falling back to CSV")
    except Exception as _e:
        print(f"→ defense_db unavailable ({_e}) — falling back to CSV")

    if def_df is None:
        if not args.defense:
            raise SystemExit("❌ No CBB defense data in DB and --defense not provided")
        print(f"→ Loading defense CSV: {args.defense}")
        def_df = pd.read_csv(args.defense, dtype=str).fillna("")

    # Detect columns in defense file
    rank_col = next((c for c in ["overall_rank","OVERALL_DEF_RANK","def_rank","rank"] if c in def_df.columns), None)
    ppg_col  = next((c for c in ["opp_ppg","def_ppg","ppg","opp_def_ppg"]            if c in def_df.columns), None)
    tier_col = next((c for c in ["def_tier","tier","opp_def_tier"]                   if c in def_df.columns), None)
    name_col = next((c for c in ["sr_name","team","school","team_name"]              if c in def_df.columns), None)

    if not name_col:
        raise SystemExit("Defense file has no recognizable team name column.")

    # Build lookup: sr_name (uppercase) -> payload
    by_sr = {}
    for _, r in def_df.iterrows():
        key = norm_key(r[name_col])
        # handle HTML entities
        key = key.replace("&AMP;", "&")
        by_sr[key] = {
            "rank": r[rank_col] if rank_col else None,
            "ppg":  r[ppg_col]  if ppg_col  else None,
            "tier": r[tier_col] if tier_col else None,
        }

    # Also normalise ABBR_TO_SR values to uppercase for lookup
    abbr_map = {k: v.upper().replace("&AMP;", "&") for k, v in ABBR_TO_SR.items()}

    opp_ranks, opp_ppg, opp_tiers = [], [], []
    misses = 0

    opp_col = "opp_team_abbr" if "opp_team_abbr" in df.columns else None

    for _, row in df.iterrows():
        abbr = norm_key(row.get(opp_col, "")) if opp_col else ""
        sr   = abbr_map.get(abbr, "")
        payload = by_sr.get(sr) if sr else None

        # fallback: try matching pp_opp_team as full name directly
        if payload is None:
            for fc in ["pp_opp_team", "opp_team", "opponent"]:
                if fc in df.columns:
                    fn = norm_key(row.get(fc, "")).replace("&AMP;", "&")
                    payload = by_sr.get(fn)
                    if payload:
                        break

        if payload is None:
            misses += 1
            opp_ranks.append(None)
            opp_ppg.append(None)
            opp_tiers.append(None)
        else:
            rank_val = payload["rank"]
            tier_val = payload["tier"]
            # ── FIX: if the defense file has no tier column, derive it from rank ──
            if not tier_val:
                tier_val = rank_to_tier(rank_val)
            opp_ranks.append(rank_val)
            opp_ppg.append(payload["ppg"])
            opp_tiers.append(tier_val)

    df["opp_def_rank"]     = opp_ranks
    df["opp_def_ppg"]      = opp_ppg
    df["opp_def_tier"]     = opp_tiers
    df["def_tier"]         = opp_tiers
    df["OVERALL_DEF_RANK"] = opp_ranks

    df.to_csv(args.output, index=False)
    print(f"✅ Defense attached. Output={args.output} | rows={len(df)} | missing_def_rows={misses}")


if __name__ == "__main__":
    main()
