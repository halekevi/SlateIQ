#!/usr/bin/env python3
"""
defense_db.py  — shared helper for reading/writing defense ratings to slateiq_ref.db

Used by:
  - defense_report.py (NBA/WNBA)    → write after generating CSV
  - nhl_defense_report.py            → write after generating CSV
  - soccer_defense_report.py         → write after generating CSV
  - step3_attach_defense.py          → read instead of --defense CSV
  - step3_attach_defense_nhl.py      → read instead of live API call
  - step3_attach_defense_soccer.py   → read instead of --defense CSV

Usage pattern (defense reports):
    from defense_db import write_defense_to_db
    write_defense_to_db(df, sport="soccer")

Usage pattern (step3):
    from defense_db import load_defense_from_db
    d = load_defense_from_db(sport="soccer")   # returns DataFrame or None
    if d is None:
        # fall back to CSV
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

# ── DB path resolution ────────────────────────────────────────────────────────
# Walks up from this file's location to find data/cache/slateiq_ref.db
def _find_db() -> Path:
    here = Path(__file__).resolve().parent
    for _ in range(6):
        candidate = here / "data" / "cache" / "slateiq_ref.db"
        if candidate.exists():
            return candidate
        here = here.parent
    # Return default even if not found yet — init_db will create it
    return Path(__file__).resolve().parent.parent / "data" / "cache" / "slateiq_ref.db"

DB_PATH = _find_db()

# ── Schema (mirrors build_boxscore_ref.py) ────────────────────────────────────
CREATE_DEFENSE = """
CREATE TABLE IF NOT EXISTS defense (
    sport            TEXT NOT NULL,
    team             TEXT NOT NULL,
    TEAM_NAME        TEXT,
    OVERALL_DEF_RANK REAL,
    OPP_PPG          TEXT,
    opp_gaa          REAL,
    opp_saa          REAL,
    opp_pk_pct       REAL,
    opp_gf_per_game  REAL,
    opp_sf_per_game  REAL,
    opp_pp_pct       REAL,
    opp_wins         REAL,
    opp_gp           REAL,
    def_rank         REAL,
    def_tier         TEXT,
    pp_name          TEXT,
    league           TEXT,
    extra_json       TEXT,
    updated_at       TEXT,
    PRIMARY KEY (sport, team)
);
"""


def _open_db(db_path: Optional[Path] = None) -> sqlite3.Connection:
    p = db_path or DB_PATH
    p.parent.mkdir(parents=True, exist_ok=True)
    con = sqlite3.connect(p)
    con.execute("PRAGMA journal_mode=WAL;")
    con.execute(CREATE_DEFENSE)
    con.commit()
    return con


def _float_or_none(v):
    try:
        f = float(v)
        return None if f != f else f
    except (TypeError, ValueError):
        return None


def _str_or_none(v):
    if v is None:
        return None
    s = str(v).strip()
    return s if s and s.lower() not in ("nan", "none", "") else None


# ── KNOWN COLUMNS (go into fixed schema slots) ────────────────────────────────
_KNOWN = {
    "TEAM_NAME", "OVERALL_DEF_RANK", "OPP_PPG",
    "opp_gaa", "opp_saa", "opp_pk_pct", "opp_gf_per_game",
    "opp_sf_per_game", "opp_pp_pct", "opp_wins", "opp_gp",
    "def_rank", "def_tier", "DEF_TIER",
    "pp_name", "league",
}

# ── Team key candidates (in priority order) ───────────────────────────────────
_TEAM_KEYS = ["pp_name", "team", "TEAM_ABBREVIATION", "team_abbr", "abbr", "TEAM_ABBR", "sr_name", "school", "team_name"]


def write_defense_to_db(
    df: pd.DataFrame,
    sport: str,
    db_path: Optional[Path] = None,
) -> int:
    """
    Write a defense report DataFrame into the defense table.
    sport: 'nba' | 'cbb' | 'nhl' | 'soccer' | 'wnba'
    Returns number of rows upserted.
    """
    con = _open_db(db_path)
    ts = datetime.now(timezone.utc).isoformat()

    # Find which column holds the team key
    team_key = next((c for c in _TEAM_KEYS if c in df.columns), None)
    if not team_key:
        print(f"  ⚠️  defense_db.write: no team key in columns {list(df.columns)}")
        con.close()
        return 0

    count = 0
    for _, row in df.iterrows():
        team = str(row[team_key]).strip().upper()
        if not team or team in ("NAN", "NONE", ""):
            continue

        # Anything not in known schema goes into extra_json
        extra = {
            k: v for k, v in row.items()
            if k not in _KNOWN and k != team_key and pd.notna(v)
        }

        con.execute("""
            INSERT OR REPLACE INTO defense
              (sport, team, TEAM_NAME, OVERALL_DEF_RANK, OPP_PPG,
               opp_gaa, opp_saa, opp_pk_pct, opp_gf_per_game,
               opp_sf_per_game, opp_pp_pct, opp_wins, opp_gp,
               def_rank, def_tier, pp_name, league,
               extra_json, updated_at)
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, (
            sport, team,
            _str_or_none(row.get("TEAM_NAME")),
            _float_or_none(row.get("OVERALL_DEF_RANK")),
            _str_or_none(row.get("OPP_PPG")),
            _float_or_none(row.get("opp_gaa")),
            _float_or_none(row.get("opp_saa")),
            _float_or_none(row.get("opp_pk_pct")),
            _float_or_none(row.get("opp_gf_per_game")),
            _float_or_none(row.get("opp_sf_per_game")),
            _float_or_none(row.get("opp_pp_pct")),
            _float_or_none(row.get("opp_wins")),
            _float_or_none(row.get("opp_gp")),
            _float_or_none(row.get("def_rank")),
            _str_or_none(row.get("def_tier") or row.get("DEF_TIER")),
            _str_or_none(row.get("pp_name")),
            _str_or_none(row.get("league")),
            json.dumps(extra) if extra else None,
            ts,
        ))
        count += 1

    con.commit()
    con.close()
    print(f"  ✅ defense_db: {count} {sport} teams written to DB")
    return count


def load_defense_from_db(
    sport: str,
    db_path: Optional[Path] = None,
    min_rows: int = 5,
) -> Optional[pd.DataFrame]:
    """
    Load defense ratings for a sport from the DB.
    Returns a DataFrame ready for step3 merges, or None if insufficient data.

    The returned DataFrame has a normalized 'team' column (uppercase) that
    step3 can merge on. For soccer it also has 'pp_name' (uppercase).
    """
    try:
        con = _open_db(db_path)
        rows = con.execute(
            "SELECT * FROM defense WHERE sport = ? ORDER BY OVERALL_DEF_RANK ASC",
            (sport,)
        ).fetchall()
        if not rows:
            con.close()
            return None

        cols = [d[0] for d in con.execute(
            "SELECT * FROM defense LIMIT 0"
        ).description]
        con.close()

        df = pd.DataFrame(rows, columns=cols)
        if len(df) < min_rows:
            return None

        # Expand extra_json columns back into DataFrame
        def _expand(j):
            try:
                return json.loads(j) if j else {}
            except Exception:
                return {}

        extra_df = pd.DataFrame(df["extra_json"].apply(_expand).tolist(), index=df.index)
        df = pd.concat([df.drop(columns=["extra_json", "sport"]), extra_df], axis=1)

        # Normalize team key to uppercase for merges
        df["team"] = df["team"].astype(str).str.strip().str.upper()
        if "pp_name" in df.columns:
            df["pp_name"] = df["pp_name"].astype(str).str.strip().str.upper()

        return df

    except Exception as e:
        print(f"  ⚠️  defense_db.load ({sport}): {e}")
        return None


def defense_freshness(sport: str, db_path: Optional[Path] = None) -> Optional[str]:
    """Returns the most recent updated_at timestamp for a sport, or None."""
    try:
        con = _open_db(db_path)
        row = con.execute(
            "SELECT MAX(updated_at) FROM defense WHERE sport = ?", (sport,)
        ).fetchone()
        con.close()
        return row[0] if row else None
    except Exception:
        return None
