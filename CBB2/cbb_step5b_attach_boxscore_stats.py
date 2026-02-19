#!/usr/bin/env python3
"""
cbb_step5b_attach_boxscore_stats.py  (upgraded)
------------------------------------------------
Mirrors NBA step4 logic exactly.

Improvements over original:
- stat_season_avg added (all games in window)
- stat_last10_avg already present, now also stat_last5_avg
- line_hit_rate_over_ou_5  (last 5 vs line, excl push)
- line_hit_rate_over_ou_10 (last 10 vs line, excl push)  ← NEW
- line_hit_rate_over_5 / line_hit_rate_under_5
- MIN averages: min_last5_avg, min_season_avg
- Matches by espn_athlete_id first, then player_norm fallback

Input : step2_normalized_cbb.csv  (or step5_with_espn_ids.csv)
Output: step5b_with_stats_cbb.csv
"""

from __future__ import annotations

import argparse
import datetime as dt
import re
import time
from typing import Dict, List, Tuple, Optional

import pandas as pd
import requests

HEADERS = {"User-Agent": "Mozilla/5.0", "Accept": "application/json, text/plain, */*"}
ESPN_SCOREBOARD_URL = "https://site.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/scoreboard"
ESPN_SUMMARY_URL    = "https://site.web.api.espn.com/apis/site/v2/sports/basketball/mens-college-basketball/summary"


def norm(s: str) -> str:
    s = (s or "").lower().strip()
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    return re.sub(r"\s+", " ", s).strip()


def request_json(url, params=None, max_tries=5, backoff=1.4, sleep=0.0):
    for i in range(1, max_tries + 1):
        try:
            if sleep: time.sleep(sleep)
            r = requests.get(url, headers=HEADERS, params=params, timeout=30)
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff ** (i - 1))
                continue
            r.raise_for_status()
            return r.json()
        except Exception:
            time.sleep(backoff ** (i - 1))
    return None


def date_range(end_date: dt.date, days_back: int) -> List[str]:
    return [(end_date - dt.timedelta(days=i)).strftime("%Y%m%d") for i in range(days_back + 1)]


def pull_scoreboard(d: str) -> dict:
    return request_json(ESPN_SCOREBOARD_URL,
                        params={"dates": d, "groups": "50", "limit": "500"},
                        sleep=0.10) or {}


def extract_events(sb: dict) -> List[Tuple[str, str, str]]:
    out = []
    for ev in sb.get("events", []) or []:
        eid = str(ev.get("id", "")).strip()
        comps = ev.get("competitions", []) or []
        if not eid or not comps: continue
        tids = [str((c.get("team") or {}).get("id", "")).strip()
                for c in comps[0].get("competitors", []) or []]
        tids = [t for t in tids if t]
        if len(tids) == 2:
            out.append((eid, tids[0], tids[1]))
    return out


def pull_summary(eid: str) -> dict:
    return request_json(ESPN_SUMMARY_URL, params={"event": eid}, sleep=0.08) or {}


def parse_min(x) -> float:
    try:
        s = str(x).strip()
        if s in ("", "--", "nan", "None"): return 0.0
        if ":" in s:
            mm, ss = s.split(":", 1)
            return float(mm) + float(ss) / 60.0
        return float(s)
    except Exception:
        return 0.0


def parse_players(summary: dict) -> List[dict]:
    box    = summary.get("boxscore", {}) or {}
    blocks = box.get("players", []) or []
    rows   = []

    for tb in blocks:
        team_id = str((tb.get("team") or {}).get("id", "")).strip()
        if not team_id: continue

        labels = athletes = None
        for g in tb.get("statistics", []) or []:
            if isinstance(g, dict) and "labels" in g and "athletes" in g:
                labels   = g.get("labels", [])
                athletes = g.get("athletes", [])
                if any(str(x).upper() == "PTS" for x in labels): break

        if not labels or not athletes: continue
        ul = [str(x).upper() for x in labels]

        def idx(lbl):
            return ul.index(lbl) if lbl in ul else None

        i = {k: idx(k) for k in ("MIN","PTS","REB","AST","STL","BLK")}
        i["TO"]  = idx("TO") if "TO" in ul else idx("TOV")
        i["3PM"] = idx("3PM") if "3PM" in ul else (idx("FG3M") if "FG3M" in ul else None)

        for a in athletes:
            ath = a.get("athlete", {}) or {}
            aid = str(ath.get("id", "")).strip()
            pn  = norm(ath.get("displayName") or ath.get("fullName") or "")
            st  = a.get("stats", []) or []

            def getf(key):
                ii = i.get(key)
                if ii is None or ii >= len(st): return 0.0
                try: return float(st[ii])
                except Exception: return 0.0

            min_val = parse_min(st[i["MIN"]]) if i["MIN"] is not None and i["MIN"] < len(st) else 0.0

            if pn:
                rows.append({"team_id": team_id, "player_norm": pn,
                             "espn_athlete_id": aid, "MIN": min_val,
                             "PTS": getf("PTS"), "REB": getf("REB"),
                             "AST": getf("AST"), "STL": getf("STL"),
                             "BLK": getf("BLK"), "TO":  getf("TO"),
                             "3PM": getf("3PM")})
    return rows


def fantasy(r: dict) -> float:
    return (r.get("PTS",0) + 1.2*r.get("REB",0) + 1.5*r.get("AST",0)
            + 3*r.get("STL",0) + 3*r.get("BLK",0) - r.get("TO",0))


def prop_value(prop_norm: str, r: dict) -> Optional[float]:
    p = str(prop_norm or "").strip().lower()
    m = {"pts": r.get("PTS"), "reb": r.get("REB"), "ast": r.get("AST"),
         "stl": r.get("STL"), "blk": r.get("BLK"), "tov": r.get("TO"), "3pm": r.get("3PM"),
         "stocks": r.get("STL",0)+r.get("BLK",0),
         "pra": r.get("PTS",0)+r.get("REB",0)+r.get("AST",0),
         "pr":  r.get("PTS",0)+r.get("REB",0),
         "pa":  r.get("PTS",0)+r.get("AST",0),
         "ra":  r.get("REB",0)+r.get("AST",0),
         "fantasy": fantasy(r)}
    # also handle full text fallbacks
    if "fantasy" in p: return fantasy(r)
    if "points"  in p: return r.get("PTS")
    if "rebounds" in p: return r.get("REB")
    if "assists"  in p: return r.get("AST")
    if "steals"   in p: return r.get("STL")
    if "blocks"   in p: return r.get("BLK")
    if "turnovers" in p or p in ("to","tov"): return r.get("TO")
    if "3-pt" in p or "3pt" in p or "3pm" in p or "threes" in p: return r.get("3PM")
    return m.get(p)


def hit_rates(vals: List[float], line: float, n: int):
    """Compute hit rate over/under/push for last n games, excl push."""
    sub = vals[:n]
    over = sum(1 for v in sub if v > line)
    under = sum(1 for v in sub if v < line)
    push  = sum(1 for v in sub if v == line)
    denom_ou = len(sub) - push
    hr_over_ou  = over  / denom_ou if denom_ou > 0 else None
    hr_under_ou = under / denom_ou if denom_ou > 0 else None
    hr_over     = over  / len(sub) if sub else None
    hr_under    = under / len(sub) if sub else None
    return over, under, push, hr_over, hr_under, hr_over_ou, hr_under_ou


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input",  required=True)
    ap.add_argument("--output", default="step5b_with_stats_cbb.csv")
    ap.add_argument("--days",   type=int, default=45)
    ap.add_argument("--n",      type=int, default=10)
    args = ap.parse_args()

    print("→ Loading:", args.input)
    df = pd.read_csv(args.input, dtype=str).fillna("")
    df["line"] = pd.to_numeric(df["line"], errors="coerce")
    if "player_norm" not in df.columns:
        df["player_norm"] = df["player"].astype(str).apply(norm)
    if "team_id" not in df.columns:
        df["team_id"] = ""
    if "espn_athlete_id" not in df.columns:
        df["espn_athlete_id"] = ""

    # use prop_norm if available, else prop_type
    prop_col = "prop_norm" if "prop_norm" in df.columns else "prop_type"

    slate_ids = {x for x in df["team_id"].astype(str).str.strip() if x}
    print("→ Slate teams:", len(slate_ids))

    # Pull boxscores
    hist_aid:  Dict[Tuple[str,str], List[dict]] = {}
    hist_name: Dict[Tuple[str,str], List[dict]] = {}
    seen = set()

    for d in date_range(dt.date.today(), args.days):
        print("→ Pulling:", d)
        for eid, t1, t2 in extract_events(pull_scoreboard(d)):
            if eid in seen: continue
            if slate_ids and t1 not in slate_ids and t2 not in slate_ids: continue
            seen.add(eid)
            for rr in parse_players(pull_summary(eid)):
                tid, pn, aid = rr["team_id"], rr["player_norm"], rr["espn_athlete_id"]
                if tid and aid:  hist_aid.setdefault((tid, aid), []).append(rr)
                if tid and pn:   hist_name.setdefault((tid, pn), []).append(rr)

    print(f"→ Player histories — by ID: {len(hist_aid)} | by name: {len(hist_name)}")

    out_rows, stat_status = [], []

    for _, row in df.iterrows():
        tid  = str(row.get("team_id",       "")).strip()
        pn   = str(row.get("player_norm",   "")).strip()
        aid  = str(row.get("espn_athlete_id","")).strip()
        prop = str(row.get(prop_col,        "")).strip()
        line = row.get("line", None)

        if not tid:
            stat_status.append("NO_TEAM_ID"); out_rows.append({}); continue

        games = (hist_aid.get((tid, aid), []) if aid else []) or hist_name.get((tid, pn), [])
        if not games:
            stat_status.append("NO_BOX_HISTORY"); out_rows.append({}); continue

        played = [g for g in games if float(g.get("MIN", 0) or 0) > 0]
        vals = [float(v) for g in played
                if (v := prop_value(prop, g)) is not None]

        if not vals:
            stat_status.append("UNSUPPORTED_PROP"); out_rows.append({}); continue
        if len(vals) < 5:
            stat_status.append("INSUFFICIENT_GAMES"); out_rows.append({"games_used": len(vals)}); continue

        vals = vals[:args.n]
        last5  = vals[:5]
        last10 = vals[:10]

        o = {"games_used": len(vals)}
        for k in range(1, args.n + 1):
            o[f"stat_g{k}"] = vals[k-1] if k-1 < len(vals) else ""

        o["stat_last5_avg"]  = round(sum(last5)  / len(last5),  3) if last5  else ""
        o["stat_last10_avg"] = round(sum(last10) / len(last10), 3) if last10 else ""
        o["stat_season_avg"] = round(sum(vals)   / len(vals),   3) if vals   else ""

        # minutes averages
        min_vals = [float(g.get("MIN", 0) or 0) for g in played]
        min5 = min_vals[:5]
        o["min_last5_avg"]   = round(sum(min5)    / len(min5),    1) if min5    else ""
        o["min_season_avg"]  = round(sum(min_vals) / len(min_vals), 1) if min_vals else ""

        # hit rates vs line
        if pd.notna(line):
            ln = float(line)

            over5, under5, push5, hr_ov5, hr_un5, hr_ov_ou5, hr_un_ou5 = hit_rates(vals, ln, 5)
            o["line_hits_over_5"]         = over5
            o["line_hits_under_5"]        = under5
            o["line_hits_push_5"]         = push5
            o["line_hit_rate_over_5"]     = round(hr_ov5,    3) if hr_ov5    is not None else ""
            o["line_hit_rate_under_5"]    = round(hr_un5,    3) if hr_un5    is not None else ""
            o["line_hit_rate_over_ou_5"]  = round(hr_ov_ou5, 3) if hr_ov_ou5 is not None else ""
            o["line_hit_rate_under_ou_5"] = round(hr_un_ou5, 3) if hr_un_ou5 is not None else ""

            over10, under10, push10, hr_ov10, hr_un10, hr_ov_ou10, hr_un_ou10 = hit_rates(vals, ln, 10)
            o["line_hits_over_10"]         = over10
            o["line_hits_under_10"]        = under10
            o["line_hits_push_10"]         = push10
            o["line_hit_rate_over_10"]     = round(hr_ov10,    3) if hr_ov10    is not None else ""
            o["line_hit_rate_under_10"]    = round(hr_un10,    3) if hr_un10    is not None else ""
            o["line_hit_rate_over_ou_10"]  = round(hr_ov_ou10, 3) if hr_ov_ou10 is not None else ""
            o["line_hit_rate_under_ou_10"] = round(hr_un_ou10, 3) if hr_un_ou10 is not None else ""

            o["model_dir_5"] = "OVER" if over5 >= under5 else "UNDER"

        stat_status.append("OK")
        out_rows.append(o)

    stats_df = pd.DataFrame(out_rows).fillna("")
    df["stat_status"] = stat_status
    out = pd.concat([df.reset_index(drop=True), stats_df], axis=1)
    out.to_csv(args.output, index=False)

    print(f"✅ Saved → {args.output} | rows={len(out)}")
    print("stat_status breakdown:")
    print(out["stat_status"].value_counts().to_string())


if __name__ == "__main__":
    main()
