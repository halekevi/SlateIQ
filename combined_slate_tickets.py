#!/usr/bin/env python3
"""
Combined NBA + CBB Slate & Ticket Generator
Merges NBA (step8_all_direction_clean.xlsx) and CBB (step6_ranked_cbb.xlsx ELIGIBLE)
Outputs: combined_slate_tickets_YYYY-MM-DD.xlsx
  Sheets: SUMMARY, Full Slate, NBA Slate, CBB Slate,
          NBA 3/4/5/6-Leg tickets (Goblin/Standard/Demon/Mix),
          CBB 3/4/5/6-Leg tickets, Combined 3/4/5/6-Leg tickets
"""
import pandas as pd
import numpy as np
from openpyxl import Workbook
from openpyxl.styles import Font, PatternFill, Alignment, Border, Side
from openpyxl.utils import get_column_letter
import argparse
import os
from datetime import datetime
from itertools import combinations

# ── Color palette ─────────────────────────────────────────────────────────────
C = {
    'hdr':    '1C1C1C', 'hdr_nba': '1A5276', 'hdr_cbb': '1E8449',
    'hdr_mix':'6C3483', 'hdr_sum': '117A65',
    'hit':    '27AE60', 'miss':    'E74C3C', 'push':    'F39C12',
    'tier_a': 'D5F5E3', 'tier_b':  'D6EAF8', 'tier_c':  'FEF9E7', 'tier_d':  'FDEDEC',
    'goblin': 'E8D5F5', 'demon':   'FDEDEC', 'standard':'F2F3F4',
    'over':   'D6EAF8', 'under':   'FDEBD0',
    'alt':    'F2F3F4', 'white':   'FFFFFF',
    'nba':    'EBF5FB', 'cbb':     'EAFAF1', 'mix':     'F5EEF8',
    'gold':   'F9E79F',
}

PAYOUT = {
    2:  {'power': 3.0,  'flex': 3.0},
    3:  {'power': 4.37, 'flex': 1.73},
    4:  {'power': 10.0, 'flex': 6.0},
    5:  {'power': 20.0, 'flex': 10.0},
    6:  {'power': 40.0, 'flex': 16.0},
}

def side(color='CCCCCC'):
    s = Side(style='thin', color=color)
    return Border(left=s, right=s, top=s, bottom=s)

def hc(ws, r, c, v, bg=None, fc='FFFFFF', bold=True, sz=9, align='center'):
    cell = ws.cell(row=r, column=c, value=v)
    cell.font = Font(bold=bold, color=fc, name='Arial', size=sz)
    if bg: cell.fill = PatternFill('solid', start_color=bg)
    cell.alignment = Alignment(horizontal=align, vertical='center', wrap_text=True)
    cell.border = side()
    return cell

def dc(ws, r, c, v, bg=None, bold=False, sz=9, align='center', fc='000000', fmt=None):
    cell = ws.cell(row=r, column=c, value=v)
    cell.font = Font(bold=bold, name='Arial', size=sz, color=fc)
    cell.fill = PatternFill('solid', start_color=bg or C['white'])
    cell.alignment = Alignment(horizontal=align, vertical='center')
    cell.border = side()
    if fmt: cell.number_format = fmt
    return cell

def sw(ws, widths):
    for ci, w in enumerate(widths, 1):
        ws.column_dimensions[get_column_letter(ci)].width = w

def tier_bg(t):
    return {'A': C['tier_a'], 'B': C['tier_b'], 'C': C['tier_c'], 'D': C['tier_d']}.get(str(t).upper(), C['white'])

def pt_bg(pt):
    return {'Goblin': C['goblin'], 'Demon': C['demon'], 'Standard': C['standard']}.get(pt, C['white'])

def hr_bg(v):
    if v is None or (isinstance(v, float) and np.isnan(v)): return 'DDDDDD'
    if v >= 0.65: return C['hit']
    if v >= 0.50: return C['push']
    return C['miss']

def pct_cell(ws, r, c, val):
    nan = val is None or (isinstance(val, float) and np.isnan(val))
    bg = hr_bg(val) if not nan else 'DDDDDD'
    cell = dc(ws, r, c, val if not nan else '', bg=bg, bold=True)
    if not nan:
        cell.number_format = '0%'
        cell.font = Font(bold=True, name='Arial', size=9, color='FFFFFF')
    return cell

def win_prob(hit_rates, n):
    return float(np.prod([h for h in hit_rates if not np.isnan(h)]))

# ── Load & normalize NBA ───────────────────────────────────────────────────────
def load_nba(path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine='openpyxl')
    sheet = 'ALL' if 'ALL' in xl.sheet_names else xl.sheet_names[0]
    df = pd.read_excel(path, sheet_name=sheet, engine='openpyxl')
    df = df.rename(columns={
        'Player': 'player', 'Prop': 'prop_type', 'Pick Type': 'pick_type',
        'Line': 'line', 'Direction': 'direction', 'Edge': 'edge',
        'Hit Rate (5g)': 'hit_rate', 'Rank Score': 'rank_score', 'Tier': 'tier',
        'Projection': 'projection', 'Team': 'team', 'Opp': 'opp',
        'Game Time': 'game_time', 'Def Tier': 'def_tier', 'Min Tier': 'min_tier',
        'L5 Avg': 'l5_avg', 'Season Avg': 'season_avg',
        'L5 Over': 'l5_over', 'L5 Under': 'l5_under', 'Pos': 'pos',
        'Def Rank': 'def_rank', 'Shot Role': 'shot_role', 'Usage Role': 'usage_role',
        'Void Reason': 'void_reason',
    })
    df['sport'] = 'NBA'
    df['direction'] = df['direction'].str.upper()
    df['tier'] = df['tier'].astype(str).str.upper()
    # drop void rows
    if 'void_reason' in df.columns:
        df = df[df['void_reason'].isna() | (df['void_reason'].astype(str).str.strip() == '')]
    return df

# ── Load & normalize CBB ───────────────────────────────────────────────────────
def load_cbb(path: str) -> pd.DataFrame:
    xl = pd.ExcelFile(path, engine='openpyxl')
    sheet = 'ELIGIBLE' if 'ELIGIBLE' in xl.sheet_names else 'ALL'
    df = pd.read_excel(path, sheet_name=sheet, engine='openpyxl')
    df = df.rename(columns={
        'prop_type': 'prop_type', 'final_bet_direction': 'direction',
        'opp_team_abbr': 'opp',
        'start_time': 'game_time', 'line_hit_rate': 'hit_rate',
        'stat_last5_avg': 'l5_avg', 'stat_season_avg': 'season_avg',
        'line_hits_over_5': 'l5_over', 'line_hits_under_5': 'l5_under',
    })
    if 'direction' not in df.columns and 'bet_direction' in df.columns:
        df['direction'] = df['bet_direction']
    df['sport'] = 'CBB'
    df['direction'] = df['direction'].str.upper()
    df['tier'] = df['tier'].astype(str).str.upper()
    if 'void_reason' in df.columns:
        df = df[df['void_reason'].isna() | (df['void_reason'].astype(str).str.strip() == '')]
    return df

# ── Merge to full slate ────────────────────────────────────────────────────────
def build_combined_slate(nba: pd.DataFrame, cbb: pd.DataFrame) -> pd.DataFrame:
    keep = ['sport','tier','rank_score','player','team','opp','game_time',
            'prop_type','pick_type','line','direction','edge','projection',
            'hit_rate','l5_avg','season_avg','l5_over','l5_under','def_tier']
    def safe_keep(df, cols):
        df = df.loc[:, ~df.columns.duplicated()].copy()
        return df[[c for c in cols if c in df.columns]].copy()
    combined = pd.concat([safe_keep(nba, keep), safe_keep(cbb, keep)], ignore_index=True)
    combined['rank_score'] = pd.to_numeric(combined['rank_score'], errors='coerce')
    combined['hit_rate']   = pd.to_numeric(combined['hit_rate'],   errors='coerce')
    combined['edge']       = pd.to_numeric(combined['edge'],       errors='coerce')
    combined = combined.sort_values('rank_score', ascending=False).reset_index(drop=True)
    return combined

# ── Filter eligible props for tickets ─────────────────────────────────────────
def filter_eligible(df: pd.DataFrame, min_hit_rate=0.0, min_edge=0.0, min_rank=None,
                    tiers=None, pick_types=None) -> pd.DataFrame:
    mask = pd.Series([True] * len(df), index=df.index)
    if min_hit_rate > 0:
        mask &= df['hit_rate'].fillna(0) >= min_hit_rate
    if min_edge > 0:
        mask &= df['edge'].fillna(0) >= min_edge
    if min_rank is not None:
        mask &= df['rank_score'].fillna(-99) >= min_rank
    if tiers:
        mask &= df['tier'].isin([t.upper() for t in tiers])
    if pick_types:
        mask &= df['pick_type'].isin(pick_types)
    return df[mask].copy()

# ── Build tickets ──────────────────────────────────────────────────────────────
def build_tickets(pool: pd.DataFrame, n_legs: int, max_tickets=20, require_mix=False) -> list:
    """Build top tickets of n_legs from pool, sorted by avg rank score.
    If require_mix=True, each ticket must contain at least 1 NBA and 1 CBB leg."""
    pool = pool.copy().reset_index(drop=True)
    tickets = []

    has_sport_col = 'sport' in pool.columns
    sports_available = pool['sport'].unique().tolist() if has_sport_col else []
    can_mix = require_mix and has_sport_col and len(sports_available) >= 2

    eligible = pool.sort_values('rank_score', ascending=False).reset_index(drop=True)

    for _ in range(max_tickets * 5):
        if len(tickets) >= max_tickets: break
        ticket_rows = []
        ticket_players = set()
        sports_in_ticket = set()

        if can_mix:
            # Step 1: seed with best prop from each sport first
            for sport in sports_available:
                sport_pool = eligible[eligible['sport'] == sport]
                for _, row in sport_pool.iterrows():
                    player = row.get('player', '')
                    if player not in ticket_players:
                        ticket_rows.append(row)
                        ticket_players.add(player)
                        sports_in_ticket.add(sport)
                        break
            # Step 2: fill remaining legs from full pool by rank
            for _, row in eligible.iterrows():
                if len(ticket_rows) == n_legs: break
                player = row.get('player', '')
                if player not in ticket_players:
                    ticket_rows.append(row)
                    ticket_players.add(player)
                    sports_in_ticket.add(row.get('sport', ''))
        else:
            for _, row in eligible.iterrows():
                if len(ticket_rows) == n_legs: break
                player = row.get('player', '')
                if player not in ticket_players:
                    ticket_rows.append(row)
                    ticket_players.add(player)

        if len(ticket_rows) == n_legs:
            # Enforce mix requirement
            if can_mix and len(sports_in_ticket) < 2:
                if len(eligible) > 1:
                    eligible = eligible.iloc[1:].reset_index(drop=True)
                continue

            # Sort legs: by sport then rank score so NBA/CBB alternate nicely
            if can_mix:
                ticket_rows = sorted(ticket_rows,
                    key=lambda r: (r.get('sport',''), -r.get('rank_score', 0)))

            key = frozenset(r.get('player','') + '|' + str(r.get('prop_type','')) for r in ticket_rows)
            if key not in [t['key'] for t in tickets]:
                avg_hr = float(np.mean([r.get('hit_rate', 0.5) for r in ticket_rows]))
                avg_rs = float(np.mean([r.get('rank_score', 0) for r in ticket_rows]))
                ep = win_prob([r.get('hit_rate', 0.5) for r in ticket_rows], n_legs)
                pout = PAYOUT.get(n_legs, {'power': 0, 'flex': 0})
                tickets.append({
                    'key': key,
                    'rows': ticket_rows,
                    'avg_hit_rate': avg_hr,
                    'avg_rank_score': avg_rs,
                    'est_win_prob': ep,
                    'power_payout': pout['power'],
                    'flex_payout': pout['flex'],
                    'n_legs': n_legs,
                })

        if len(eligible) > n_legs:
            eligible = eligible.iloc[1:].reset_index(drop=True)
        else:
            break

    tickets.sort(key=lambda x: (-x['avg_rank_score'], -x['avg_hit_rate']))
    return tickets[:max_tickets]

# ── Write slate sheet ──────────────────────────────────────────────────────────
SLATE_COLS = ['sport','tier','rank_score','player','team','opp','prop_type','pick_type',
              'line','direction','edge','projection','hit_rate','l5_avg','season_avg',
              'l5_over','l5_under','def_tier','game_time']
SLATE_WIDTHS = [6,5,10,20,6,6,18,10,6,8,7,10,10,8,10,7,7,10,16]
SLATE_HDRS   = ['Sport','Tier','Rank Score','Player','Team','Opp','Prop','Pick Type',
                'Line','Dir','Edge','Proj','Hit Rate','L5 Avg','Szn Avg',
                'L5 Over','L5 Under','Def Tier','Game Time']

def write_slate_sheet(wb, df, sheet_name, bg_hdr, sport_label=''):
    ws = wb.create_sheet(sheet_name)
    cols = [c for c in SLATE_COLS if c in df.columns]
    hdrs = [SLATE_HDRS[SLATE_COLS.index(c)] for c in cols]
    widths = [SLATE_WIDTHS[SLATE_COLS.index(c)] for c in cols]
    sw(ws, widths)
    ws.row_dimensions[1].height = 22
    for ci, h in enumerate(hdrs, 1):
        hc(ws, 1, ci, h, bg=bg_hdr)
    ws.freeze_panes = 'A2'

    for ri, row in enumerate(df[cols].itertuples(), 2):
        bg = C['alt'] if ri % 2 == 0 else C['white']
        sp = getattr(row, 'sport', '')
        if sp == 'NBA': bg_row = C['nba'] if ri % 2 == 0 else C['white']
        elif sp == 'CBB': bg_row = C['cbb'] if ri % 2 == 0 else C['white']
        else: bg_row = bg

        for ci, col in enumerate(cols, 1):
            val = getattr(row, col, '')
            if val is None or (isinstance(val, float) and np.isnan(val)): val = ''
            if col == 'tier':
                cell = dc(ws, ri, ci, val, bg=tier_bg(val), bold=True, align='center')
            elif col == 'pick_type':
                cell = dc(ws, ri, ci, val, bg=pt_bg(val), align='center')
            elif col == 'hit_rate':
                pct_cell(ws, ri, ci, val if val != '' else np.nan)
                continue
            elif col == 'rank_score':
                dc(ws, ri, ci, round(val, 2) if val != '' else '', bg=bg_row, bold=True, fmt='0.00')
            elif col == 'direction':
                dbg = C['over'] if str(val).upper() == 'OVER' else C['under']
                dc(ws, ri, ci, val, bg=dbg, bold=True)
            elif col == 'sport':
                sbg = C['hdr_nba'] if val == 'NBA' else C['hdr_cbb']
                dc(ws, ri, ci, val, bg=sbg, bold=True, fc='FFFFFF')
            elif col == 'player':
                dc(ws, ri, ci, val, bg=bg_row, align='left', bold=True)
            elif col == 'game_time':
                try:
                    if val and val != '':
                        dt = pd.to_datetime(val)
                        dc(ws, ri, ci, dt.strftime('%m/%d %I:%M%p'), bg=bg_row, align='center')
                    else:
                        dc(ws, ri, ci, '', bg=bg_row)
                except:
                    dc(ws, ri, ci, str(val)[:16], bg=bg_row)
                continue
            else:
                dc(ws, ri, ci, val, bg=bg_row, align='center')

    ws.auto_filter.ref = f"A1:{get_column_letter(len(cols))}1"

# ── Write ticket sheet ─────────────────────────────────────────────────────────
TICKET_COLS = ['#','player','team','opp','prop_type','pick_type','line','direction',
               'edge','hit_rate','l5_avg','season_avg','l5_over','l5_under','rank_score','def_tier','sport']
TICKET_HDRS = ['#','Player','Team','Opp','Prop','Pick Type','Line','Dir',
               'Edge','Hit Rate','L5 Avg','Szn Avg','L5 Over','L5 Under','Rank Score','Def Tier','Sport']
TICKET_W    = [4,20,6,6,18,10,6,6,7,9,8,9,7,8,11,10,6]

def write_ticket_sheet(wb, tickets, sheet_name, bg_hdr, label=''):
    if not tickets:
        return
    ws = wb.create_sheet(sheet_name)
    sw(ws, TICKET_W)
    ws.freeze_panes = 'A2'

    ri = 1
    for ti, ticket in enumerate(tickets, 1):
        n = ticket['n_legs']
        pout = ticket['power_payout']
        fout = ticket['flex_payout']
        cost = round(100 / pout, 0) if pout else 0
        avg_hr = ticket['avg_hit_rate']
        ep = ticket['est_win_prob']
        avg_rs = ticket['avg_rank_score']

        # Ticket header banner
        banner = (f"  Ticket #{ti}  ·  {n}-Leg {label}  ·  "
                  f"Power: {pout}x (${cost:.0f} to win $100)  ·  Flex: {fout}x  ·  "
                  f"Avg Hit Rate: {avg_hr:.0%}  ·  Est Win Prob: {ep:.0%}  ·  "
                  f"Avg Rank Score: {avg_rs:.2f}")
        ws.merge_cells(start_row=ri, start_column=1, end_row=ri, end_column=len(TICKET_COLS))
        hbg = bg_hdr
        hc(ws, ri, 1, banner, bg=hbg, sz=9, align='left')
        ws.row_dimensions[ri].height = 16
        ri += 1

        # Column headers
        for ci, h in enumerate(TICKET_HDRS, 1):
            hc(ws, ri, ci, h, bg=C['hdr'], sz=8)
        ws.row_dimensions[ri].height = 14
        ri += 1

        # Legs
        for leg_i, row in enumerate(ticket['rows'], 1):
            bg = C['alt'] if leg_i % 2 == 0 else C['white']
            sp = row.get('sport', '') if isinstance(row, dict) else getattr(row, 'sport', '')
            if sp == 'NBA': bg = C['nba']
            elif sp == 'CBB': bg = C['cbb']

            def gv(field):
                return row.get(field, '') if isinstance(row, dict) else getattr(row, field, '')

            dc(ws, ri, 1, leg_i, bg=bg, bold=True, align='center')
            dc(ws, ri, 2, gv('player'), bg=bg, align='left', bold=True)
            dc(ws, ri, 3, gv('team'), bg=bg)
            dc(ws, ri, 4, gv('opp'), bg=bg)
            dc(ws, ri, 5, gv('prop_type'), bg=bg, align='left')
            ptv = gv('pick_type')
            dc(ws, ri, 6, ptv, bg=pt_bg(str(ptv)), align='center')
            dc(ws, ri, 7, gv('line'), bg=bg)
            dirv = str(gv('direction')).upper()
            dc(ws, ri, 8, dirv, bg=C['over'] if dirv == 'OVER' else C['under'], bold=True)
            dc(ws, ri, 9, gv('edge'), bg=bg)
            pct_cell(ws, ri, 10, gv('hit_rate') if gv('hit_rate') != '' else np.nan)
            dc(ws, ri, 11, gv('l5_avg'), bg=bg)
            dc(ws, ri, 12, gv('season_avg'), bg=bg)
            dc(ws, ri, 13, gv('l5_over'), bg=bg)
            dc(ws, ri, 14, gv('l5_under'), bg=bg)
            rs = gv('rank_score')
            dc(ws, ri, 15, round(float(rs), 2) if rs != '' and rs is not None else '', bg=bg, bold=True)
            dc(ws, ri, 16, gv('def_tier'), bg=bg)
            sv = gv('sport')
            sbg = C['hdr_nba'] if sv == 'NBA' else (C['hdr_cbb'] if sv == 'CBB' else C['hdr'])
            dc(ws, ri, 17, sv, bg=sbg, bold=True, fc='FFFFFF')
            ws.row_dimensions[ri].height = 14
            ri += 1

        # Spacer
        ws.row_dimensions[ri].height = 6
        ri += 1

# ── Write SUMMARY sheet ───────────────────────────────────────────────────────
def write_summary(wb, nba, cbb, combined, all_ticket_groups, date_str, thresholds):
    ws = wb.create_sheet('SUMMARY', 0)
    sw(ws, [28, 14, 10, 10, 10, 10, 10, 12, 18])
    # Title
    ws.merge_cells('A1:I1')
    c = ws['A1']
    c.value = f"COMBINED NBA + CBB SLATE  |  {date_str}  |  Generated {datetime.now().strftime('%Y-%m-%d %H:%M')}"
    c.font = Font(bold=True, name='Arial', size=13, color='FFFFFF')
    c.fill = PatternFill('solid', start_color=C['hdr'])
    c.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[1].height = 30

    # Thresholds used
    ws.merge_cells('A2:I2')
    c2 = ws['A2']
    c2.value = (f"Filters: Tier {thresholds.get('tiers','ALL')} | "
                f"Min Hit Rate: {thresholds.get('min_hit_rate',0):.0%} | "
                f"Min Edge: {thresholds.get('min_edge',0)} | "
                f"Min Rank Score: {thresholds.get('min_rank','None')} | "
                f"Pick Types: {thresholds.get('pick_types','ALL')}")
    c2.font = Font(bold=False, name='Arial', size=9, color='000000')
    c2.fill = PatternFill('solid', start_color=C['gold'])
    c2.alignment = Alignment(horizontal='center', vertical='center')
    ws.row_dimensions[2].height = 16

    row = 4
    def sec(r, label, bg):
        ws.merge_cells(start_row=r, start_column=1, end_row=r, end_column=9)
        hc(ws, r, 1, label, bg=bg, sz=10, align='left')
        ws.row_dimensions[r].height = 20
        return r + 1

    def stat_row(r, label, total, elig, bg=None):
        bg = bg or (C['alt'] if r % 2 == 0 else C['white'])
        dc(ws, r, 1, label, bg=bg, align='left', bold=True)
        dc(ws, r, 2, total, bg=bg)
        dc(ws, r, 3, elig, bg=bg)
        for ci in range(4, 10): dc(ws, r, ci, '', bg=bg)
        return r + 1

    # Slate overview
    row = sec(row, '📊 SLATE OVERVIEW', C['hdr_sum'])
    for ci, h in enumerate(['Category','Total Props','Eligible','','','','','',''], 1):
        hc(ws, row, ci, h, bg=C['hdr'], sz=8)
    ws.row_dimensions[row].height = 14
    row += 1
    row = stat_row(row, 'NBA Props', len(nba), len(nba[nba['tier'].isin(['A','B'])]), C['nba'])
    row = stat_row(row, 'CBB Props', len(cbb), len(cbb[cbb['tier'].isin(['A','B'])]), C['cbb'])
    row = stat_row(row, 'Combined Slate', len(combined), len(combined[combined['tier'].isin(['A','B'])]))
    row += 1

    # Ticket summary
    row = sec(row, '🎟️ TICKET SUMMARY', C['hdr_mix'])
    for ci, h in enumerate(['Sheet','Legs','Type','# Tickets','Avg Hit Rate','Avg Win Prob','Avg Rank Score','Power Payout','Players'], 1):
        hc(ws, row, ci, h, bg=C['hdr'], sz=8)
    ws.row_dimensions[row].height = 14
    row += 1

    for group_name, tickets, bg_row in all_ticket_groups:
        if not tickets: continue
        avg_hr = np.mean([t['avg_hit_rate'] for t in tickets])
        avg_wp = np.mean([t['est_win_prob'] for t in tickets])
        avg_rs = np.mean([t['avg_rank_score'] for t in tickets])
        n = tickets[0]['n_legs']
        pout = tickets[0]['power_payout']
        bg = bg_row if bg_row else (C['alt'] if row % 2 == 0 else C['white'])
        dc(ws, row, 1, group_name, bg=bg, align='left', bold=True)
        dc(ws, row, 2, n, bg=bg)
        lbl = group_name.split(' ')[0] if group_name else ''
        dc(ws, row, 3, lbl, bg=bg)
        dc(ws, row, 4, len(tickets), bg=bg)
        pct_cell(ws, row, 5, avg_hr)
        pct_cell(ws, row, 6, avg_wp)
        dc(ws, row, 7, round(avg_rs, 2), bg=bg)
        dc(ws, row, 8, f'{pout}x', bg=bg)
        # Sample players from top ticket
        sample = ' | '.join(
            f"{r.get('player','') if isinstance(r,dict) else getattr(r,'player','')}"
            for r in tickets[0]['rows'][:3]
        ) + ('...' if n > 3 else '')
        dc(ws, row, 9, sample, bg=bg, align='left', sz=8)
        row += 1

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--nba',     required=True, help='NBA step8_all_direction_clean.xlsx')
    ap.add_argument('--cbb',     required=True, help='CBB step6_ranked_cbb.xlsx')
    ap.add_argument('--output',  default='')
    ap.add_argument('--date',    default=datetime.now().strftime('%Y-%m-%d'))
    ap.add_argument('--tiers',   default='A,B', help='Comma-separated tiers e.g. A,B')
    ap.add_argument('--min-hit-rate', type=float, default=0.0,  dest='min_hit_rate')
    ap.add_argument('--min-edge',     type=float, default=0.0,  dest='min_edge')
    ap.add_argument('--min-rank',     type=float, default=None, dest='min_rank')
    ap.add_argument('--pick-types',   default='Goblin,Standard,Demon', dest='pick_types')
    ap.add_argument('--max-tickets',  type=int,   default=20,   dest='max_tickets')
    args = ap.parse_args()

    if not args.output:
        args.output = f'combined_slate_tickets_{args.date}.xlsx'

    tiers = [t.strip() for t in args.tiers.split(',')]
    pick_types = [p.strip() for p in args.pick_types.split(',')]
    thresholds = {
        'tiers': args.tiers, 'min_hit_rate': args.min_hit_rate,
        'min_edge': args.min_edge, 'min_rank': args.min_rank, 'pick_types': args.pick_types
    }

    print(f'Loading NBA slate from {args.nba}...')
    nba = load_nba(args.nba)
    print(f'  {len(nba)} NBA props loaded')

    print(f'Loading CBB slate from {args.cbb}...')
    cbb = load_cbb(args.cbb)
    print(f'  {len(cbb)} CBB props loaded')

    print('Building combined slate...')
    combined = build_combined_slate(nba, cbb)
    print(f'  {len(combined)} total props')

    # Filter pools
    def pool(df, pt=None):
        return filter_eligible(df, args.min_hit_rate, args.min_edge, args.min_rank,
                               tiers, pt or pick_types)

    nba_pool    = pool(nba)
    cbb_pool    = pool(cbb)
    combo_pool  = pool(combined)
    print(f'  NBA eligible: {len(nba_pool)} | CBB eligible: {len(cbb_pool)} | Combined: {len(combo_pool)}')

    print('Generating tickets...')
    wb = Workbook()
    wb.remove(wb.active)

    all_ticket_groups = []
    leg_sizes = [3, 4, 5, 6]

    def gen_tickets(pool_df, sport_label, bg_hdr, sport_prefix, pick_type_filter=None):
        rows_out = []
        for n in leg_sizes:
            sub_pool = pool_df if pick_type_filter is None else pool_df[pool_df['pick_type'].isin([pick_type_filter])]
            tickets = build_tickets(sub_pool, n, args.max_tickets)
            if tickets:
                pt_label = pick_type_filter or 'Mix'
                sheet_name = f'{sport_prefix} {pt_label} {n}-Leg'[:31]
                label = f'{n}-Leg {sport_label} {pt_label}'
                write_ticket_sheet(wb, tickets, sheet_name, bg_hdr, label=f'{sport_label} {pt_label}')
                rows_out.append((sheet_name, tickets, None))
                print(f'  {sheet_name}: {len(tickets)} tickets')
        return rows_out

    # NBA tickets by pick type
    for pt in ['Goblin', 'Standard', 'Demon']:
        pt_pool = pool(nba, [pt])
        if len(pt_pool) >= 3:
            all_ticket_groups += gen_tickets(pt_pool, 'NBA', C['hdr_nba'], 'NBA', pt)

    # NBA Mix
    if len(nba_pool) >= 3:
        all_ticket_groups += gen_tickets(nba_pool, 'NBA', C['hdr_nba'], 'NBA Mix')

    # CBB tickets by pick type
    for pt in ['Goblin', 'Standard', 'Demon']:
        pt_pool = pool(cbb, [pt])
        if len(pt_pool) >= 3:
            all_ticket_groups += gen_tickets(pt_pool, 'CBB', C['hdr_cbb'], 'CBB', pt)

    # CBB Mix
    if len(cbb_pool) >= 3:
        all_ticket_groups += gen_tickets(cbb_pool, 'CBB', C['hdr_cbb'], 'CBB Mix')

    # Combined NBA+CBB tickets (all pick types mixed)
    if len(combo_pool) >= 3:
        all_ticket_groups += gen_tickets(combo_pool, 'COMBO', C['hdr_mix'], 'COMBO')

    # Cross-sport Standard Mix (NBA Standard + CBB Standard) — enforces at least 1 leg from each
    nba_std = pool(nba, ['Standard'])
    cbb_std = pool(cbb, ['Standard'])
    std_mix_pool = pd.concat([nba_std, cbb_std], ignore_index=True).sort_values('rank_score', ascending=False)
    if len(std_mix_pool) >= 3:
        print('Generating cross-sport Standard Mix tickets...')
        for n in leg_sizes:
            tickets = build_tickets(std_mix_pool, n, args.max_tickets, require_mix=True)
            if tickets:
                sheet_name = f'MIX Standard {n}-Leg'
                write_ticket_sheet(wb, tickets, sheet_name, C['hdr_mix'], label='NBA+CBB Standard')
                all_ticket_groups.append((sheet_name, tickets, C['mix']))
                print(f'  {sheet_name}: {len(tickets)} tickets')

    # Cross-sport Goblin Mix (NBA Goblin + CBB Goblin) — enforces at least 1 leg from each
    nba_gob = pool(nba, ['Goblin'])
    cbb_gob = pool(cbb, ['Goblin'])
    gob_mix_pool = pd.concat([nba_gob, cbb_gob], ignore_index=True).sort_values('rank_score', ascending=False)
    if len(gob_mix_pool) >= 3:
        print('Generating cross-sport Goblin Mix tickets...')
        for n in leg_sizes:
            tickets = build_tickets(gob_mix_pool, n, args.max_tickets, require_mix=True)
            if tickets:
                sheet_name = f'MIX Goblin {n}-Leg'
                write_ticket_sheet(wb, tickets, sheet_name, C['goblin'], label='NBA+CBB Goblin')
                all_ticket_groups.append((sheet_name, tickets, C['goblin']))
                print(f'  {sheet_name}: {len(tickets)} tickets')

    # Write slate sheets
    print('Writing slate sheets...')
    write_slate_sheet(wb, combined, 'Full Slate',  C['hdr'],     'ALL')
    write_slate_sheet(wb, nba,      'NBA Slate',   C['hdr_nba'], 'NBA')
    write_slate_sheet(wb, cbb,      'CBB Slate',   C['hdr_cbb'], 'CBB')

    # Summary
    write_summary(wb, nba, cbb, combined, all_ticket_groups, args.date, thresholds)

    # Reorder: SUMMARY first, then slates, then tickets
    summary_sheet = wb['SUMMARY']
    wb.move_sheet(summary_sheet, offset=-len(wb.sheetnames))
    for sname in ['Full Slate','NBA Slate','CBB Slate']:
        if sname in wb.sheetnames:
            wb.move_sheet(wb[sname], offset=-(len(wb.sheetnames) - 1))

    wb.save(args.output)
    print(f'\n✅ Saved -> {args.output}')
    print(f'   Sheets ({len(wb.sheetnames)}): {wb.sheetnames}')

if __name__ == '__main__':
    main()
