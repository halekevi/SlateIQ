# ===============================
# Daily NBA Props Pipeline
# ===============================

py -3.14 .\step1_fetch_prizepicks_api.py --out step1_fetch_prizepicks_api.csv;
py -3.14 .\step2_attach_picktypes.py --input step1_fetch_prizepicks_api.csv --output step2_attach_picktypes.csv;
py -3.14 .\step3_attach_defense.py --input step2_attach_picktypes.csv --defense defense_team_summary.csv --output step3_with_defense.csv;
py -3.14 .\step4_attach_player_stats.py --input step3_with_defense.csv --output step4_with_stats.csv --season 2025-26;
py -3.14 .\step5_add_line_hit_rates.py --input step4_with_stats.csv --output step5_with_line_hit_rates.csv;
py -3.14 .\step6_team_role_context.py --input step5_with_line_hit_rates.csv --output step6_with_team_role_context.csv;
py -3.14 .\step7_rank_props.py --input step6_with_team_role_context.csv --output step7_ranked_props.xlsx;
py -3.14 .\step8_add_direction_context.py --input step7_ranked_props.xlsx --sheet STANDARD --output step8_standard_direction.csv

#.\run_daily_steps.ps1