"""
py -3 check_graded.py --folder "outputs\2026-03-06"
"""
import argparse, sys
from pathlib import Path

try:
    import pandas as pd
except ImportError:
    print("pip install pandas openpyxl"); sys.exit(1)

parser = argparse.ArgumentParser()
parser.add_argument("--folder", required=True)
parser.add_argument("--file",   default=None)
args = parser.parse_args()

files = []
if args.file:
    files = [Path(args.file)]
else:
    folder = Path(args.folder)
    files = sorted(folder.glob("graded_*.xlsx")) + sorted(folder.glob("graded_*.csv"))

if not files:
    print(f"No graded files found in {args.folder}")
    sys.exit(1)

for fpath in files:
    print(f"\n{'='*60}")
    print(f"FILE: {fpath.name}")
    print('='*60)
    
    if fpath.suffix == ".xlsx":
        xf = pd.ExcelFile(fpath)
        print(f"Sheets: {xf.sheet_names}")
        for sheet in xf.sheet_names:
            df = pd.read_excel(fpath, sheet_name=sheet, nrows=3)
            print(f"\n  Sheet '{sheet}': {len(pd.read_excel(fpath, sheet_name=sheet))} rows")
            print(f"  Columns: {list(df.columns)}")
            print(f"  Sample:")
            for col in df.columns:
                vals = df[col].tolist()
                print(f"    {col}: {vals}")
    else:
        df = pd.read_csv(fpath, nrows=3)
        print(f"  Rows: {len(pd.read_csv(fpath))}")
        print(f"  Columns: {list(df.columns)}")
        for col in df.columns:
            vals = df[col].tolist()
            print(f"    {col}: {vals}")
