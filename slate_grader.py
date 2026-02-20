#!/usr/bin/env python3
import argparse
import pandas as pd

SHOOTING_MAP = {
    "Field Goals Made": ["fgm", "field goals made"],
    "Field Goals Attempted": ["fga", "field goals attempted"],
    "3-PT Made": ["3pm", "3pt made", "3-pt made"],
    "3-PT Attempted": ["3pa", "3pt attempted", "3-pt attempted"],
    "2-PT Made": ["2pm", "2pt made", "2-pt made"],
    "2-PT Attempted": ["2pa", "2pt attempted", "2-pt attempted"],
}

def normalize_prop(prop):
    p = str(prop).lower()
    for canon, variants in SHOOTING_MAP.items():
        for v in variants:
            if v in p:
                return canon
    return prop

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--sport", required=True)
    parser.add_argument("--slate", required=True)
    parser.add_argument("--actuals", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    slate = pd.read_excel(args.slate)
    actuals = pd.read_csv(args.actuals)

    slate["prop_type"] = slate["prop_type"].apply(normalize_prop)
    actuals["prop_type"] = actuals["prop_type"].apply(normalize_prop)

    merged = slate.merge(
        actuals,
        on=["player", "prop_type"],
        how="left"
    )

    merged.to_excel(args.output, index=False)
    print(f"Saved graded file -> {args.output}")

if __name__ == "__main__":
    main()
