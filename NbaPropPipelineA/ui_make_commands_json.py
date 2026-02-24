#!/usr/bin/env python3
from __future__ import annotations

import json
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parent

PIPELINES = {
    "NBA - Pipeline A (Debug)": ROOT / "NbaPropPipelineA",
    "CBB - CBB2 (Debug)": ROOT / "CBB2",
}

def find_step_scripts(folder: Path):
    # Prefer step*.py, but also include common runners if present
    scripts = []
    for p in sorted(folder.glob("step*.py")):
        scripts.append(p.name)
    for extra in ["run_daily.ps1", "run_grade.ps1"]:
        if (ROOT / extra).exists():
            pass
    return scripts

def build():
    repo_root = str(ROOT)
    config = {
        "repo_root": repo_root,
        "pipelines": {
            "DAILY (One-Click)": {
                "workdir": "",
                "commands": []
            }
        }
    }

    # Add master daily/grade if present
    daily = ROOT / "run_daily.ps1"
    grade = ROOT / "run_grade.ps1"
    if daily.exists():
        config["pipelines"]["DAILY (One-Click)"]["commands"].append({
            "id": "run_daily",
            "label": "Run DAILY (Master) — NBA + CBB",
            "cmd": ["powershell", "-ExecutionPolicy", "Bypass", "-File", "run_daily.ps1"]
        })
    if grade.exists():
        config["pipelines"]["DAILY (One-Click)"]["commands"].append({
            "id": "run_grade",
            "label": "Run GRADE (Master) — NBA + CBB",
            "cmd": ["powershell", "-ExecutionPolicy", "Bypass", "-File", "run_grade.ps1"]
        })

    # Add debug pipelines (auto buttons from step*.py)
    for pname, folder in PIPELINES.items():
        if not folder.exists():
            continue
        workdir = folder.relative_to(ROOT).as_posix()
        scripts = find_step_scripts(folder)

        cmds = []
        for s in scripts:
            cmd_id = s.replace(".py", "")
            cmds.append({
                "id": cmd_id,
                "label": f"{pname.split('(')[0].strip()} • {s}",
                "cmd": ["py", "-3.14", "-u", s]
            })

        config["pipelines"][pname] = {
            "workdir": workdir,
            "commands": cmds
        }

    out = ROOT / "ui_runner" / "commands.json"
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(config, indent=2), encoding="utf-8")
    print(f"✅ wrote {out}")
    print("Pipelines included:")
    for k in config["pipelines"].keys():
        print(" -", k)

if __name__ == "__main__":
    build()