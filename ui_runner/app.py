from __future__ import annotations

# ──────────────────────────────────────────────────────────────────────────────
# Windows UTF-8 fix — MUST be at the very top, before any other imports.
# Forces Python itself, all subprocesses, and PowerShell to speak UTF-8.
# Prevents the cp1252 crash when scripts print emojis or non-ASCII player names.
# ──────────────────────────────────────────────────────────────────────────────
import os
import sys

# 1. Force this Python process to use UTF-8 for all I/O
os.environ.setdefault("PYTHONUTF8", "1")
os.environ.setdefault("PYTHONIOENCODING", "utf-8")

# 2. On Windows, reconfigure stdout/stderr to UTF-8 so Flask's own print()
#    calls never crash on emoji characters.
if sys.platform == "win32":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except AttributeError:
        pass  # Python < 3.7 fallback — shouldn't happen on 3.14

import json
import time
import uuid
import threading
import subprocess
from dataclasses import dataclass, field
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime

from flask import Flask, jsonify, render_template, request, send_from_directory, abort

# ──────────────────────────────────────────────────────────────────────────────
# Paths / App
# ──────────────────────────────────────────────────────────────────────────────
BASE_DIR      = Path(__file__).resolve().parent
CONFIG_PATH   = BASE_DIR / "commands.json"
DOCS_DIR      = BASE_DIR / "docs"
TEMPLATES_DIR = BASE_DIR / "templates"
STATIC_DIR    = BASE_DIR / "static"

app = Flask(
    __name__,
    template_folder=str(TEMPLATES_DIR),
    static_folder=str(STATIC_DIR) if STATIC_DIR.exists() else None,
)

# ──────────────────────────────────────────────────────────────────────────────
# Job Model
# ──────────────────────────────────────────────────────────────────────────────
@dataclass
class RunJob:
    job_id: str
    label: str
    started_at: float = field(default_factory=time.time)
    ended_at: Optional[float] = None
    status: str = "RUNNING"          # RUNNING | OK | FAIL
    return_code: Optional[int] = None
    lines: List[str] = field(default_factory=list)

JOBS: Dict[str, RunJob] = {}
LOCK = threading.Lock()


# ──────────────────────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────────────────────
def load_config() -> dict:
    if not CONFIG_PATH.exists():
        raise FileNotFoundError(f"commands.json not found at: {CONFIG_PATH}")
    return json.loads(CONFIG_PATH.read_text(encoding="utf-8-sig"))


def safe_tail(lines: List[str], max_lines: int = 2500) -> List[str]:
    return lines if len(lines) <= max_lines else lines[-max_lines:]


def _build_subprocess_env() -> dict:
    """
    Return an environment dict that forces every child process to use UTF-8.

    Key variables explained:
      PYTHONUTF8          – Python 3.7+ opt-in UTF-8 mode (overrides locale)
      PYTHONIOENCODING    – Fallback for older Pythons / third-party scripts
      PYTHONLEGACYWINDOWSSTDIO – Must be UNSET so Python 3 doesn't revert to cp1252
      PYTHONUTF8 + chcp   – PowerShell will inherit this; we also call chcp 65001
      PSDefaultParameterValues – Tells PowerShell to write UTF-8 by default
    """
    env = os.environ.copy()
    env["PYTHONUTF8"]                     = "1"
    env["PYTHONIOENCODING"]               = "utf-8"
    env["PYTHONLEGACYWINDOWSSTDIO"]       = ""    # explicitly clear, never set to 1
    # Tell PowerShell / cmd to use UTF-8 code page
    env["PYTHONLEGACYWINDOWSFSENCODING"]  = ""
    return env


def _powershell_utf8_prefix() -> List[str]:
    """
    Returns a PowerShell preamble injected before every .ps1 invocation so that:
      • chcp 65001   switches the console code page to UTF-8
      • [Console]::OutputEncoding  makes PS write UTF-8 bytes to stdout
      • $OutputEncoding             makes PS send UTF-8 over pipes
    This is injected via -Command wrapping the -File call.
    """
    return [
        "powershell",
        "-NoProfile",
        "-ExecutionPolicy", "Bypass",
        "-Command",
        (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$OutputEncoding = [System.Text.Encoding]::UTF8; "
            "chcp 65001 | Out-Null; "
        ),
    ]


def _maybe_wrap_powershell(cmd: List[str]) -> List[str]:
    """
    If the command is a plain `powershell … -File foo.ps1` invocation,
    rewrite it so UTF-8 output encoding is set before the script runs.

    Before:
        ["powershell", "-ExecutionPolicy", "Bypass", "-File", "run_daily.ps1"]

    After:
        ["powershell", "-NoProfile", "-ExecutionPolicy", "Bypass", "-Command",
         "[Console]::OutputEncoding = ...; & './run_daily.ps1'"]
    """
    if not cmd:
        return cmd

    exe = cmd[0].lower()
    if exe not in ("powershell", "powershell.exe", "pwsh", "pwsh.exe"):
        return cmd  # not a PowerShell command — leave unchanged

    # Already using -Command: just prepend the UTF-8 setup lines
    lower_args = [a.lower() for a in cmd]
    if "-command" in lower_args:
        idx = lower_args.index("-command")
        utf8_setup = (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$OutputEncoding = [System.Text.Encoding]::UTF8; "
            "chcp 65001 | Out-Null; "
        )
        cmd = list(cmd)
        cmd[idx + 1] = utf8_setup + cmd[idx + 1]
        return cmd

    # Using -File: convert to -Command so we can inject the setup
    if "-file" in lower_args:
        idx = lower_args.index("-file")
        script_path = cmd[idx + 1]
        # Collect any extra args after the script path
        extra_args = cmd[idx + 2:]
        extra_str  = " ".join(f'"{a}"' for a in extra_args) if extra_args else ""

        # Build new -Command string
        ps_body = (
            "[Console]::OutputEncoding = [System.Text.Encoding]::UTF8; "
            "$OutputEncoding = [System.Text.Encoding]::UTF8; "
            "chcp 65001 | Out-Null; "
            f"& '{script_path}' {extra_str}".strip()
        )

        # Reconstruct: keep any flags that came before -File (e.g. -ExecutionPolicy Bypass)
        pre_flags: List[str] = []
        i = 1
        while i < idx:
            pre_flags.append(cmd[i])
            i += 1

        new_cmd = [cmd[0], "-NoProfile"] + pre_flags + ["-Command", ps_body]
        return new_cmd

    return cmd  # fallback — unrecognised shape, leave as-is


def _run_process(job: RunJob, cmd: List[str], workdir: Path) -> None:
    try:
        if not isinstance(cmd, list) or not all(isinstance(x, str) for x in cmd):
            raise ValueError(f"cmd must be List[str]. Got: {type(cmd)}")

        if not workdir.exists():
            raise FileNotFoundError(f"workdir does not exist: {workdir}")

        env = _build_subprocess_env()

        # Wrap PowerShell commands so they always output UTF-8
        safe_cmd = _maybe_wrap_powershell(cmd)

        proc = subprocess.Popen(
            safe_cmd,
            cwd=str(workdir),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            encoding="utf-8",
            errors="replace",   # replaces any un-decodable bytes with U+FFFD
            bufsize=1,
            env=env,
        )

        assert proc.stdout is not None
        for raw_line in proc.stdout:
            # Strip \r\n (Windows) and lone \n
            line = raw_line.rstrip("\r\n")
            # Replace any remaining null bytes that could break JSON serialisation
            line = line.replace("\x00", "")
            with LOCK:
                job.lines.append(line)
                job.lines = safe_tail(job.lines)

        rc = proc.wait()
        with LOCK:
            job.return_code = rc
            job.ended_at    = time.time()
            job.status      = "OK" if rc == 0 else "FAIL"

    except Exception as exc:
        with LOCK:
            job.lines.append(f"[ERROR] {type(exc).__name__}: {exc}")
            job.ended_at    = time.time()
            job.status      = "FAIL"
            job.return_code = -1


def start_job(label: str, cmd: List[str], workdir: Path) -> str:
    job_id = str(uuid.uuid4())
    job    = RunJob(job_id=job_id, label=label)
    with LOCK:
        JOBS[job_id] = job
    t = threading.Thread(target=_run_process, args=(job, cmd, workdir), daemon=True)
    t.start()
    return job_id


def resolve_command(config: dict, pipeline_name: str, command_id: str) -> Dict[str, Any]:
    pipelines = config.get("pipelines") or {}
    if pipeline_name not in pipelines:
        raise KeyError(
            f"Unknown pipeline '{pipeline_name}'. Available: {list(pipelines.keys())}"
        )

    pipe          = pipelines[pipeline_name]
    commands_list = pipe.get("commands") or []
    cmds          = {
        c.get("id"): c
        for c in commands_list
        if isinstance(c, dict) and c.get("id")
    }

    if command_id not in cmds:
        raise KeyError(
            f"Unknown command_id '{command_id}' for pipeline '{pipeline_name}'."
        )

    c = cmds[command_id]

    if "cmd_chain" in c:
        chain_ids = c.get("cmd_chain") or []
        expanded  = []
        for x in chain_ids:
            if x not in cmds:
                raise KeyError(f"cmd_chain references missing command id '{x}'.")
            expanded.append(cmds[x])
        return {"type": "chain", "items": expanded, "label": c.get("label", command_id)}

    return {"type": "single", "item": c, "label": c.get("label", command_id)}


def subst_tokens(cmd: List[str], config: Optional[dict] = None) -> List[str]:
    today     = datetime.now().strftime("%Y-%m-%d")
    now_ts    = datetime.now().strftime("%Y-%m-%d_%H%M%S")
    repo_root = ""
    if config and isinstance(config, dict):
        repo_root = (
            str(Path(config.get("repo_root", "")).resolve())
            if config.get("repo_root")
            else ""
        )
    out: List[str] = []
    for x in cmd:
        y = x.replace("{TODAY}", today).replace("{NOW}", now_ts)
        if repo_root:
            y = y.replace("{REPO_ROOT}", repo_root)
        out.append(y)
    return out


def latest_template(prefix: str, suffix: str = ".html") -> Optional[str]:
    """Return filename of newest template matching e.g. slate_eval_*.html"""
    if not TEMPLATES_DIR.exists():
        return None
    candidates = sorted(
        TEMPLATES_DIR.glob(f"{prefix}*{suffix}"),
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    return candidates[0].name if candidates else None


# ──────────────────────────────────────────────────────────────────────────────
# Pages
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/")
def home():
    return render_template("index.html", config=load_config())


@app.get("/tickets")
def page_tickets():
    target = DOCS_DIR / "tickets_latest.html"
    if target.exists():
        return send_from_directory(DOCS_DIR, "tickets_latest.html")
    return (
        "tickets_latest.html not found in ui_runner/docs. "
        "Run the pipeline or copy the file.",
        404,
    )


@app.get("/payout")
def page_payout():
    return render_template("payout_calculator.html")


@app.get("/slate")
def page_slate():
    fname = latest_template("slate_eval_")
    if not fname:
        return (
            "No slate_eval_*.html template found in ui_runner/templates. "
            "Generate it or copy it there.",
            404,
        )
    return render_template(fname)


@app.get("/grades")
def page_grades():
    return render_template("indexGrades.html")


# ──────────────────────────────────────────────────────────────────────────────
# Docs Static Serving
# ──────────────────────────────────────────────────────────────────────────────
@app.get("/docs/<path:filename>")
def serve_docs(filename: str):
    if not DOCS_DIR.exists():
        abort(404)
    return send_from_directory(str(DOCS_DIR), filename)


# ──────────────────────────────────────────────────────────────────────────────
# API
# ──────────────────────────────────────────────────────────────────────────────
@app.post("/api/run")
def api_run():
    data       = request.get_json(force=True) or {}
    pipeline   = data.get("pipeline")
    command_id = data.get("command_id")

    if not pipeline or not command_id:
        return jsonify({"error": "missing_pipeline_or_command_id"}), 400

    try:
        config       = load_config()
        repo_root    = Path(config["repo_root"]).expanduser().resolve()
        cmd_def      = resolve_command(config, pipeline, command_id)
        workdir_rel  = (config["pipelines"][pipeline].get("workdir") or "").strip()
        workdir      = (repo_root / workdir_rel).resolve()
    except Exception as exc:
        return jsonify({"error": "config_or_command_error", "detail": str(exc)}), 400

    # ── Chain ──
    if cmd_def["type"] == "chain":
        parent_id = str(uuid.uuid4())
        parent    = RunJob(job_id=parent_id, label=cmd_def["label"])
        with LOCK:
            JOBS[parent_id] = parent

        def chain_runner() -> None:
            ok = True
            for item in cmd_def["items"]:
                label   = item.get("label") or item.get("id") or "STEP"
                raw_cmd = item.get("cmd")

                if not isinstance(raw_cmd, list):
                    with LOCK:
                        parent.lines.append(
                            f"[ERROR] Bad cmd for '{label}': expected list, got {type(raw_cmd)}"
                        )
                    ok = False
                    break

                cmd = subst_tokens(raw_cmd, config=config)

                with LOCK:
                    parent.lines.append("")
                    parent.lines.append(f"=== {label} ===")
                    parent.lines.append(" ".join(cmd))

                child = RunJob(job_id=str(uuid.uuid4()), label=label)
                _run_process(child, cmd, workdir)

                with LOCK:
                    parent.lines.extend(child.lines)
                    parent.lines = safe_tail(parent.lines)
                    if child.return_code != 0:
                        ok = False
                        parent.lines.append("[CHAIN] Stopping early due to failure.")
                        break

            with LOCK:
                parent.ended_at    = time.time()
                parent.status      = "OK" if ok else "FAIL"
                parent.return_code = 0 if ok else 1

        threading.Thread(target=chain_runner, daemon=True).start()
        return jsonify({"job_id": parent_id})

    # ── Single ──
    item    = cmd_def["item"]
    raw_cmd = item.get("cmd")
    if not isinstance(raw_cmd, list):
        return jsonify(
            {"error": "bad_cmd_type", "detail": f"Expected list, got {type(raw_cmd)}"}
        ), 400

    cmd    = subst_tokens(raw_cmd, config=config)
    job_id = start_job(cmd_def["label"], cmd, workdir)
    return jsonify({"job_id": job_id})


@app.get("/api/job/<job_id>")
def api_job(job_id: str):
    with LOCK:
        j = JOBS.get(job_id)
        if not j:
            return jsonify({"error": "not_found"}), 404
        return jsonify(
            {
                "job_id":      j.job_id,
                "label":       j.label,
                "status":      j.status,
                "return_code": j.return_code,
                "started_at":  j.started_at,
                "ended_at":    j.ended_at,
                "lines":       j.lines[-400:],
            }
        )


@app.get("/api/jobs")
def api_jobs():
    with LOCK:
        out = [
            {
                "job_id":      j.job_id,
                "label":       j.label,
                "status":      j.status,
                "started_at":  j.started_at,
                "ended_at":    j.ended_at,
                "return_code": j.return_code,
            }
            for j in JOBS.values()
        ]
    out.sort(key=lambda x: x["started_at"], reverse=True)
    return jsonify(out[:25])


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    DOCS_DIR.mkdir(exist_ok=True)
    app.run(host="0.0.0.0", port=8787, debug=False)
