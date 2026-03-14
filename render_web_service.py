from __future__ import annotations

import asyncio
import secrets
import sys
import threading
import traceback
from datetime import datetime
from typing import Any

from dotenv import load_dotenv
from flask import Flask, jsonify, request
from rich.console import Console

import main as botfactory_main

try:
    from apscheduler.schedulers.background import BackgroundScheduler
    from apscheduler.triggers.cron import CronTrigger
except Exception:  # pragma: no cover
    BackgroundScheduler = None
    CronTrigger = None


app = Flask(__name__)
_RUN_LOCK = threading.Lock()
_STATE_LOCK = threading.Lock()
_SCHEDULER: Any | None = None
_STATE: dict[str, Any] = {
    "status": "idle",
    "running": False,
    "last_started_at": "",
    "last_finished_at": "",
    "last_mode": "",
    "last_seed_url": "",
    "last_trigger": "",
    "last_error": "",
    "last_output": "",
}


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def set_state(**updates: Any) -> None:
    with _STATE_LOCK:
        _STATE.update(updates)


def append_state_output(text: str, max_chars: int = 12000) -> None:
    if not text:
        return
    with _STATE_LOCK:
        _STATE["last_output"] = (_STATE.get("last_output", "") + text)[-max_chars:]


class StateLogStream:
    encoding = "utf-8"

    def write(self, text: str) -> int:
        if not text:
            return 0
        sys.stdout.write(text)
        sys.stdout.flush()
        append_state_output(text)
        return len(text)

    def flush(self) -> None:
        sys.stdout.flush()

    def isatty(self) -> bool:
        return False


def authorize_request() -> bool:
    expected_token = botfactory_main.getenv_raw("RENDER_TRIGGER_TOKEN", "").strip()
    if not expected_token:
        return True
    provided_token = (
        request.headers.get("X-Trigger-Token", "")
        or request.headers.get("Authorization", "").removeprefix("Bearer ").strip()
        or request.args.get("token", "")
    )
    return secrets.compare_digest(provided_token, expected_token)


def build_config(mode: str, seed_url: str | None = None) -> botfactory_main.AppConfig:
    load_dotenv(override=False)
    argv = ["--mode", mode]
    if seed_url:
        argv.extend(["--seed-url", seed_url])
    return botfactory_main.build_config(botfactory_main.build_parser().parse_args(argv))


def collect_lead_snapshot(config: botfactory_main.AppConfig) -> dict[str, Any]:
    try:
        leads_df = botfactory_main.load_leads_dataframe(config.leads_file, config.sheets)
    except Exception as exc:
        return {"error": str(exc)}

    status_series = leads_df["Status"].fillna("").astype(str).str.strip().str.casefold()
    return {
        "total": int(len(leads_df.index)),
        "new": int(status_series.eq("new").sum()),
        "sent": int(status_series.eq("sent").sum()),
        "error": int(status_series.eq("error").sum()),
        "blacklisted": int(status_series.eq("blacklisted").sum()),
    }


def pipeline_worker(mode: str, seed_url: str | None, trigger: str) -> None:
    stream = StateLogStream()
    console = Console(file=stream, force_terminal=False, color_system=None, width=120)
    set_state(
        status="running",
        running=True,
        last_started_at=now_iso(),
        last_finished_at="",
        last_mode=mode,
        last_seed_url=seed_url or "",
        last_trigger=trigger,
        last_error="",
        last_output="",
    )
    print(f"[render_web_service] Pipeline started mode={mode} trigger={trigger} at {now_iso()}", flush=True)
    try:
        config = build_config(mode, seed_url)
        asyncio.run(botfactory_main.main_async(config, console))
        set_state(
            status="success",
            running=False,
            last_finished_at=now_iso(),
        )
        print(f"[render_web_service] Pipeline finished successfully at {now_iso()}", flush=True)
    except Exception as exc:
        error_trace = traceback.format_exc()
        append_state_output("\n" + error_trace)
        set_state(
            status="error",
            running=False,
            last_finished_at=now_iso(),
            last_error=str(exc),
        )
        print(f"[render_web_service] Pipeline failed: {exc}", flush=True)
    finally:
        _RUN_LOCK.release()


def trigger_pipeline(mode: str = "all", seed_url: str | None = None, trigger: str = "manual") -> tuple[bool, str]:
    if not _RUN_LOCK.acquire(blocking=False):
        return False, "Pipeline already running."
    worker = threading.Thread(target=pipeline_worker, args=(mode, seed_url, trigger), daemon=True)
    worker.start()
    return True, "Pipeline started."


def initialize_scheduler() -> None:
    global _SCHEDULER
    if _SCHEDULER is not None:
        return
    if not botfactory_main.getenv_bool("RENDER_ENABLE_SCHEDULER", False):
        return
    if BackgroundScheduler is None or CronTrigger is None:
        set_state(status="scheduler-unavailable", last_error="APScheduler is not installed.")
        return

    cron_expression = botfactory_main.getenv_str("RENDER_SCHEDULE_CRON", "0 6 * * *")
    timezone_name = botfactory_main.getenv_str("RENDER_TIMEZONE", "Asia/Tashkent")
    scheduler = BackgroundScheduler(timezone=timezone_name)
    scheduler.add_job(
        lambda: trigger_pipeline(trigger="scheduler"),
        CronTrigger.from_crontab(cron_expression, timezone=timezone_name),
        id="daily-pipeline",
        replace_existing=True,
    )
    scheduler.start()
    _SCHEDULER = scheduler
    set_state(status="scheduler-ready")


@app.before_request
def ensure_scheduler() -> None:
    initialize_scheduler()


@app.get("/")
def index() -> Any:
    load_dotenv(override=False)
    try:
        config = build_config("email")
        leads = collect_lead_snapshot(config)
        storage_mode = "google-sheets" if config.sheets else "excel"
        response = {
            "service": "botfactory-render-web",
            "ok": True,
            "storage": storage_mode,
            "gemini_model": config.gemini_model,
            "scheduler_enabled": botfactory_main.getenv_bool("RENDER_ENABLE_SCHEDULER", False),
            "leads": leads,
            "state": _STATE,
        }
        return jsonify(response), 200
    except Exception as exc:
        return jsonify({"service": "botfactory-render-web", "ok": False, "error": str(exc), "state": _STATE}), 500


@app.get("/healthz")
def healthz() -> Any:
    return jsonify({"ok": True, "running": _STATE["running"], "status": _STATE["status"]}), 200


@app.get("/status")
def status() -> Any:
    load_dotenv(override=False)
    try:
        config = build_config("email")
        return (
            jsonify(
                {
                    "ok": True,
                    "state": _STATE,
                    "storage": "google-sheets" if config.sheets else "excel",
                    "gemini_model": config.gemini_model,
                    "leads": collect_lead_snapshot(config),
                }
            ),
            200,
        )
    except Exception as exc:
        return jsonify({"ok": False, "state": _STATE, "error": str(exc)}), 500


@app.post("/trigger")
def trigger() -> Any:
    if not authorize_request():
        return jsonify({"ok": False, "error": "Unauthorized"}), 401

    payload = request.get_json(silent=True) or {}
    mode = str(payload.get("mode", "all")).strip().lower() or "all"
    seed_url = str(payload.get("seed_url", "")).strip() or None
    if mode not in {"scrape", "email", "all", "sync-replies"}:
        return jsonify({"ok": False, "error": "Invalid mode"}), 400

    started, message = trigger_pipeline(mode=mode, seed_url=seed_url, trigger="http")
    status_code = 202 if started else 409
    return jsonify({"ok": started, "message": message, "state": _STATE}), status_code


if __name__ == "__main__":
    initialize_scheduler()
    port = botfactory_main.getenv_int("PORT", 10000)
    app.run(host="0.0.0.0", port=port)
