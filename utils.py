"""Utility helpers for the Symbioza aider agent."""
from __future__ import annotations

import asyncio
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict

TRACE_LOG_PATH = Path("logs/agent_traces.jsonl")


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def get_env(name: str, default: str) -> str:
    value = os.getenv(name)
    return value if value else default


def configure_logger(name: str = "symbioza.aider") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("[AIDER] %(message)s")
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.propagate = False
        logger.setLevel(logging.INFO)
    return logger


def _append_trace_line(line: str) -> None:
    TRACE_LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
    with TRACE_LOG_PATH.open("a", encoding="utf-8") as handle:
        handle.write(line + "\n")


async def log_trace(event: str, payload: Dict[str, Any]) -> None:
    record = {
        "event": event,
        "timestamp": utc_now().isoformat(),
        **payload,
    }
    line = json.dumps(record, ensure_ascii=False)
    await asyncio.to_thread(_append_trace_line, line)


def log_trace_sync(event: str, payload: Dict[str, Any]) -> None:
    record = {
        "event": event,
        "timestamp": utc_now().isoformat(),
        **payload,
    }
    line = json.dumps(record, ensure_ascii=False)
    _append_trace_line(line)
