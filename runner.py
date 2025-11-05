"""Async runner for the Symbioza aider executor agent."""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import json
import logging
import signal
from typing import Any, Dict, List, Tuple

try:  # pragma: no cover - optional dependency
    import uvloop
except ImportError:  # pragma: no cover
    uvloop = None

from redis.asyncio import Redis
from redis.exceptions import RedisError

from modules.symbioza_core.config.loader import get_effective_config

from .executor import run_exec_command
from .utils import configure_logger, get_env, log_trace, utc_now

INBOX_STREAM = "symbioza:msg:symbioza-aider"
DEFAULT_RESULT_STREAM = "symbioza:msg:owner"
IDLE_SLEEP_SECONDS = 3.0


async def _publish_result(
    redis: Redis,
    result: Dict[str, Any],
    *,
    origin_id: str,
    target_module: str,
    files: List[str],
) -> None:
    resolved_target = target_module or "owner"
    stream = (
        DEFAULT_RESULT_STREAM if resolved_target == "owner" else f"symbioza:msg:{resolved_target}"
    )

    payload = {
        "type": "ExecResult",
        "status": result.get("status", "unknown"),
        "commit": result.get("commit_id") or result.get("commit", ""),
        "detail": result.get("detail", ""),
        "timestamp": result.get("timestamp", utc_now().isoformat()),
        "origin": {"id": origin_id},
        "target": resolved_target,
        "files": files,
        "push_status": result.get("push_status", ""),
        "stdout": result.get("stdout", ""),
        "stderr": result.get("stderr", ""),
    }
    if result.get("push_detail"):
        payload["push_detail"] = result["push_detail"]

    body = {
        "from": "symbioza-aider",
        "payload": json.dumps(payload),
    }
    entry_id = await redis.xadd(stream, body)
    await log_trace(
        "aider_result_enqueued",
        {"stream": stream, "entry_id": entry_id, "payload": payload},
    )


async def _report_exec_result_to_critic(
    redis: Redis,
    result: Dict[str, Any],
    instruction_text: str,
    logger: logging.Logger,
) -> None:
    instruction = (instruction_text or "").strip()
    payload = {
        "type": "ExecResult",
        "commit_id": result.get("commit_id") or result.get("commit"),
        "status": "success" if (result.get("exit_code", 0) == 0) else "failure",
        "stdout": (result.get("stdout", "") or "")[:5000],
        "stderr": (result.get("stderr", "") or "")[:5000],
        "instruction": instruction,
    }

    await redis.xadd(
        "symbioza:msg:symbioza-critic",
        {"from": "symbioza-aider", "payload": json.dumps(payload)},
    )
    logger.info(
        "[AIDER] Reported ExecResult → symbioza:msg:symbioza-critic (%s)",
        payload["status"],
    )


async def _handle_exec_command(
    redis: Redis,
    message_id: str,
    sender: str,
    content: Dict[str, Any],
    logger: logging.Logger,
) -> None:
    instruction = content.get("instruction", "")
    files_field = content.get("files") or []
    if isinstance(files_field, list):
        files = files_field
    elif isinstance(files_field, str):
        files = [files_field]
    else:
        files = []
    target_module = content.get("reply_to") or sender or "owner"

    await log_trace(
        "aider_exec_start",
        {
            "id": message_id,
            "from": sender,
            "instruction": instruction,
            "files": files,
            "target": target_module,
        },
    )
    logger.info(
        "ExecCommand received from %s -> %s (instruction=%s)",
        sender,
        files,
        instruction[:60],
    )

    result = await asyncio.to_thread(run_exec_command, instruction, files)

    await log_trace(
        "aider_exec_end",
        {
            "id": message_id,
            "result": result,
            "target": target_module,
        },
    )

    logger.info(
        "ExecCommand finished with status=%s commit=%s",
        result.get("status"),
        result.get("commit_id") or result.get("commit", "unknown"),
    )

    try:
        await _publish_result(
            redis,
            result,
            origin_id=message_id,
            target_module=target_module,
            files=files,
        )
        await _report_exec_result_to_critic(redis, result, instruction, logger)
    except RedisError as exc:
        logger.error("Failed to publish result: %s", exc)
        await log_trace(
            "aider_result_publish_error",
            {"id": message_id, "error": str(exc), "result": result},
        )


async def _process_message(
    redis: Redis,
    message_id: str,
    payload: Dict[str, str],
    logger: logging.Logger,
) -> None:
    sender = payload.get("from", "unknown")
    raw_payload = payload.get("payload") or "{}"
    try:
        content = json.loads(raw_payload)
    except json.JSONDecodeError:
        content = {"_raw": raw_payload}

    await log_trace(
        "aider_message_received",
        {"id": message_id, "from": sender, "payload": content},
    )

    if content.get("type") != "ExecCommand":
        logger.info("Skipping message %s (type=%s)", message_id, content.get("type"))
        await log_trace(
            "aider_message_skipped",
            {"id": message_id, "reason": "unsupported_type", "payload": content},
        )
        return

    await _handle_exec_command(redis, message_id, sender, content, logger)


async def _consume(redis: Redis, logger: logging.Logger, *, debug: bool) -> None:
    last_id = "0"
    catching_up = True

    if debug:
        logger.info("Starting backlog replay from %s", last_id)

    while True:
        try:
            if catching_up:
                response: List[Tuple[str, List[Tuple[str, Dict[str, str]]]]] = await redis.xread(
                    {INBOX_STREAM: last_id},
                    count=50,
                )
            else:
                logger.info("Waiting for new messages…")
                response = await redis.xread({INBOX_STREAM: "$"}, block=5000, count=10)
        except RedisError as exc:
            logger.error("Redis read error: %s", exc)
            await asyncio.sleep(IDLE_SLEEP_SECONDS)
            continue

        if not response:
            if catching_up:
                catching_up = False
                logger.info("Backlog drained; switching to live stream.")
            else:
                logger.info("Idle, sleeping for %.0fs.", IDLE_SLEEP_SECONDS)
                await asyncio.sleep(IDLE_SLEEP_SECONDS)
            continue

        for _, messages in response:
            for message_id, payload in messages:
                if catching_up:
                    last_id = message_id
                logger.info("Received message %s", message_id)
                await log_trace(
                    "aider_message_raw",
                    {"id": message_id, "payload": payload},
                )
                try:
                    await _process_message(redis, message_id, payload, logger)
                except Exception as exc:  # noqa: BLE001
                    logger.error("Failed to process message %s: %s", message_id, exc)
                    await log_trace(
                        "aider_message_error",
                        {"id": message_id, "error": str(exc)},
                    )
        catching_up = False


async def main(debug: bool = False) -> None:
    redis_host = get_env("REDIS_HOST", "localhost")
    redis_port = int(get_env("REDIS_PORT", "6379"))
    redis_db = int(get_env("REDIS_DB", "0"))

    logger = configure_logger()
    if debug:
        logger.setLevel(logging.DEBUG)

    cfg = get_effective_config("symbioza-aider")
    logger.info("[CFG] Agent symbioza-aider using %s via %s", cfg.get("model"), cfg.get("provider"))

    logger.info("Connecting to Redis at %s:%s/%s", redis_host, redis_port, redis_db)

    redis = Redis(
        host=redis_host,
        port=redis_port,
        db=redis_db,
        decode_responses=True,
        encoding="utf-8",
    )

    stop_event = asyncio.Event()

    def _handle_signal(*_: Any) -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    consumer = asyncio.create_task(_consume(redis, logger, debug=debug))

    await stop_event.wait()
    consumer.cancel()
    with contextlib.suppress(asyncio.CancelledError):
        await consumer

    try:
        await redis.aclose()
        await redis.connection_pool.disconnect()
    finally:
        logger.info("Shutdown complete.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Symbioza aider agent runner")
    parser.add_argument("--debug", action="store_true", help="Enable verbose logging")
    args = parser.parse_args()

    if uvloop is not None:  # pragma: no cover - optional
        uvloop.install()

    asyncio.run(main(debug=args.debug))
