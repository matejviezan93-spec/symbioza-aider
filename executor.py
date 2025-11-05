"""Execution helpers for the Symbioza aider agent."""
from __future__ import annotations

import logging
import shutil
import subprocess
import time
from pathlib import Path
from typing import Dict, List

from .utils import log_trace_sync, utc_now


LOGGER = logging.getLogger("symbioza.aider")
_MAX_ATTEMPTS = 3
_INITIAL_BACKOFF = 1.0


def _git(args: List[str], cwd: Path) -> subprocess.CompletedProcess:
    return subprocess.run(
        ["git", *args],
        cwd=cwd,
        capture_output=True,
        text=True,
        check=False,
    )


def _prepare_file_args(files: List[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for path in files:
        normalized = (path or "").strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        ordered.append(normalized)
    return ordered


def run_exec_command(instruction: str, files: List[str]) -> Dict[str, str]:
    """Execute aider with the provided instruction and files.

    Returns a structured result dictionary describing the execution outcome.
    """

    instruction = (instruction or "").strip()
    timestamp = utc_now().isoformat()

    if not instruction:
        return {
            "status": "error",
            "detail": "Instruction must not be empty",
            "timestamp": timestamp,
            "stdout": "",
            "stderr": "",
            "commit_id": "",
        }

    file_args = _prepare_file_args(files)
    if not file_args:
        return {
            "status": "error",
            "detail": "No valid files provided",
            "timestamp": timestamp,
            "stdout": "",
            "stderr": "",
            "commit_id": "",
        }

    repo_root = Path.cwd()
    git_root_proc = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"],
        cwd=repo_root,
        capture_output=True,
        text=True,
        check=False,
    )
    if git_root_proc.returncode == 0 and git_root_proc.stdout.strip():
        repo_root = Path(git_root_proc.stdout.strip())

    git_dir = repo_root / ".git"
    if not git_dir.exists():
        LOGGER.warning("Exec skipped: missing .git directory (repo=%s)", repo_root)
        message = "[AIDER] Exec skipped (reason=no_git_directory)"
        log_trace_sync(
            "aider_exec_skip",
            {"message": message, "repo_root": str(repo_root)},
        )
        return {
            "status": "error",
            "detail": "Git repository not found",
            "timestamp": timestamp,
            "stdout": "",
            "stderr": message,
            "commit_id": "",
        }

    aider_path = shutil.which("aider")
    base_command: List[str] = [aider_path] if aider_path else ["python3", "-m", "aider_chat.main"]
    if not aider_path:
        log_trace_sync(
            "aider_exec_resolve",
            {"message": "[AIDER] Using python fallback", "command": base_command},
        )

    command: List[str] = [*base_command, *file_args, "-m", instruction, "--yes", "--commit"]

    stdout = ""
    stderr = ""
    backoff = _INITIAL_BACKOFF
    for attempt in range(1, _MAX_ATTEMPTS + 1):
        start_message = f"[AIDER] Exec started (attempt={attempt})"
        LOGGER.info("Exec started (attempt=%s)", attempt)
        log_trace_sync(
            "aider_exec_started",
            {
                "message": start_message,
                "attempt": attempt,
                "command": command,
                "files": file_args,
            },
        )

        completed = subprocess.run(
            command,
            cwd=repo_root,
            capture_output=True,
            text=True,
            check=False,
        )

        stdout = (completed.stdout or "").strip()
        stderr = (completed.stderr or "").strip()

        if completed.returncode == 0:
            commit_proc = _git(["rev-parse", "HEAD"], repo_root)
            commit_hash = (commit_proc.stdout or "").strip()

            push_proc = _git(["push"], repo_root)
            push_info = (push_proc.stdout or push_proc.stderr or "").strip()
            push_status = "ok" if push_proc.returncode == 0 else "error"

            commit_message = f"[AIDER] Commit completed: {commit_hash or 'unknown'}"
            LOGGER.info("Commit completed: %s", commit_hash or "unknown")
            log_trace_sync(
                "aider_exec_commit",
                {
                    "message": commit_message,
                    "commit_id": commit_hash,
                    "push_status": push_status,
                    "push_detail": push_info,
                },
            )

            detail = stdout.splitlines()[-1] if stdout else "aider run succeeded"
            result: Dict[str, str] = {
                "status": "ok",
                "detail": detail,
                "timestamp": timestamp,
                "stdout": stdout,
                "stderr": stderr,
                "commit_id": commit_hash,
                "push_status": push_status,
            }
            if push_info:
                result["push_detail"] = push_info
            return result

        failure_reason = stderr or stdout or "Aider execution failed"
        fail_message = f"[AIDER] Exec failed (reason={failure_reason[:120]})"
        LOGGER.error("Exec failed (attempt=%s): %s", attempt, failure_reason)
        log_trace_sync(
            "aider_exec_failed",
            {
                "message": fail_message,
                "attempt": attempt,
                "returncode": completed.returncode,
                "stdout": stdout,
                "stderr": stderr,
            },
        )

        if attempt < _MAX_ATTEMPTS:
            time.sleep(backoff)
            backoff *= 2

    return {
        "status": "error",
        "detail": failure_reason,
        "timestamp": timestamp,
        "stdout": stdout,
        "stderr": stderr,
        "commit_id": "",
    }
