from __future__ import annotations

import json
import os
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any
from uuid import uuid4

_REPLACE_RETRIES = 8
_REPLACE_BASE_DELAY_SECONDS = 0.025


def write_json_atomically(
    path: Path,
    payload: Any,
    *,
    default: Callable[[Any], Any] | None = None,
    sort_keys: bool = False,
) -> None:
    """Durably replace a JSON file without exposing partial contents."""

    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid4().hex}.tmp")
    try:
        kwargs: dict[str, Any] = {
            "ensure_ascii": False,
            "indent": 2,
            "sort_keys": sort_keys,
        }
        if default is not None:
            kwargs["default"] = default
        serialized = json.dumps(payload, **kwargs)
        with temporary.open("w", encoding="utf-8", newline="\n") as handle:
            handle.write(serialized)
            handle.flush()
            os.fsync(handle.fileno())
        _replace_with_bounded_retry(temporary, path)
        _fsync_directory(path.parent)
    except BaseException:
        temporary.unlink(missing_ok=True)
        raise


def append_jsonl_durably(path: Path, payload: Any) -> None:
    """Append one complete JSONL record and force it to stable storage."""

    path.parent.mkdir(parents=True, exist_ok=True)
    line = json.dumps(payload, ensure_ascii=False, default=str) + "\n"
    with path.open("a", encoding="utf-8", newline="\n") as handle:
        handle.write(line)
        handle.flush()
        os.fsync(handle.fileno())


def _fsync_directory(path: Path) -> None:
    if os.name == "nt":
        return
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _replace_with_bounded_retry(source: Path, target: Path) -> None:
    """Tolerate short-lived Windows sharing violations from checkpoint readers."""

    for attempt in range(_REPLACE_RETRIES + 1):
        try:
            os.replace(source, target)
            return
        except PermissionError:
            if attempt == _REPLACE_RETRIES:
                raise
            time.sleep(
                min(
                    _REPLACE_BASE_DELAY_SECONDS * (2**attempt),
                    0.4,
                )
            )
