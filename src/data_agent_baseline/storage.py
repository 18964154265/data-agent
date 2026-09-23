"""原子产物写入和任务事件记录。"""

from __future__ import annotations

import json
import os
import tempfile
import threading
from datetime import date, datetime, time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any
from uuid import UUID


def json_value(value: Any) -> Any:
    if isinstance(value, (date, datetime, time, timedelta, Decimal, UUID)):
        return str(value)
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, Path):
        return str(value)
    raise TypeError(f"不能序列化 {type(value).__name__}")


def atomic_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8", newline="") as handle:
            handle.write(text)
        os.replace(name, path)
    finally:
        Path(name).unlink(missing_ok=True)


def write_json(path: Path, payload: Any) -> None:
    atomic_text(path, json.dumps(payload, ensure_ascii=False, indent=2, default=json_value) + "\n")


class EventLog:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def emit(self, event: str, **fields: Any) -> None:
        record = {"time": datetime.now(timezone.utc).isoformat(), "event": event, **fields}
        with self._lock, self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, default=json_value) + "\n")
