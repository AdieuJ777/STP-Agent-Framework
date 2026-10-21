"""事件存储：内存、JSON Lines、SQLite。"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from .models import ExecutionEvent


def _to_json(event: ExecutionEvent) -> str:
    return event.model_dump_json()


def _from_json(raw: str) -> ExecutionEvent:
    return ExecutionEvent.model_validate_json(raw)


class InMemoryEventStore:
    def __init__(self) -> None:
        self._events: list[ExecutionEvent] = []

    def append(self, event: ExecutionEvent) -> None:
        self._events.append(event)

    def events(self, run_id: str | None = None) -> list[ExecutionEvent]:
        if run_id is None:
            return list(self._events)
        return [event for event in self._events if event.run_id == run_id]

    def close(self) -> None:
        return None


class JsonlEventStore:
    """追加写 JSON Lines；每次 append 后 flush，便于崩溃后排查。"""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fh = self.path.open("a", encoding="utf-8")

    def append(self, event: ExecutionEvent) -> None:
        self._fh.write(_to_json(event) + "\n")
        self._fh.flush()

    def events(self, run_id: str | None = None) -> list[ExecutionEvent]:
        result: list[ExecutionEvent] = []
        if not self.path.exists():
            return result
        with self.path.open("r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                event = _from_json(line)
                if run_id is None or event.run_id == run_id:
                    result.append(event)
        return result

    def close(self) -> None:
        self._fh.close()


class SqliteEventStore:
    """简单的事件表；payload 以 JSON 文本保存。"""

    def __init__(self, path: str | Path = ":memory:") -> None:
        self.path = str(path)
        self._conn = sqlite3.connect(self.path)
        self._conn.execute(
            """
            CREATE TABLE IF NOT EXISTS execution_events (
                event_id TEXT PRIMARY KEY,
                run_id TEXT NOT NULL,
                seq INTEGER NOT NULL,
                step_index INTEGER NOT NULL,
                attempt INTEGER NOT NULL,
                event_type TEXT NOT NULL,
                payload TEXT NOT NULL,
                timestamp TEXT NOT NULL,
                raw TEXT NOT NULL
            )
            """
        )
        self._conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_events_run_seq ON execution_events(run_id, seq)"
        )
        self._conn.commit()

    def append(self, event: ExecutionEvent) -> None:
        self._conn.execute(
            """
            INSERT INTO execution_events
            (event_id, run_id, seq, step_index, attempt, event_type, payload, timestamp, raw)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                event.event_id,
                event.run_id,
                event.seq,
                event.step_index,
                event.attempt,
                event.event_type.value,
                json.dumps(event.payload, ensure_ascii=False, default=str),
                event.timestamp.isoformat(),
                _to_json(event),
            ),
        )
        self._conn.commit()

    def events(self, run_id: str | None = None) -> list[ExecutionEvent]:
        if run_id is None:
            rows = self._conn.execute(
                "SELECT raw FROM execution_events ORDER BY run_id, seq"
            ).fetchall()
        else:
            rows = self._conn.execute(
                "SELECT raw FROM execution_events WHERE run_id = ? ORDER BY seq", (run_id,)
            ).fetchall()
        return [_from_json(row[0]) for row in rows]

    def close(self) -> None:
        self._conn.close()
