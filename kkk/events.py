"""事件总线：为一次 run 分配严格递增的 seq，并写入一个或多个 sink。"""
from __future__ import annotations

from typing import Any, Protocol

from .models import EventType, ExecutionEvent


class EventSink(Protocol):
    def append(self, event: ExecutionEvent) -> None: ...

    def events(self, run_id: str | None = None) -> list[ExecutionEvent]: ...

    def close(self) -> None: ...


class EventBus:
    def __init__(self, run_id: str, sinks: list[EventSink] | None = None) -> None:
        self.run_id = run_id
        self._seq = 0
        self._events: list[ExecutionEvent] = []
        self._sinks = list(sinks or [])

    @property
    def events(self) -> list[ExecutionEvent]:
        return list(self._events)

    def emit(
        self,
        event_type: EventType,
        *,
        step_index: int = -1,
        attempt: int = 0,
        payload: dict[str, Any] | None = None,
    ) -> ExecutionEvent:
        self._seq += 1
        event = ExecutionEvent(
            run_id=self.run_id,
            seq=self._seq,
            step_index=step_index,
            attempt=attempt,
            event_type=event_type,
            payload=payload or {},
        )
        self._events.append(event)
        for sink in self._sinks:
            sink.append(event)
        return event
