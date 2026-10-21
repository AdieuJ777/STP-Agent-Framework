"""事件总线：为一次 run 分配严格递增的 seq，并写入一个或多个 sink。"""
from __future__ import annotations

from typing import Any, Protocol

from .models import EventType, ExecutionEvent
from .redaction import redact


class EventSink(Protocol):
    def append(self, event: ExecutionEvent) -> None: ...

    def events(self, run_id: str | None = None) -> list[ExecutionEvent]: ...

    def close(self) -> None: ...


class EventBus:
    def __init__(
        self,
        run_id: str,
        sinks: list[EventSink] | None = None,
        *,
        task_id: str | None = None,
    ) -> None:
        self.run_id = run_id
        self.task_id = task_id
        self._seq = 0
        self._events: list[ExecutionEvent] = []
        self._sinks = list(sinks or [])
        # sink 失败不在这里抛出，避免中断执行或掩盖原始错误；
        # TaskRuntime 会在任务结束时把这些错误升级为失败。
        self.sink_errors: list[str] = []

    @property
    def events(self) -> list[ExecutionEvent]:
        return list(self._events)

    def emit(
        self,
        event_type: EventType,
        *,
        step_index: int = -1,
        step_id: str | None = None,
        attempt: int = 0,
        payload: dict[str, Any] | None = None,
        duration_ms: float | None = None,
    ) -> ExecutionEvent:
        self._seq += 1
        event = ExecutionEvent(
            run_id=self.run_id,
            task_id=self.task_id,
            seq=self._seq,
            step_index=step_index,
            step_id=step_id,
            attempt=attempt,
            event_type=event_type,
            payload=redact(payload or {}),
            duration_ms=duration_ms,
        )
        self._events.append(event)
        for sink in self._sinks:
            try:
                sink.append(event)
            except Exception as exc:
                self.sink_errors.append(
                    f"{type(sink).__name__} failed on {event_type.value}: {exc}"
                )
        return event
