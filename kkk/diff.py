"""双模式可观察结果比较。

比较的是归一化后的业务结果，而不是事件序列、节点名称或时间戳。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable

from .models import ExecutionEvent, EventType, TaskRun


@dataclass
class EquivalenceReport:
    equivalent: bool
    differences: list[str] = field(default_factory=list)
    observations: dict[str, Any] = field(default_factory=dict)

    def raise_if_different(self) -> None:
        if not self.equivalent:
            raise AssertionError("; ".join(self.differences))


def _retry_count(events: Iterable[ExecutionEvent]) -> int:
    return sum(1 for event in events if event.event_type == EventType.RETRY_SCHEDULED)


def _tool_sequence(events: Iterable[ExecutionEvent]) -> list[str]:
    return [
        str(event.payload.get("tool_name"))
        for event in events
        if event.event_type == EventType.TOOL_CALL_START
    ]


def compare_runs(
    left: TaskRun,
    right: TaskRun,
    *,
    left_events: Iterable[ExecutionEvent] | None = None,
    right_events: Iterable[ExecutionEvent] | None = None,
    strict_fields: Iterable[str] | None = None,
    compare_tool_sequence: bool = True,
) -> EquivalenceReport:
    differences: list[str] = []
    observations: dict[str, Any] = {
        "left_status": left.status.value,
        "right_status": right.status.value,
        "left_error": left.error.code if left.error else None,
        "right_error": right.error.code if right.error else None,
    }

    if left.status != right.status:
        differences.append(f"status differs: {left.status.value} != {right.status.value}")
    if (left.error.code if left.error else None) != (right.error.code if right.error else None):
        differences.append(
            f"error code differs: {observations['left_error']} != {observations['right_error']}"
        )

    fields = list(strict_fields) if strict_fields is not None else None
    left_output = left.output or {}
    right_output = right.output or {}
    if fields is None:
        fields = sorted(set(left_output) | set(right_output))
    for key in fields:
        if left_output.get(key) != right_output.get(key):
            differences.append(
                f"output field {key!r} differs: {left_output.get(key)!r} != {right_output.get(key)!r}"
            )

    if left_events is not None and right_events is not None:
        left_retries = _retry_count(left_events)
        right_retries = _retry_count(right_events)
        observations["left_retries"] = left_retries
        observations["right_retries"] = right_retries
        if left_retries != right_retries:
            differences.append(f"retry count differs: {left_retries} != {right_retries}")
        if compare_tool_sequence:
            left_tools = _tool_sequence(left_events)
            right_tools = _tool_sequence(right_events)
            observations["left_tools"] = left_tools
            observations["right_tools"] = right_tools
            if left_tools != right_tools:
                differences.append(f"tool call sequence differs: {left_tools} != {right_tools}")

    return EquivalenceReport(equivalent=not differences, differences=differences, observations=observations)
