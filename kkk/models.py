"""核心数据契约：TaskSpec / TaskRun / ExecutionEvent / ToolSpec。"""
from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Callable, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, field_validator

from .errors import InvalidStateTransitionError


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Constraint(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str
    value: Any = None


class RetryPolicy(BaseModel):
    model_config = ConfigDict(extra="forbid")

    max_attempts: int = 2
    backoff_factor: float = 2.0
    retryable_errors: list[str] = Field(default_factory=list)
    initial_delay_seconds: float = 0.05
    max_delay_seconds: float = 5.0

    @field_validator("max_attempts")
    @classmethod
    def _positive_attempts(cls, v: int) -> int:
        if v < 1:
            raise ValueError("max_attempts must be >= 1")
        return v

    @field_validator("backoff_factor")
    @classmethod
    def _backoff(cls, v: float) -> float:
        if v < 1:
            raise ValueError("backoff_factor must be >= 1")
        return v


class TerminationCondition(BaseModel):
    model_config = ConfigDict(extra="forbid")

    type: Literal["steps", "time", "output_match"] = "steps"
    value: Any = None


class ValidatorRef(BaseModel):
    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    name: str
    func: Callable[[dict[str, Any]], None] = Field(exclude=True)


class TaskSpec(BaseModel):
    """一个任务“要做什么”的静态描述，与一次执行实例分离。"""

    model_config = ConfigDict(extra="forbid", arbitrary_types_allowed=True)

    task_id: str
    name: str
    description: str = ""
    protocol_version: str = "1.0"

    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})

    constraints: list[Constraint] = Field(default_factory=list)
    retry_policy: RetryPolicy = Field(default_factory=RetryPolicy)
    termination_condition: TerminationCondition = Field(default_factory=TerminationCondition)

    max_steps: int = 20
    timeout_seconds: float = 300.0
    max_model_calls: int = 20

    validators: list[ValidatorRef] = Field(default_factory=list)
    max_concurrency: int = 1

    @field_validator("task_id")
    @classmethod
    def _task_id_format(cls, v: str) -> str:
        if not v.startswith("TSK-"):
            raise ValueError('task_id must start with "TSK-"')
        return v

    @field_validator("max_steps", "max_model_calls")
    @classmethod
    def _non_negative(cls, v: int) -> int:
        if v < 0:
            raise ValueError("value must be >= 0")
        return v

    @field_validator("timeout_seconds")
    @classmethod
    def _positive_timeout(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("timeout_seconds must be > 0")
        return v

    @field_validator("max_concurrency")
    @classmethod
    def _single_concurrency(cls, v: int) -> int:
        if v != 1:
            raise ValueError("this prototype supports max_concurrency == 1 only")
        return v


class TaskStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    PAUSED = "PAUSED"
    RETRYING = "RETRYING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


TERMINAL_STATUSES = {TaskStatus.SUCCEEDED, TaskStatus.FAILED, TaskStatus.CANCELLED}

_ALLOWED_TRANSITIONS: dict[TaskStatus, set[TaskStatus]] = {
    TaskStatus.PENDING: {TaskStatus.RUNNING, TaskStatus.CANCELLED},
    TaskStatus.RUNNING: {
        TaskStatus.SUCCEEDED,
        TaskStatus.FAILED,
        TaskStatus.PAUSED,
        TaskStatus.RETRYING,
        TaskStatus.CANCELLED,
    },
    TaskStatus.PAUSED: {TaskStatus.RUNNING, TaskStatus.CANCELLED, TaskStatus.FAILED},
    TaskStatus.RETRYING: {TaskStatus.RUNNING, TaskStatus.FAILED, TaskStatus.CANCELLED},
}


class RunError(BaseModel):
    model_config = ConfigDict(extra="forbid")

    code: str
    message: str
    retryable: bool = False
    details: dict[str, Any] = Field(default_factory=dict)


class TaskRun(BaseModel):
    """TaskSpec 的一次具体执行。"""

    model_config = ConfigDict(validate_assignment=True, extra="forbid")

    run_id: str = Field(default_factory=lambda: uuid4().hex)
    task_spec_id: str
    status: TaskStatus = TaskStatus.PENDING
    input: dict[str, Any]
    output: dict[str, Any] | None = None
    attempt_count: int = 0
    model_call_count: int = 0
    last_checkpoint_id: str | None = None
    error: RunError | None = None
    created_at: datetime = Field(default_factory=utcnow)
    updated_at: datetime = Field(default_factory=utcnow)

    def transition_to(self, new_status: TaskStatus) -> None:
        if new_status == self.status:
            return
        if new_status not in _ALLOWED_TRANSITIONS.get(self.status, set()):
            raise InvalidStateTransitionError(
                f"illegal transition {self.status.value} -> {new_status.value}",
                details={"from": self.status.value, "to": new_status.value},
            )
        self.status = new_status
        self.updated_at = utcnow()


class EventType(str, Enum):
    TASK_STARTED = "TASK_STARTED"
    STEP_START = "STEP_START"
    STEP_END = "STEP_END"
    LLM_CALL_START = "LLM_CALL_START"
    LLM_CALL_END = "LLM_CALL_END"
    TOOL_CALL_START = "TOOL_CALL_START"
    TOOL_CALL_END = "TOOL_CALL_END"
    ERROR_OCCURRED = "ERROR_OCCURRED"
    RETRY_SCHEDULED = "RETRY_SCHEDULED"
    TASK_PAUSED = "TASK_PAUSED"
    TASK_RESUMED = "TASK_RESUMED"
    CHECKPOINT_SAVED = "CHECKPOINT_SAVED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_FAILED = "TASK_FAILED"
    TASK_CANCELLED = "TASK_CANCELLED"


class ExecutionEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str = Field(default_factory=lambda: uuid4().hex)
    run_id: str
    seq: int
    step_index: int = -1
    attempt: int = 0
    event_type: EventType
    payload: dict[str, Any] = Field(default_factory=dict)
    timestamp: datetime = Field(default_factory=utcnow)


class ToolSpec(BaseModel):
    """工具的能力与执行约束声明。"""

    model_config = ConfigDict(extra="forbid")

    tool_name: str
    tool_version: str = "1.0"
    description: str = ""
    input_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})
    output_schema: dict[str, Any] = Field(default_factory=lambda: {"type": "object"})

    side_effect: bool = False
    idempotent: bool = True
    supports_cancellation: bool = False
    supports_idempotency_key: bool = False
    allow_concurrent: bool = False
    required_permissions: list[str] | None = None
    timeout_seconds: float = 30.0

    @field_validator("timeout_seconds")
    @classmethod
    def _positive_timeout(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("timeout_seconds must be > 0")
        return v
