"""统一错误体系。

每个错误都带一个稳定的 ``code``，运行时据此决定是否重试、终止状态和事件字段。
``retryable`` 只是默认值；实际重试还受 TaskSpec.retry_policy 和工具副作用声明约束。
"""
from __future__ import annotations

from typing import Any


class FrameworkError(Exception):
    """所有框架错误的基类。"""

    code: str = "E_FRAMEWORK"
    retryable: bool = False

    def __init__(
        self,
        message: str,
        *,
        details: dict[str, Any] | None = None,
        cause: BaseException | None = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.details = details or {}
        self.cause = cause

    def __str__(self) -> str:  # pragma: no cover - trivial
        return f"[{self.code}] {self.message}"


# --- 契约 / 计划 ---------------------------------------------------------
class ContractValidationError(FrameworkError):
    code = "E_CONTRACT_INPUT"
    retryable = False


class OutputValidationError(FrameworkError):
    code = "E_CONTRACT_OUTPUT"
    retryable = True


class PlanValidationError(FrameworkError):
    code = "E_PLAN_INVALID"
    retryable = False


class UnsupportedScriptError(PlanValidationError):
    code = "E_PLAN_INVALID"


class InvalidStateTransitionError(FrameworkError):
    code = "E_STATE_INVALID"
    retryable = False


# --- 执行限制 ------------------------------------------------------------
class MaxStepsExceededError(FrameworkError):
    code = "E_LIMIT_EXCEEDED"
    retryable = False


class MaxModelCallsExceededError(FrameworkError):
    code = "E_LIMIT_EXCEEDED"
    retryable = False


class StepTimeoutError(FrameworkError):
    code = "E_STEP_TIMEOUT"
    retryable = True


class TaskTimeoutError(FrameworkError):
    code = "E_TASK_TIMEOUT"
    retryable = False


# --- 外部调用 ------------------------------------------------------------
class ModelCallError(FrameworkError):
    code = "E_MODEL_CALL"
    retryable = True


class StructuredOutputError(FrameworkError):
    code = "E_STRUCTURED_OUTPUT"
    retryable = True


class ToolExecutionError(FrameworkError):
    code = "E_TOOL_CALL"
    retryable = True


class NonIdempotentRetryWarning(FrameworkError):
    code = "W_NON_IDEMPOTENT"
    retryable = False


class BusinessRuleViolationError(FrameworkError):
    code = "E_BUSINESS_RULE"
    retryable = False


class TaskCancelledError(FrameworkError):
    code = "E_CANCELLED"
    retryable = False


class BranchConditionError(FrameworkError):
    code = "E_BRANCH_CONDITION"
    retryable = False


class TerminationConditionError(FrameworkError):
    code = "E_TERMINATION"
    retryable = False


class ResumeNotSupportedError(FrameworkError):
    code = "E_RESUME_UNSUPPORTED"
    retryable = False
