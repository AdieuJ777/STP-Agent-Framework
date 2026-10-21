"""工具契约与工具注册表。"""
from __future__ import annotations

import ast
import asyncio
import inspect
import math
import operator
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import uuid4

from .errors import (
    ContractValidationError,
    FrameworkError,
    OutputValidationError,
    ToolExecutionError,
)
from .models import EventType, ExecutionEvent, ToolSpec
from .schema import validate_instance
from .tokens import CancellationToken


@dataclass
class ToolCallContext:
    run_id: str
    step_index: int
    attempt: int
    call_id: str
    idempotency_key: str
    cancellation: CancellationToken | None = None
    emit: Callable[[EventType, dict[str, Any]], ExecutionEvent] | None = None

    def event(self, event_type: EventType, payload: dict[str, Any]) -> None:
        if self.emit is not None:
            self.emit(event_type, payload)


class Tool(ABC):
    spec: ToolSpec

    @abstractmethod
    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        raise NotImplementedError


class FunctionTool(Tool):
    """把一个同步或异步 Python 函数包装成工具。"""

    def __init__(self, spec: ToolSpec, func: Callable[[dict[str, Any]], Any]) -> None:
        self.spec = spec
        self._func = func

    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        result = self._func(input_data)
        if inspect.isawaitable(result):
            result = await result
        return result


class ToolRegistry:
    def __init__(self, tools: list[Tool] | None = None, allowed_tools: set[str] | None = None) -> None:
        self._tools: dict[str, Tool] = {}
        self._allowed = allowed_tools
        for tool in tools or []:
            self.register(tool)

    def register(self, tool: Tool) -> None:
        name = tool.spec.tool_name
        if name in self._tools:
            raise ValueError(f"tool {name!r} is already registered")
        self._tools[name] = tool

    def get(self, name: str) -> Tool:
        try:
            return self._tools[name]
        except KeyError as exc:
            raise ToolExecutionError(
                f"tool {name!r} is not registered",
                details={"tool_name": name, "available": sorted(self._tools)},
            ) from exc

    def list_tools(self) -> list[str]:
        return sorted(self._tools)

    async def execute(
        self,
        name: str,
        input_data: dict[str, Any],
        context: ToolCallContext,
    ) -> dict[str, Any]:
        if self._allowed is not None and name not in self._allowed:
            raise ToolExecutionError(
                f"tool {name!r} is not in the allowlist",
                details={"tool_name": name},
            )
        tool = self.get(name)
        validate_instance(
            tool.spec.input_schema,
            input_data,
            error_cls=ContractValidationError,
            context=f"tool {name!r} input",
        )

        context.event(
            EventType.TOOL_CALL_START,
            {
                "tool_name": name,
                "tool_version": tool.spec.tool_version,
                "call_id": context.call_id,
                "idempotency_key": context.idempotency_key,
                "input": input_data,
                "side_effect": tool.spec.side_effect,
                "idempotent": tool.spec.idempotent,
            },
        )

        try:
            result = await asyncio.wait_for(
                tool.execute(input_data, context),
                timeout=tool.spec.timeout_seconds,
            )
            validate_instance(
                tool.spec.output_schema,
                result,
                error_cls=OutputValidationError,
                context=f"tool {name!r} output",
            )
        except asyncio.TimeoutError as exc:
            error = ToolExecutionError(
                f"tool {name!r} timed out after {tool.spec.timeout_seconds}s",
                details={"tool_name": name, "timeout": True},
                cause=exc,
            )
            context.event(
                EventType.TOOL_CALL_END,
                {
                    "tool_name": name,
                    "call_id": context.call_id,
                    "status": "error",
                    "error_code": error.code,
                    "message": str(error),
                },
            )
            raise error from exc
        except FrameworkError as exc:
            context.event(
                EventType.TOOL_CALL_END,
                {
                    "tool_name": name,
                    "call_id": context.call_id,
                    "status": "error",
                    "error_code": exc.code,
                    "message": str(exc),
                },
            )
            raise
        except Exception as exc:
            error = ToolExecutionError(
                f"tool {name!r} failed: {exc}",
                details={"tool_name": name},
                cause=exc,
            )
            context.event(
                EventType.TOOL_CALL_END,
                {
                    "tool_name": name,
                    "call_id": context.call_id,
                    "status": "error",
                    "error_code": error.code,
                    "message": str(error),
                },
            )
            raise error from exc

        context.event(
            EventType.TOOL_CALL_END,
            {
                "tool_name": name,
                "call_id": context.call_id,
                "status": "ok",
                "output": result,
            },
        )
        return result


# ---------------------------------------------------------------------------
# 示例工具：安全计算器
# ---------------------------------------------------------------------------
_BIN_OPS = {
    ast.Add: operator.add,
    ast.Sub: operator.sub,
    ast.Mult: operator.mul,
    ast.Div: operator.truediv,
    ast.Mod: operator.mod,
    ast.Pow: operator.pow,
}
_UNARY_OPS = {ast.UAdd: operator.pos, ast.USub: operator.neg}
_FUNCTIONS = {
    "sqrt": math.sqrt,
    "abs": abs,
    "round": round,
    "min": min,
    "max": max,
}
_CONSTANTS = {"pi": math.pi, "e": math.e}


def _eval_node(node: ast.AST) -> Any:
    if isinstance(node, ast.Constant) and isinstance(node.value, (int, float)):
        return node.value
    if isinstance(node, ast.BinOp) and type(node.op) in _BIN_OPS:
        return _BIN_OPS[type(node.op)](_eval_node(node.left), _eval_node(node.right))
    if isinstance(node, ast.UnaryOp) and type(node.op) in _UNARY_OPS:
        return _UNARY_OPS[type(node.op)](_eval_node(node.operand))
    if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
        func = _FUNCTIONS.get(node.func.id)
        if func is None:
            raise ValueError(f"unsupported function {node.func.id!r}")
        return func(*[_eval_node(arg) for arg in node.args])
    if isinstance(node, ast.Name) and node.id in _CONSTANTS:
        return _CONSTANTS[node.id]
    raise ValueError(f"unsupported expression element {type(node).__name__}")


def safe_calculate(expression: str) -> float | int:
    if not expression or not expression.strip():
        raise ValueError("expression must not be empty")
    tree = ast.parse(expression, mode="eval")
    return _eval_node(tree.body)


class CalculatorTool(Tool):
    spec = ToolSpec(
        tool_name="calculator",
        tool_version="1.0",
        description="安全计算器，支持 + - * / % **、sqrt/abs/round/min/max 和 pi/e",
        input_schema={
            "type": "object",
            "properties": {"expression": {"type": "string", "minLength": 1}},
            "required": ["expression"],
            "additionalProperties": False,
        },
        output_schema={
            "type": "object",
            "properties": {"result": {"type": ["number", "integer"]}},
            "required": ["result"],
            "additionalProperties": False,
        },
        side_effect=False,
        idempotent=True,
        timeout_seconds=5.0,
    )

    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        try:
            value = safe_calculate(input_data["expression"])
        except Exception as exc:
            raise ToolExecutionError(
                f"calculator failed: {exc}",
                details={"expression": input_data.get("expression")},
                cause=exc,
            ) from exc
        return {"result": value}
