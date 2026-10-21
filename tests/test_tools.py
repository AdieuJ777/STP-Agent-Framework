import asyncio

import pytest

from kkk import (
    CalculatorTool,
    ContractValidationError,
    OutputValidationError,
    Tool,
    ToolCallContext,
    ToolExecutionError,
    ToolRegistry,
    ToolSpec,
)
from kkk.tools import safe_calculate


def _context():
    return ToolCallContext(
        run_id="run",
        step_index=0,
        attempt=1,
        call_id="call",
        idempotency_key="run:call",
    )


def test_safe_calculator():
    assert safe_calculate("2 + 3 * 4") == 14
    assert safe_calculate("sqrt(16)") == 4
    with pytest.raises(Exception):
        safe_calculate("__import__('os').system('echo hi')")


def test_calculator_tool_success():
    registry = ToolRegistry([CalculatorTool()])
    result = asyncio.run(registry.execute("calculator", {"expression": "sqrt(16) + 2 * 3"}, _context()))
    assert result == {"result": 10}


def test_tool_input_schema_validation():
    registry = ToolRegistry([CalculatorTool()])
    with pytest.raises(ContractValidationError):
        asyncio.run(registry.execute("calculator", {}, _context()))


class _BadOutputTool(Tool):
    spec = ToolSpec(
        tool_name="bad_output",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )

    async def execute(self, input_data, context):
        return {"wrong": "shape"}


def test_tool_output_schema_validation():
    registry = ToolRegistry([_BadOutputTool()])
    with pytest.raises(OutputValidationError):
        asyncio.run(registry.execute("bad_output", {}, _context()))


def test_tool_allowlist():
    registry = ToolRegistry([CalculatorTool()], allowed_tools={"other"})
    with pytest.raises(ToolExecutionError):
        asyncio.run(registry.execute("calculator", {"expression": "1+1"}, _context()))


class _KeyTool(Tool):
    spec = ToolSpec(
        tool_name="key_tool",
        input_schema={"type": "object", "properties": {}, "additionalProperties": False},
        output_schema={"type": "object", "properties": {"key": {"type": "string"}}, "required": ["key"]},
        supports_idempotency_key=True,
    )

    def __init__(self):
        self.seen_key = None

    async def execute(self, input_data, context):
        self.seen_key = context.idempotency_key
        return {"key": context.idempotency_key}


def test_idempotency_key_is_propagated():
    tool = _KeyTool()
    registry = ToolRegistry([tool])
    result = asyncio.run(registry.execute("key_tool", {}, _context()))
    assert tool.seen_key == "run:call"
    assert result == {"key": "run:call"}
