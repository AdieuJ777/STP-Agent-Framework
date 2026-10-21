"""记账 Agent 示例。

任务流程：
1. LLM 从自然语言中抽取金额、类别、商户；
2. 查询该类别剩余预算；
3. 金额超过预算则申请审批，否则直接记账；
4. 输出结构化结果。

默认且唯一使用本地 Ollama 的 ``qwen3:8b``（OpenAI 兼容接口）。
MockLLM 只属于测试层，不进入 Agent 应用；离线验证请运行 pytest。
"""
from __future__ import annotations

import argparse
import asyncio
import hashlib
import os
import sys
from pathlib import Path
from typing import Any
from uuid import uuid4

# 允许两种运行方式：
#   python -m kkk.demo.accounting
#   python kkk/demo/accounting.py
# 直接运行脚本时，把项目根目录加入 sys.path，再使用绝对导入。
if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from kkk.llm import LLMBackend, OpenAICompatBackend
from kkk.models import TaskSpec, ToolSpec
from kkk.script import BuildContext, task
from kkk.stores import InMemoryEventStore
from kkk.tools import Tool, ToolCallContext, ToolRegistry


EXPENSE_SCHEMA = {
    "type": "object",
    "properties": {
        "amount": {"type": "number", "minimum": 0},
        "category": {
            "type": "string",
            "enum": ["travel", "meal", "office", "other"],
        },
        "vendor": {"type": "string"},
        "description": {"type": "string"},
    },
    "required": ["amount", "category", "vendor", "description"],
    "additionalProperties": False,
}

DEFAULT_OLLAMA_BASE_URL = "http://localhost:11434/v1"
DEFAULT_OLLAMA_MODEL = "qwen3:8b"


def default_ollama_backend() -> LLMBackend:
    """本地 Ollama 的 OpenAI 兼容后端。"""
    return OpenAICompatBackend(
        model=os.getenv("KKK_LLM_MODEL", DEFAULT_OLLAMA_MODEL),
        api_key=os.getenv("KKK_LLM_API_KEY", "ollama"),
        base_url=os.getenv("KKK_LLM_BASE_URL", DEFAULT_OLLAMA_BASE_URL),
        timeout=float(os.getenv("KKK_LLM_TIMEOUT", "120")),
    )

OUTPUT_SCHEMA = {
    "type": "object",
    "properties": {
        "status": {"type": "string"},
        "expense": {"type": "object"},
        "result": {"type": "object"},
    },
    "required": ["status", "expense", "result"],
    "additionalProperties": False,
}


class ExpenseStore:
    def __init__(self) -> None:
        self.budgets: dict[str, float] = {"travel": 500.0, "meal": 300.0, "office": 1000.0}
        self.ledger: dict[str, dict[str, Any]] = {}
        self.approvals: dict[str, dict[str, Any]] = {}

    def budget_for(self, category: str) -> float:
        return float(self.budgets.get(category, 0.0))


class QueryBudgetTool(Tool):
    def __init__(self, store: ExpenseStore) -> None:
        self.store = store
        self.spec = ToolSpec(
            tool_name="query_budget",
            description="查询某费用类别的剩余预算",
            input_schema={
                "type": "object",
                "properties": {"category": {"type": "string"}},
                "required": ["category"],
                "additionalProperties": False,
            },
            output_schema={
                "type": "object",
                "properties": {
                    "category": {"type": "string"},
                    "remaining": {"type": "number"},
                },
                "required": ["category", "remaining"],
                "additionalProperties": False,
            },
            side_effect=False,
            idempotent=True,
            timeout_seconds=5.0,
        )

    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        category = input_data["category"]
        return {"category": category, "remaining": self.store.budget_for(category)}


class RecordExpenseTool(Tool):
    def __init__(self, store: ExpenseStore) -> None:
        self.store = store
        self.spec = ToolSpec(
            tool_name="record_expense",
            description="把一笔费用写入账本；按 expense_id 幂等",
            input_schema={
                "type": "object",
                "properties": {
                    "expense_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "category": {"type": "string"},
                    "vendor": {"type": "string"},
                },
                "required": ["expense_id", "amount", "category", "vendor"],
                "additionalProperties": False,
            },
            output_schema={
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "ledger_id": {"type": "string"},
                    "expense_id": {"type": "string"},
                },
                "required": ["status", "ledger_id", "expense_id"],
                "additionalProperties": False,
            },
            side_effect=True,
            idempotent=True,
            supports_idempotency_key=True,
            timeout_seconds=5.0,
        )

    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        expense_id = input_data["expense_id"]
        existing = self.store.ledger.get(expense_id)
        if existing is not None:
            return existing
        record = {
            "status": "recorded",
            "ledger_id": f"LED-{uuid4().hex[:8]}",
            "expense_id": expense_id,
        }
        self.store.ledger[expense_id] = record
        return record


class RequestApprovalTool(Tool):
    def __init__(self, store: ExpenseStore) -> None:
        self.store = store
        self.spec = ToolSpec(
            tool_name="request_approval",
            description="为超预算费用发起审批；按 expense_id 幂等",
            input_schema={
                "type": "object",
                "properties": {
                    "expense_id": {"type": "string"},
                    "amount": {"type": "number"},
                    "category": {"type": "string"},
                    "reason": {"type": "string"},
                },
                "required": ["expense_id", "amount", "category", "reason"],
                "additionalProperties": False,
            },
            output_schema={
                "type": "object",
                "properties": {
                    "status": {"type": "string"},
                    "request_id": {"type": "string"},
                    "expense_id": {"type": "string"},
                },
                "required": ["status", "request_id", "expense_id"],
                "additionalProperties": False,
            },
            side_effect=True,
            idempotent=True,
            supports_idempotency_key=True,
            timeout_seconds=5.0,
        )

    async def execute(self, input_data: dict[str, Any], context: ToolCallContext) -> dict[str, Any]:
        expense_id = input_data["expense_id"]
        existing = self.store.approvals.get(expense_id)
        if existing is not None:
            return existing
        record = {
            "status": "pending_approval",
            "request_id": f"APR-{uuid4().hex[:8]}",
            "expense_id": expense_id,
        }
        self.store.approvals[expense_id] = record
        return record


def _stable_expense_id(text: str) -> str:
    return "EXP-" + hashlib.sha256(text.encode("utf-8")).hexdigest()[:10]


def build_accounting_task():
    spec = TaskSpec(
        task_id="TSK-ACCOUNTING",
        name="accounting_agent",
        description="从自然语言中抽取费用并决定记账或审批",
        input_schema={
            "type": "object",
            "properties": {"text": {"type": "string", "minLength": 1}},
            "required": ["text"],
            "additionalProperties": False,
        },
        output_schema=OUTPUT_SCHEMA,
        max_steps=10,
        max_model_calls=2,
        timeout_seconds=30.0,
        retry_policy={"max_attempts": 2, "backoff_factor": 1.0, "initial_delay_seconds": 0.001},
    )

    @task(spec)
    def build(ctx: BuildContext) -> None:
        ctx.step(
            "make_id",
            func=lambda data: {"expense_id": _stable_expense_id(data["text"])},
            input={"text": ctx.input["text"]},
            output_key="meta",
        )
        ctx.step(
            "extract",
            llm=[
                {
                    "role": "system",
                    "content": (
                        "你是记账助手。请从用户描述中抽取费用信息，只输出一个 JSON 对象，"
                        "不要输出 markdown 代码块，不要输出思考过程。\n"
                        "字段要求：\n"
                        "- amount：数字，单位元；\n"
                        "- category：只能是 travel、meal、office、other 之一；\n"
                        "- vendor：商户或航空公司名称；\n"
                        "- description：简短描述。\n"
                        '示例：{"amount": 1200, "category": "travel", '
                        '"vendor": "Airline", "description": "flight"}'
                    ),
                },
                {"role": "user", "content": ctx.input["text"]},
            ],
            output_schema=EXPENSE_SCHEMA,
            output_key="expense",
        )
        ctx.step(
            "check_budget",
            tool="query_budget",
            input={"category": ctx.state["expense"]["category"]},
            output_key="budget",
        )

        def approval_case(c: BuildContext) -> None:
            c.step(
                "request_approval",
                tool="request_approval",
                input={
                    "expense_id": c.state["meta"]["expense_id"],
                    "amount": c.state["expense"]["amount"],
                    "category": c.state["expense"]["category"],
                    "reason": "金额超过剩余预算",
                },
                output_key="result",
            )

        def record_case(c: BuildContext) -> None:
            c.step(
                "record_expense",
                tool="record_expense",
                input={
                    "expense_id": c.state["meta"]["expense_id"],
                    "amount": c.state["expense"]["amount"],
                    "category": c.state["expense"]["category"],
                    "vendor": c.state["expense"]["vendor"],
                },
                output_key="result",
            )

        ctx.branch(
            "route",
            condition=lambda state: (
                "approval"
                if state["expense"]["amount"] > state["budget"]["remaining"]
                else "record"
            ),
            cases={"approval": approval_case, "record": record_case},
            default="record",
        )
        ctx.end(
            {
                "status": ctx.state["result"]["status"],
                "expense": ctx.state["expense"],
                "result": ctx.state["result"],
            }
        )

    return build


async def run_demo(
    text: str,
    *,
    backend: LLMBackend | None = None,
) -> dict[str, Any]:
    """运行记账 Agent。

    ``backend`` 只用于测试注入；不传时始终使用本地 Ollama。
    应用层不创建、也不回退到 MockLLM。
    """
    if backend is None:
        backend = default_ollama_backend()

    store = ExpenseStore()
    tools = ToolRegistry(
        [
            QueryBudgetTool(store),
            RecordExpenseTool(store),
            RequestApprovalTool(store),
        ]
    )
    task_obj = build_accounting_task()
    from kkk.executor import TaskRuntime

    runtime = TaskRuntime(backend=backend, tools=tools, event_store=InMemoryEventStore())
    run = await task_obj.run({"text": text}, runtime=runtime)
    return {
        "status": run.status.value,
        "backend": type(backend).__name__,
        "model": getattr(backend, "model", None) or getattr(backend, "model_name", None),
        "output": run.output,
        "error": run.error.model_dump() if run.error else None,
        "ledger": store.ledger,
        "approvals": store.approvals,
        "events": [event.event_type.value for event in runtime.last_events],
    }


def main() -> None:
    # Windows 控制台默认代码页可能不是 UTF-8，避免中文和错误信息乱码。
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")

    parser = argparse.ArgumentParser(description="记账 Agent 示例（真实模型）")
    parser.add_argument("text", nargs="?", default="昨天从北京飞上海，机票 1200 元，走差旅报销。")
    args = parser.parse_args()

    try:
        result = asyncio.run(run_demo(args.text))
    except Exception as exc:
        print(f"运行失败：{exc}", file=sys.stderr)
        print(
            "请确认：1) `ollama serve` 已启动；2) 已执行 `ollama pull qwen3:8b`；"
            "3) `KKK_LLM_BASE_URL` / `KKK_LLM_MODEL` 配置正确。",
            file=sys.stderr,
        )
        raise SystemExit(1) from exc

    import json

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))

    if result["status"] != "SUCCEEDED":
        print(
            "Agent 执行失败，未产生有效结果。请根据上面的 error 字段排查模型服务；"
            "本 Demo 不会回退到 MockLLM。",
            file=sys.stderr,
        )
        raise SystemExit(1)


if __name__ == "__main__":
    main()
