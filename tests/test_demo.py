import asyncio

from kkk import MockLLM
from kkk.demo import accounting
from kkk.demo.accounting import default_ollama_backend, run_demo
from kkk.llm import OpenAICompatBackend


def test_application_layer_does_not_create_mockllm():
    # MockLLM 属于测试层；Agent 应用层不应导入或创建它。
    assert "MockLLM" not in vars(accounting)
    assert "DEFAULT_MOCK_RESPONSE" not in vars(accounting)


def test_default_backend_is_ollama(monkeypatch):
    monkeypatch.delenv("KKK_LLM_MODEL", raising=False)
    monkeypatch.delenv("KKK_LLM_BASE_URL", raising=False)
    backend = default_ollama_backend()
    assert isinstance(backend, OpenAICompatBackend)
    assert backend.base_url == "http://localhost:11434/v1"
    assert backend.model == "qwen3:8b"


def test_accounting_demo_routes_to_approval():
    result = asyncio.run(
        run_demo(
            "机票 1200 元",
            backend=MockLLM(
                [
                    {
                        "amount": 1200,
                        "category": "travel",
                        "vendor": "Airline",
                        "description": "flight",
                    }
                ]
            ),
        )
    )
    assert result["status"] == "SUCCEEDED"
    assert result["backend"] == "MockLLM"
    assert result["model"] == "mock-model"
    assert result["output"]["status"] == "pending_approval"
    assert result["approvals"]
    assert not result["ledger"]
    assert "TASK_COMPLETED" in result["events"]


def test_accounting_demo_routes_to_record():
    result = asyncio.run(
        run_demo(
            "午餐 120 元",
            backend=MockLLM(
                [
                    {
                        "amount": 120,
                        "category": "meal",
                        "vendor": "Cafe",
                        "description": "lunch",
                    }
                ]
            ),
        )
    )
    assert result["status"] == "SUCCEEDED"
    assert result["output"]["status"] == "recorded"
    assert result["ledger"]
    assert not result["approvals"]


def test_expense_id_is_stable_for_same_text():
    response = {
        "amount": 120,
        "category": "meal",
        "vendor": "Cafe",
        "description": "lunch",
    }
    first = asyncio.run(
        run_demo("same text", backend=MockLLM([response]))
    )
    second = asyncio.run(
        run_demo("same text", backend=MockLLM([response]))
    )
    assert first["output"]["result"]["expense_id"] == second["output"]["result"]["expense_id"]
