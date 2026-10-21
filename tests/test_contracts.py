import pytest

from kkk import (
    ContractValidationError,
    OutputValidationError,
    TaskRun,
    TaskRuntime,
    TaskSpec,
    TaskStatus,
    task,
)
from kkk.errors import InvalidStateTransitionError


def test_task_spec_rejects_unknown_fields():
    with pytest.raises(Exception):
        TaskSpec(task_id="TSK-1", name="x", input_schema={"type": "object"}, unknown=1)


def test_task_spec_requires_tsk_prefix():
    with pytest.raises(Exception):
        TaskSpec(task_id="BAD-1", name="x")


def test_task_status_transitions():
    run = TaskRun(task_spec_id="TSK-1", input={})
    run.transition_to(TaskStatus.RUNNING)
    run.transition_to(TaskStatus.SUCCEEDED)
    with pytest.raises(InvalidStateTransitionError):
        run.transition_to(TaskStatus.RUNNING)


def test_input_contract_failure():
    spec = TaskSpec(
        task_id="TSK-INPUT",
        name="input",
        input_schema={
            "type": "object",
            "properties": {"x": {"type": "integer"}},
            "required": ["x"],
            "additionalProperties": False,
        },
        output_schema={"type": "object"},
    )

    @task(spec)
    def build(ctx):
        ctx.step("noop", func=lambda data: {"ok": True}, input={}, output_key="v")
        ctx.end({})

    result = __import__("asyncio").run(build.run({}, runtime=TaskRuntime()))
    assert result.status == TaskStatus.FAILED
    assert result.error is not None
    assert result.error.code == "E_CONTRACT_INPUT"


def test_output_contract_failure():
    spec = TaskSpec(
        task_id="TSK-OUTPUT",
        name="output",
        input_schema={"type": "object"},
        output_schema={
            "type": "object",
            "properties": {"value": {"type": "integer"}},
            "required": ["value"],
            "additionalProperties": False,
        },
    )

    @task(spec)
    def build(ctx):
        ctx.step("noop", func=lambda data: {"ok": True}, input={}, output_key="v")
        ctx.end({"value": "not-an-int"})

    result = __import__("asyncio").run(build.run({}, runtime=TaskRuntime()))
    assert result.status == TaskStatus.FAILED
    assert result.error is not None
    assert result.error.code == "E_CONTRACT_OUTPUT"


def test_output_validation_error_is_framework_error():
    assert issubclass(OutputValidationError, Exception)
    assert issubclass(ContractValidationError, Exception)
