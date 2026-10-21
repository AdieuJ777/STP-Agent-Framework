"""显式图 API：节点、边、分支路由。"""
from __future__ import annotations

from typing import Any, Callable

from .errors import PlanValidationError
from .models import TaskSpec
from .plan import ConditionSpec, PlanBuilder, PlanNode, StepAction
from .refs import Ref
from .schema import schema_properties


def _messages(llm: str | list[dict[str, Any]]) -> list[dict[str, Any]]:
    if isinstance(llm, str):
        return [{"role": "user", "content": llm}]
    if isinstance(llm, list) and all(isinstance(item, dict) for item in llm):
        return [dict(item) for item in llm]
    raise PlanValidationError("llm must be a prompt string or a list of message dicts")


class GraphBuilder:
    def __init__(self, spec: TaskSpec) -> None:
        self.spec = spec
        self._builder = PlanBuilder(spec.task_id, spec.protocol_version)

    def add_step(
        self,
        name: str,
        *,
        tool: str | None = None,
        func: Callable[..., Any] | None = None,
        llm: str | list[dict[str, Any]] | None = None,
        input: dict[str, Any] | None = None,
        output_key: str | None = None,
        output_schema: dict[str, Any] | None = None,
        timeout_seconds: float | None = None,
    ) -> str:
        supplied = [tool is not None, func is not None, llm is not None]
        if sum(supplied) != 1:
            raise PlanValidationError(
                f"step {name!r} must specify exactly one of tool=, func=, llm="
            )
        if tool is not None:
            action = StepAction(kind="tool", tool_name=tool)
        elif func is not None:
            action = StepAction(kind="function", func=func, func_name=getattr(func, "__name__", None))
        else:
            action = StepAction(
                kind="llm",
                llm_messages=_messages(llm),  # type: ignore[arg-type]
                llm_output_schema=output_schema,
            )
        node = PlanNode(
            id=self._builder._new_id(name),
            kind="step",
            name=name,
            action=action,
            output_key=output_key or name,
            input_mapping=input or {},
            output_schema=output_schema,
            timeout_seconds=timeout_seconds,
        )
        self._builder.add_node(node)
        return node.id

    def add_branch(
        self,
        name: str,
        *,
        condition: Callable[[dict[str, Any]], str],
        default: str,
    ) -> str:
        node = PlanNode(
            id=self._builder._new_id(name),
            kind="branch",
            name=name,
            condition=ConditionSpec(name=name, fn=condition),
            default_route=default,
        )
        self._builder.add_node(node)
        return node.id

    def add_join(self, name: str) -> str:
        node = PlanNode(id=self._builder._new_id(name), kind="join", name=name)
        self._builder.add_node(node)
        return node.id

    def add_end(self, name: str = "end", output_mapping: dict[str, Any] | None = None) -> str:
        if output_mapping is None:
            output_mapping = {
                key: Ref("state", key) for key in schema_properties(self.spec.output_schema)
            }
        node = PlanNode(
            id=self._builder._new_id(name),
            kind="end",
            name=name,
            output_mapping=output_mapping,
        )
        self._builder.add_node(node)
        return node.id

    def set_entry(self, node_id: str) -> None:
        self._builder.set_entry(node_id)

    def add_edge(self, source_id: str, target_id: str) -> None:
        self._builder.link(source_id, target_id)

    def add_route(self, branch_id: str, label: str, target_id: str) -> None:
        self._builder.add_route(branch_id, label, target_id)

    def compile(self):
        return self._builder.build()


class GraphTask:
    def __init__(self, spec: TaskSpec, builder: GraphBuilder) -> None:
        self.spec = spec
        self.builder = builder

    def build(self):
        return self.builder.compile()

    async def run(self, input_data: dict[str, Any], *, runtime=None, **kwargs: Any):
        from .executor import TaskRuntime

        runtime = runtime or TaskRuntime()
        return await runtime.run(self.spec, self.build(), input_data, **kwargs)
