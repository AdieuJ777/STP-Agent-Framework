"""声明式脚本 API。

脚本函数在构建期执行，只登记步骤和依赖，不执行任何工具或模型调用。
``ctx.step()`` 返回 ``StepRef``；运行期分支用 ``ctx.branch()`` 表达。
"""
from __future__ import annotations

import inspect
from typing import Any, Callable

from .errors import UnsupportedScriptError
from .models import TaskSpec
from .plan import ConditionSpec, PlanBuilder, PlanNode, StepAction
from .refs import Ref, RefNamespace
from .schema import schema_properties


def _normalise_llm(llm: Any) -> list[dict[str, Any]]:
    if isinstance(llm, str):
        return [{"role": "user", "content": llm}]
    if isinstance(llm, list) and all(isinstance(item, dict) for item in llm):
        return [dict(item) for item in llm]
    raise UnsupportedScriptError(
        "llm argument must be a prompt string or a list of message dicts"
    )


class BuildContext:
    def __init__(
        self,
        builder: PlanBuilder,
        spec: TaskSpec,
        *,
        is_main: bool = False,
    ) -> None:
        self.builder = builder
        self.spec = spec
        self.is_main = is_main
        self.entry: str | None = None
        self.cursor: str | None = None
        self.ended = False

    @property
    def input(self) -> RefNamespace:
        return RefNamespace("input")

    @property
    def state(self) -> RefNamespace:
        return RefNamespace("state")

    def _attach(self, node: PlanNode) -> None:
        self.builder.add_node(node)
        if self.cursor is None:
            if self.is_main and self.builder.entry is None:
                self.builder.set_entry(node.id)
            if self.entry is None:
                self.entry = node.id
        else:
            self.builder.link(self.cursor, node.id)
        self.cursor = node.id

    def step(
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
    ) -> Ref:
        supplied = [tool is not None, func is not None, llm is not None]
        if sum(supplied) != 1:
            raise UnsupportedScriptError(
                f"step {name!r} must specify exactly one of tool=, func=, llm="
            )
        if tool is not None:
            action = StepAction(kind="tool", tool_name=tool)
        elif func is not None:
            action = StepAction(kind="function", func=func, func_name=getattr(func, "__name__", None))
        else:
            action = StepAction(
                kind="llm",
                llm_messages=_normalise_llm(llm),
                llm_output_schema=output_schema,
            )

        key = output_key or name
        node = PlanNode(
            id=self.builder._new_id(name),
            kind="step",
            name=name,
            action=action,
            output_key=key,
            input_mapping=input or {},
            output_schema=output_schema,
            timeout_seconds=timeout_seconds,
        )
        self._attach(node)
        return Ref("step", key)

    def branch(
        self,
        name: str,
        *,
        condition: Callable[[dict[str, Any]], str],
        cases: dict[str, Callable[["BuildContext"], None]],
        default: str,
    ) -> None:
        if not cases:
            raise UnsupportedScriptError(f"branch {name!r} must define at least one case")
        if default not in cases:
            raise UnsupportedScriptError(
                f"branch {name!r} default {default!r} must be one of {sorted(cases)}"
            )

        branch = PlanNode(
            id=self.builder._new_id(name),
            kind="branch",
            name=name,
            condition=ConditionSpec(name=name, fn=condition),
            default_route=default,
        )
        self._attach(branch)

        children: dict[str, BuildContext] = {}
        tails: dict[str, str] = {}
        for label, case_fn in cases.items():
            child = BuildContext(self.builder, self.spec, is_main=False)
            case_fn(child)
            if child.ended:
                raise UnsupportedScriptError(
                    f"branch case {label!r} cannot call ctx.end(); "
                    "let the branch join and call end after it"
                )
            children[label] = child
            if child.cursor is not None:
                tails[label] = child.cursor

        join = PlanNode(
            id=self.builder._new_id(f"{name}_join"),
            kind="join",
            name=f"{name}_join",
        )
        self.builder.add_node(join)

        for label, child in children.items():
            if child.entry is None:
                branch.routes[label] = join.id
            else:
                branch.routes[label] = child.entry
        for label, tail in tails.items():
            self.builder.link(tail, join.id)

        self.cursor = join.id

    def end(self, output_mapping: dict[str, Any] | None = None) -> None:
        if self.cursor is None:
            raise UnsupportedScriptError("cannot end an empty plan")
        if self.ended:
            raise UnsupportedScriptError("ctx.end() was already called")
        if output_mapping is None:
            output_mapping = {
                key: Ref("state", key) for key in schema_properties(self.spec.output_schema)
            }
        node = PlanNode(
            id=self.builder._new_id("end"),
            kind="end",
            name="end",
            output_mapping=output_mapping,
        )
        self.builder.add_node(node)
        self.builder.link(self.cursor, node.id)
        self.cursor = node.id
        self.ended = True

    def build(self) -> Any:
        if self.cursor is None:
            raise UnsupportedScriptError("script produced an empty plan")
        if not self.ended:
            self.end()
        return self.builder.build()


class ScriptTask:
    def __init__(self, spec: TaskSpec, fn: Callable[[BuildContext], None]) -> None:
        self.spec = spec
        self.fn = fn

    def build(self):
        builder = PlanBuilder(self.spec.task_id, self.spec.protocol_version)
        ctx = BuildContext(builder, self.spec, is_main=True)
        result = self.fn(ctx)
        if inspect.isawaitable(result):
            raise UnsupportedScriptError(
                "script builder must be synchronous at build time; "
                "use ctx.step() to declare async work"
            )
        return ctx.build()

    async def run(self, input_data: dict[str, Any], *, runtime=None, **kwargs: Any):
        from .executor import TaskRuntime

        runtime = runtime or TaskRuntime()
        return await runtime.run(self.spec, self.build(), input_data, **kwargs)


def task(spec: TaskSpec):
    def decorator(fn: Callable[[BuildContext], None]) -> ScriptTask:
        return ScriptTask(spec, fn)

    return decorator
