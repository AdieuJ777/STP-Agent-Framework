"""构建期的符号引用。

脚本模式在构建期不执行步骤，因此 ``ctx.input["x"]``、``ctx.state["y"]`` 和
``ctx.step(...)`` 返回的都是引用对象，由运行时解析成实际值。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

RefSource = Literal["input", "state", "step"]


@dataclass(frozen=True)
class Ref:
    source: RefSource
    key: str
    path: tuple[Any, ...] = ()

    def __getitem__(self, item: Any) -> "Ref":
        return Ref(self.source, self.key, self.path + (item,))

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        suffix = "".join(f"[{p!r}]" for p in self.path)
        return f"{self.source}[{self.key!r}]{suffix}"


class RefNamespace:
    """让 ``ctx.input["x"]`` / ``ctx.state["x"]`` 的写法自然可用。"""

    def __init__(self, source: RefSource) -> None:
        self._source = source

    def __getitem__(self, key: str) -> Ref:
        return Ref(self._source, key)


def resolve(value: Any, input_data: dict[str, Any], state: dict[str, Any]) -> Any:
    """递归解析引用；普通值原样返回。"""
    if isinstance(value, Ref):
        base = input_data[value.key] if value.source == "input" else state[value.key]
        for part in value.path:
            base = base[part]
        return base
    if isinstance(value, dict):
        return {k: resolve(v, input_data, state) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [resolve(v, input_data, state) for v in value]
    return value
