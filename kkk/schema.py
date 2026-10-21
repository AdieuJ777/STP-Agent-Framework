"""JSON Schema 校验的薄封装。"""
from __future__ import annotations

from typing import Any

from jsonschema import Draft202012Validator
from jsonschema.exceptions import SchemaError, ValidationError

from .errors import ContractValidationError, FrameworkError, PlanValidationError


def check_schema(schema: dict[str, Any], *, context: str = "") -> None:
    try:
        Draft202012Validator.check_schema(schema)
    except SchemaError as exc:
        raise PlanValidationError(
            f"invalid JSON Schema{(' for ' + context) if context else ''}: {exc.message}",
            details={"schema": schema},
            cause=exc,
        ) from exc


def validate_instance(
    schema: dict[str, Any],
    instance: Any,
    *,
    error_cls: type[FrameworkError] = ContractValidationError,
    context: str = "",
) -> None:
    validator = Draft202012Validator(schema)
    try:
        validator.validate(instance)
    except ValidationError as exc:
        path = ".".join(str(p) for p in exc.absolute_path) or "<root>"
        prefix = f"{context}: " if context else ""
        raise error_cls(
            f"{prefix}schema validation failed at {path}: {exc.message}",
            details={
                "path": path,
                "validator": exc.validator,
                "instance": instance,
                "schema": schema,
            },
            cause=exc,
        ) from exc


def schema_properties(schema: dict[str, Any]) -> dict[str, Any]:
    props = schema.get("properties")
    return props if isinstance(props, dict) else {}
