# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.2.0] - 2026-10-21

### Added

- Checkpoint snapshots and resume: `Checkpoint`, `JsonCheckpointStore`,
  `plan_hash()`, `spec_hash()`, and `TaskRuntime.resume()`.
- Resume safety checks against task id, protocol version, plan hash, spec hash,
  and non-idempotent side effects.
- Checkpoint events: `CHECKPOINT_SAVED`, `CHECKPOINT_LOADED`, and
  `RECOVERY_FAILED`.
- Event payload redaction for common credential keys and obvious
  `sk-...` / `Bearer ...` token patterns.
- Event diagnostics: `task_id`, `step_id`, `duration_ms`,
  `event_schema_version`, and the `STEP_SKIPPED` event.
- Demo checkpoint options: `--checkpoint-dir`, `--resume`, and `--resume-run`.
- GitHub project configuration: MIT `LICENSE`, CI for Python 3.11-3.13,
  packaging metadata, `.gitattributes`, and expanded ignore rules.

### Changed

- Event sink failures are collected and surfaced as `EventLogError` instead of
  being ignored or interrupting a run at an arbitrary point.
- Documentation updated in English and Chinese for checkpointing, resume,
  redaction, and current limitations.
- Project version bumped to 0.2.0.

## [0.1.0] - 2026-10-21

### Added

- Initial prototype: unified `TaskIR`, script and graph authoring APIs,
  static plan validation, retries, timeouts, cancellation, event stores,
  structured LLM output, and run equivalence comparison.
