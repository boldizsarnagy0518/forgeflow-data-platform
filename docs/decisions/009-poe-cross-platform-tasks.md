# ADR-009: Poe tasks are the cross-platform command contract

- Status: Accepted and implemented locally
- Date: 2026-07-10

## Context

The mission asks for Make-style developer commands, but the inspected Windows environment has no GNU
Make. Duplicating command bodies across shell, PowerShell, Make, and CI would invite drift.

## Decision

Define executable task bodies in `pyproject.toml` with Poe the Poet and invoke them as
`uv run poe <task>`. Keep a thin `Makefile` that maps requested names to the same Poe tasks for Unix
reviewers. CI also calls the canonical tasks where practical.

## Consequences

- Windows, macOS, Linux, CI, and Make users share one command definition.
- `uv` provides the locked environment; Poe adds one lightweight task dependency.
- Bootstrap has a small bootstrapping nuance because Poe itself is in the managed environment.
- A task name is documentation only until exercised on the target platform.

## Verification

Documented task names match `pyproject.toml`; exact commands already exercised and their current
results are recorded in `STATUS.md`.
