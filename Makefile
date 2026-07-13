POE_TASKS := \
	up-services \
	init-minio \
	up \
	down \
	reset \
	seed \
	pipeline \
	demo \
	incident-demo \
	recover-demo \
	test \
	integration \
	coverage \
	lint \
	format \
	format-check \
	typecheck \
	bandit \
	audit \
	security \
	dbt-compile \
	dbt-test \
	docs \
	dagster-validate \
	check

.PHONY: bootstrap $(POE_TASKS)

# Bootstrap stays usable before Poe has been installed into the project environment.
bootstrap:
	uv sync --locked --all-groups

$(POE_TASKS):
	uv run poe $@
