#!/usr/bin/env sh
# Every quality gate, in order of speed. Fails on the first error.
set -eu

echo "==> ruff lint";            ruff check .
echo "==> ruff format (check)";  ruff format --check .
echo "==> mypy (strict)";        mypy
echo "==> bandit (security)";    bandit -q -c pyproject.toml -r app worker
echo "==> pytest";               pytest --cov --cov-report=term-missing "$@"
