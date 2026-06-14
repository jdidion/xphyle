# AGENTS.md

xphyle: a Python library for opening, reading, and writing files (and file-like
entities like URLs and streams) agnostic of compression format.

## Build & test

- Build system: `pyproject.toml` with the setuptools backend; versioning is git-tag
  driven via `setuptools-scm` (no hardcoded version, no `versioneer.py`).
- Environment and builds are managed with **uv**.
  - `uv sync --all-extras` — install into a local venv.
  - `uv run pytest -m "not perf"` — run the test suite (or `make test`).
  - `uv build` — build sdist + wheel.
- Supported Python: 3.10+.

## Conventions

- Format/lint with `ruff` (`uv run ruff check xphyle`).
- Keep `CHANGES.md` updated with an entry per user-visible change.
- Trunk is `main`. Open PRs against `main`.

## Releasing

Tag `vX.Y.Z` and push the tag; CI builds and publishes to PyPI. Do not edit a
version string by hand.
