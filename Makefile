module = xphyle
repo = jdidion/$(module)
tests = tests

all: install test

install:
	uv sync --all-extras

test:
	uv run pytest -m "not perf" -vv --cov --cov-report term-missing $(pytestopts) $(tests)

perftest:
	uv run pytest -m "perf" $(tests)

lint:
	uv run ruff check $(module)

build:
	uv build

clean:
	rm -Rf __pycache__
	rm -Rf **/__pycache__/*
	rm -Rf dist
	rm -Rf build
	rm -Rf *.egg-info
	rm -Rf .pytest_cache
	rm -Rf .coverage

# Versioning is git-tag driven via setuptools-scm. To release:
#   1. git tag vX.Y.0 && git push origin vX.Y.0
#   2. CI builds and publishes to PyPI (or run `make publish` locally with UV_PUBLISH_TOKEN set).
publish: clean build
	uv publish

docs:
	$(MAKE) -C docs api
	$(MAKE) -C docs html
