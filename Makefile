.PHONY: install install-dev test integration style lint check clean build docs coverage provision-test-account drop-test-account
EDITION ?= standard or enterprise
EMAIL ?=

install:
	uv sync --no-dev

install-dev:
	# Legacy `pip install -e .` metadata shadows the uv-managed install; drop it.
	rm -rf *.egg-info
	uv sync

test:
	uv run pytest

integration:
	uv run pytest --snowflake -m "$(EDITION)"

setup-test-resources:
	@echo "Setting up static resources for integration tests..."
	./tests/fixtures/static_resources/apply.sh

reset-test-account:
	uv run python tools/manage_test_account.py reset

provision-test-account:
	@if [ -z "$(EMAIL)" ]; then \
		echo "EMAIL is required, e.g. make provision-test-account EMAIL=you@example.com"; \
		exit 1; \
	fi
	uv run python tools/manage_test_account.py provision --email $(EMAIL)

drop-test-account:
	uv run python tools/manage_test_account.py drop

style:
	uv run black .
	uv run codespell .

# Same checks as `style`, but read-only. This is what CI runs.
lint:
	uv run black --check .
	uv run codespell .
	uv run ruff check snowcap/


typecheck:
	uv run mypy --exclude="snowcap/resources/.*" --exclude="snowcap/sql.py" --follow-imports=skip snowcap/

check: style typecheck test

clean:
	rm -rf build dist *.egg-info
	find . -name "__pycache__" -type d -exec rm -rf {} +

build:
	mkdir -p dist
	zip -vrX dist/snowcap-$(shell grep '# version' version.md | cut -d ' ' -f3).zip snowcap/

docs:
	uv run python tools/generate_resource_docs.py

coverage: clean
	uv run python tools/check_resource_coverage.py

package: clean
	uv build

submit: package
	uv run twine upload dist/*