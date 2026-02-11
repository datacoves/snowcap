.PHONY: install install-dev test integration style check clean build docs coverage
EDITION ?= standard or enterprise

install:
	pip install -e .

install-dev:
	pip install -e ".[dev]"

test:
	python -m pytest

integration:
	python -m pytest --snowflake -m "$(EDITION)"

setup-test-resources:
	@echo "Setting up static resources for integration tests..."
	./tests/fixtures/static_resources/apply.sh

style:
	python -m black .
	codespell .


typecheck:
	mypy --exclude="snowcap/resources/.*" --exclude="snowcap/sql.py" --follow-imports=skip snowcap/

check: style typecheck test

clean:
	rm -rf build dist *.egg-info
	find . -name "__pycache__" -type d -exec rm -rf {} +

build:
	mkdir -p dist
	zip -vrX dist/snowcap-$(shell python setup.py -V).zip snowcap/

docs: 
	python tools/generate_resource_docs.py

coverage: clean
	python tools/check_resource_coverage.py

package: clean
	python -m build

submit: package
	python -m twine upload dist/*