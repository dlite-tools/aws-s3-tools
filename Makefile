PACKAGE=s3_tools
UNIT_TESTS=tests/unit

static-tests:
	###### Running linter analysis ######
	uv run ruff check $(PACKAGE) $(UNIT_TESTS)

	###### Running static type analysis ######
	uv run mypy $(PACKAGE) $(UNIT_TESTS)

unit-tests:
	###### Running unit tests ######
	uv run pytest -v $(UNIT_TESTS)

coverage:
	###### Running coverage analysis ######
	uv run pytest --cov=$(PACKAGE) --cov-report=term-missing --cov-report=xml --junitxml=junit.xml

build-docs:
	###### Build documentation ######
	uv run make -C docs html
