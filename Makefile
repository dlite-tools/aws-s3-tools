PACKAGE=s3_tools
UNIT_TESTS=tests/unit

static-tests:
	###### Running linter analysis ######
	poetry run ruff check $(PACKAGE) $(UNIT_TESTS)

	###### Running static type analysis ######
	poetry run mypy $(PACKAGE) $(UNIT_TESTS)

unit-tests:
	###### Running unit tests ######
	poetry run pytest -v $(UNIT_TESTS)

coverage:
	###### Running coverage analysis ######
	poetry run pytest --cov=$(PACKAGE) --cov-report=term-missing --cov-report=xml --junitxml=junit.xml

build-docs:
	###### Build documentation ######
	poetry run make -C docs html
