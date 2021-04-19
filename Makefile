PACKAGE=s3_tools
UNIT_TESTS=tests/unit

all: static-tests coverage

.PHONY: all

style:
		###### Running style analysis ######
		poetry run flake8 $(PACKAGE)

typecheck:
		###### Running static type analysis ######
		poetry run mypy $(PACKAGE)

doccheck:
		###### Running documentation analysis ######
		poetry run pydocstyle -v $(PACKAGE)

static-tests: style typecheck doccheck

unit-tests:
		###### Running unit tests ######
		poetry run pytest -v $(UNIT_TESTS)

coverage:
		###### Running coverage analysis ######
		poetry run pytest --cov-report term-missing --cov-report xml --cov $(PACKAGE)

coverage-html:
		###### Running coverage analysis with html export ######
		poetry run pytest --cov-report html --cov $(PACKAGE)
		open htmlcov/index.html

build-docs:
		###### Build documentation ######
		poetry run make -C docs html
