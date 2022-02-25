PACKAGE=s3_tools
UNIT_TESTS=tests/unit

static-tests:
		###### Running style analysis ######
		poetry run flake8 $(PACKAGE)
		###### Running static type analysis ######
		poetry run mypy $(PACKAGE)
		###### Running documentation analysis ######
		poetry run pydocstyle -v $(PACKAGE)

unit-tests:
		###### Running unit tests ######
		poetry run pytest -v $(UNIT_TESTS)

coverage:
		###### Running coverage analysis ######
		poetry run pytest --cov-report term-missing --cov-report xml --cov $(PACKAGE)

build-docs:
		###### Build documentation ######
		poetry run make -C docs html
