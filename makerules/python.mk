PIP_INSTALL_PACKAGE=[test,digital_land,dev]

all:: lint test coverage

lint:: black-check flake8

black-check:
	black --check .

black:
	black .

flake8:
	flake8 .

test:: test-unit test-integration

test-unit:
	[ -d digital_land_airflow/tests/unit ] && python -m pytest digital_land_airflow/tests/unit --junitxml=.junitxml/unit.xml -c setup.cfg $(PYTEST_EXTRA_ARGS)

test-integration:
	[ -d digital_land_airflow/tests/integration ] && python -m pytest digital_land_airflow/tests/integration --junitxml=.junitxml/integration.xml -c setup.cfg $(PYTEST_EXTRA_ARGS)

coverage:
	coverage run --source $(PACKAGE) -m py.test && coverage report

coveralls:
	py.test --cov $(PACKAGE) tests/ --cov-report=term --cov-report=html

bump::
	git tag $(shell python version.py)

dist:: all
	python setup.py sdist bdist_wheel

upload::	dist
	twine upload dist/*
