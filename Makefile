SHELL := /bin/bash
ENV_NAME := venv
REPORTS_FOLDER := reports
ENV_PY_VERSION = 3.6.*

.PHONY: install unittest acceptance_test test env develop clean cleanall package documentation coverage

clean:
	rm -rf build
	rm -rf htmlcov
	rm -f .coverage
	rm -rf ${REPORTS_FOLDER}
	rm -rf dist
	rm -rf .eggs
	rm -rf *.egg-info
	rm -rf .pytest_cache
	rm -rf .cache
	rm -rf ${ENV_NAME}
	find . -name "*.pyc" -type f -delete
	make -C docs clean

env:
	virtualenv ${ENV_NAME}

install:
	pip install .

unittest:
	pytest

acceptance_test:	dd/tests/acceptance/tests_functional_nb.py
	pytest -k "functional"

integration_db_test:	dd/db/tests/db_integration/tests_db_integration.py
	pytest -k "integration"

test:	unittest acceptance_test

develop:
	pip install -e .[test] && \
	pip install -e . --extra-index-url https://pypi.anaconda.org/octo/simple

package:
	python setup.py sdist bdist_wheel

documentation:
	make -C docs docs