#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

# Run `make install` or `python setup.py install` to install HTSQL, or
# see `INSTALL` for the list of prerequisites and detailed installation
# instructions.

.PHONY: default build install install-deps develop clean

# Paths to executable files; overridable through environment variables.
PYTHON?=python
PIP?=pip

# Display the list of available targets.
default:
	@echo "Run 'make <target>', where <target> is one of:"
	@echo
	@echo "  install:       to install HTSQL"
	@echo "  install-deps:  to install database drivers"
	@echo "  develop:       to install HTSQL in the development mode"
	@echo "  clean:         to remove the build directory and compiled files"
	@echo

# Install the HTSQL packages.
install:
	${PYTHON} setup.py install

# Install database drivers.
install-deps:
	if ! ${PYTHON} -c 'import psycopg2' >/dev/null 2>&1; then \
		${PIP} install psycopg2; fi
	if ! ${PYTHON} -c 'import MySQLdb' >/dev/null 2>&1; then \
		${PIP} install mysql-python; fi
	if ! ${PYTHON} -c 'import pymssql' >/dev/null 2>&1; then \
		${PIP} install pymssql \
		-f http://pypi.python.org/pypi/pymssql/ --no-index; fi
	if ! ${PYTHON} -c 'import cx_Oracle' >/dev/null 2>&1; then \
		${PIP} install cx-oracle; fi

# Install the HTSQL packages in the development mode.
develop:
	${PYTHON} setup.py develop

# Delete the build directory and compiled files.
clean:
	rm -rf build
	rm -rf src/HTSQL.egg-info
	find . -name '*.pyc' -exec rm '{}' ';'
	find . -name '*.pyo' -exec rm '{}' ';'

