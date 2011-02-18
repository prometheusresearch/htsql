# This makefile provides various build, installation and testing tasks.

.PHONY: default build install develop doc dist windist pypi clean \
	test train train-routine train-sqlite train-pgsql train-mysql \
	purge-test lint create create-sqlite create-pgsql create-mysql \
	drop drop-sqlite drop-pgsql drop-mysql demo-htraf demo-ssi


# Load configuration variables from `Makefile.common`.  Do not edit
# `Makefile` or `Makefile.common` directly, you could override any
# parameters in `Makefile.env`.  There is a sample file `Makefile.env.sample`;
# copy it to `Makefile.env` and edit it to match your configuration.
include Makefile.common


#
# The default task.
#

# Display the list of available targets.
default:
	@echo "Run 'make <target>', where <target> is one of:"
	@echo
	@echo "  *** Building and Installation ***"
	@echo "  build: to build the HTSQL packages"
	@echo "  install: to install the HTSQL packages"
	@echo "  develop: to install the HTSQL packages in the development mode"
	@echo "  doc: to build the HTSQL documentation"
	@echo "  dist: to build a source and an EGG distribution"
	@echo "  pypi: to register and upload the package to PyPI"
	@echo "  clean: to remove the build directory and object files"
	@echo
	@echo "  *** Regression Testing ***"
	@echo "  test: to run HTSQL regression tests"
	@echo "  train: to run all HTSQL tests in the train mode"
	@echo "  train-routine: to run tests for htsql-ctl tool in the train mode"
	@echo "  train-sqlite: to run SQLite-specific tests in the train mode"
	@echo "  train-pgsql: to run PostgreSQL-specific tests in the train mode"
	@echo "  train-mysql: to run MySQL-specific tests in the train mode"
	@echo "  purge-test: to purge stale test output data"
	@echo "  lint: detect errors in the source code"
	@echo "  create: to install the regression databases"
	@echo "	 create-sqlite: to install the test database for SQLite"
	@echo "  create-pgsql: to install the test database for PostgreSQL"
	@echo "  create-mysql: to install the test database for MySQL"
	@echo "  drop: to drop users and databases deployed by regression tests"
	@echo "  drop-sqlite: to delete the test database for SQLite"
	@echo "  drop-pgsql: to delete the test database for PostgreSQL"
	@echo "  drop-mysql: to delete the test database for MySQL"
	@echo
	@echo "  *** Shell and Server ***"
	@echo "  shell-sqlite: to start an HTSQL shell on the SQLite test database"
	@echo "  shell-pgsql: to start an HTSQL shell on the PostgreSQL test database"
	@echo "  shell-mysql: to start an HTSQL shell on the MySQL test database"
	@echo "  serve-sqlite: to start an HTTP server on the SQLite test database"
	@echo "  serve-pgsql: to start an HTTP server on the PostgreSQL test database"
	@echo "  serve-mysql: to start an HTTP server on the MySQL test database"
	@echo
	@echo "  *** Demos and Examples ***"
	@echo "  demo-htraf: to run the HTRAF demo"
	@echo "  demo-ssi: to run the SSI demo"
	@echo


#
# Building and installation tasks.
#

# Build the HTSQL packages.
build:
	${PYTHON} setup.py build

# Install the HTSQL packages.
install:
	${PYTHON} setup.py install

# Install the HTSQL packages in the development mode.
develop:
	${PYTHON} setup.py develop

# Build the HTSQL documentation.
doc:
	${SPHINX_BUILD} -b html doc build/doc

# Build a source and an EGG distributions.
# FIXME: include HTML documentation; `dist_dir` is broken for `--bdist-deb`.
# Note that `bdist_deb` requires `stdeb` package.
dist:
	rm -rf build
	${PYTHON} setup.py sdist
	${PYTHON} setup.py bdist_egg
	#python setup.py --command-packages=stdeb.command bdist_deb 

# Register and upload the package to PyPI.
# FIXME: include HTML documentation.
pypi:
	rm -rf build
	${PYTHON} setup.py register sdist bdist_egg upload

# Delete the build directory and object files.
clean:
	rm -rf build
	find . -name '*.pyc' -exec rm '{}' ';'
	find . -name '*.pyo' -exec rm '{}' ';'


#
# Regression testing tasks.
#

# Run HTSQL regression tests.
test:
	${HTSQL_CTL} regress -i test/regress.yaml -q

# Run HTSQL regression tests in the train mode.
train:
	${HTSQL_CTL} regress -i test/regress.yaml --train

# Run regression tests for htsql-ctl tool in the train mode.
train-routine:
	${HTSQL_CTL} regress -i test/regress.yaml --train routine

# Run SQLite-specific regression tests in the train mode.
train-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml --train sqlite

# Run PostgreSQL-specific regression tests in the train mode.
train-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml --train pgsql

# Run MySQL-specific regression tests in the train mode.
train-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml --train mysql

# Purge stale output records from HTSQL regression tests.
purge-test:
	${HTSQL_CTL} regress -i test/regress.yaml -q --train --purge

# Detect errors in the source code (requires PyFlakes)
lint:
	${PYFLAKES} src/htsql src/htsql_pgsql src/htsql_sqlite

# Install the regression databases.
create:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-sqlite create-pgsql create-mysql

# Install the regression database for SQLite.
create-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-sqlite

# Install the regression database for PostgreSQL.
create-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-pgsql

# Install the regression database for MySQL.
create-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-mysql

# Drop any users and databases deployed by the regression tests.
drop:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-sqlite drop-pgsql drop-mysql

# Drop the regression database for SQLite
drop-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-sqlite

# Drop the regression database for PostgreSQL.
drop-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-pgsql

# Drop the regression database for PostgreSQL.
drop-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-mysql


#
# Shell and server tasks.
#

# Start an HTSQL shell on the SQLite regression database.
shell-sqlite:
	${HTSQL_CTL} shell ${SQLITE_URI}

# Start an HTSQL shell on the PostgreSQL regression database.
shell-pgsql:
	${HTSQL_CTL} shell ${PGSQL_URI}

# Start an HTSQL shell on the MySQL regression database.
shell-mysql:
	${HTSQL_CTL} shell ${MYSQL_URI}

# Start an HTTP/HTSQL server on the SQLite regression database.
serve-sqlite:
	${HTSQL_CTL} serve ${SQLITE_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the PostgreSQL regression database.
serve-pgsql:
	${HTSQL_CTL} serve ${PGSQL_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the MySQL regression database.
serve-mysql:
	${HTSQL_CTL} serve ${MYSQL_URI} ${HTSQL_HOST} ${HTSQL_PORT}


#
# Demos and examples.
#

# Start the HTRAF demo.
demo-htraf:
	cd demo/htraf; ${MAKE}

# Start the SSI demo.
demo-ssi:
	cd demo/ssi; ${MAKE}


