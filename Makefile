# This makefile provides various build, installation and testing tasks.

.PHONY: default build install deps develop doc dist windist pypi clean \
	test train test-routine train-routine test-sqlite train-sqlite \
	test-pgsql train-pgsql test-mysql train-mysql test-oracle train-oracle \
	test-mssql train-mssql purge-test lint \
	create-sqlite create-pgsql create-mysql create-oracle create-mssql \
	drop-sqlite drop-pgsql drop-mysql drop-oracle drop-mssql \
	build-all check-all start-pgsql84 start-pgsql90 start-mysql51 \
	start-oracle10g start-mssql2005 start-mssql2008 stop-pgsql84 stop-pgsql90 \
	stop-mysql51 stop-oracle10g stop-mssql2005 stop-mssql2008 \
	demo-htraf demo-ssi shell-sqlite shell-pgsql shell-mysql shell-oracle \
	shell-mssql serve-sqlite serve-pgsql serve-mysql serve-oracle serve-mssql \
	client-sqlite client-pgsql client-mysql client-oracle client-mssql


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
	@echo "  update: to update the HTSQL source code"
	@echo "  build: to build the HTSQL packages"
	@echo "  install: to install the HTSQL packages"
	@echo "  deps: to install database drivers"
	@echo "  develop: to install the HTSQL packages in the development mode"
	@echo "  doc: to build the HTSQL documentation"
	@echo "  dist: to build a source and an EGG distribution"
	@echo "  pypi: to register and upload the package to PyPI"
	@echo "  clean: to remove the build directory and object files"
	@echo
	@echo "  *** Regression Testing ***"
	@echo "  test: to run HTSQL regression tests"
	@echo "  train: to run all HTSQL tests in the train mode"
	@echo "  test-<suite>: to run a specific test suite"
	@echo "  train-<suite>: to run a specific test suite in the train mode"
	@echo "    where <suite> is one of:"
	@echo "      routine, sqlite, pgsql, mysql, oracle, mssql"
	@echo "  purge-test: to purge stale test output data"
	@echo "	 create-<db>: to install the test database for a specific database"
	@echo "  drop-<db>: to delete the test database for a specific database"
	@echo "    where <db> is one of:"
	@echo "        sqlite, pgsql, mysql, oracle, mssql"
	@echo "  lint: detect errors in the source code"
	@echo
	@echo "  *** Integration Testing ***"
	@echo "  build-all: to build all test benches"
	@echo "  check-all: run HTSQL regression tests on all supported platforms"
	@echo "  start-<bench>: to start the specified test bench"
	@echo "  stop-<bench>: to stop the specified test bench"
	@echo "    where <bench> is one of:"
	@echo "      py25, py26, pgsql84, pgsql90, mysql51, oracle10g,"
	@echo "      mssql2005, mssql2008"
	@echo
	@echo "  *** Shell and Server ***"
	@echo "  shell-<db>: to start the HTSQL shell on the specified test database"
	@echo "  serve-<db>: to start an HTTP server on the specified test database"
	@echo "  client-<db> to start the native client for the specified test database"
	@echo "    where <db> is one of:"
	@echo "      sqlite, pgsql, mysql, oracle, mssql"
	@echo
	@echo "  *** Demos and Examples ***"
	@echo "  demo-htraf: to run the HTRAF demo"
	@echo "  demo-ssi: to run the SSI demo"
	@echo


#
# Building and installation tasks.
#

# Update the HTSQL source code.
update:
	hg pull
	hg update

# Build the HTSQL packages.
build:
	${PYTHON} setup.py build

# Install the HTSQL packages.
install:
	${PYTHON} setup.py install

# Install database drivers.
deps:
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

# Build the HTSQL documentation.
doc:
	${SPHINX_BUILD} -b html doc build/doc

# Build a source and an EGG distributions.
# FIXME: include HTML documentation; `dist_dir` is broken for `--bdist-deb`.
# Note that `bdist_deb` requires `stdeb` package.
dist:
	rm -rf build/dist build/lib.* build/bdist.*
	${PYTHON} setup.py sdist
	${PYTHON} setup.py bdist_egg
	#python setup.py --command-packages=stdeb.command bdist_deb 

# Register and upload the package to PyPI.
# FIXME: include HTML documentation.
pypi:
	rm -rf build/dist build/lib.* build/bdist.*
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

# Run regression tests for htsql-ctl tool.
test-routine:
	${HTSQL_CTL} regress -i test/regress.yaml -q routine

# Run regression tests for htsql-ctl tool in the train mode.
train-routine:
	${HTSQL_CTL} regress -i test/regress.yaml --train routine

# Run SQLite-specific regression tests.
test-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml -q sqlite

# Run SQLite-specific regression tests in the train mode.
train-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml --train sqlite

# Run PostgreSQL-specific regression tests.
test-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml -q pgsql

# Run PostgreSQL-specific regression tests in the train mode.
train-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml --train pgsql

# Run MySQL-specific regression tests.
test-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml -q mysql

# Run MySQL-specific regression tests in the train mode.
train-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml --train mysql

# Run Oracle-specific regression tests.
test-oracle:
	${HTSQL_CTL} regress -i test/regress.yaml -q oracle

# Run Oracle-specific regression tests in the train mode.
train-oracle:
	${HTSQL_CTL} regress -i test/regress.yaml --train oracle

# Run MS SQL Server-specific regression tests.
test-mssql:
	${HTSQL_CTL} regress -i test/regress.yaml -q mssql

# Run MS SQL Server-specific regression tests in the train mode.
train-mssql:
	${HTSQL_CTL} regress -i test/regress.yaml --train mssql

# Purge stale output records from HTSQL regression tests.
purge-test:
	${HTSQL_CTL} regress -i test/regress.yaml -q --train --purge

# Detect errors in the source code (requires PyFlakes)
lint:
	${PYFLAKES} src/htsql src/htsql_pgsql src/htsql_sqlite

# Install the regression database for SQLite.
create-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-sqlite

# Install the regression database for PostgreSQL.
create-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-pgsql

# Install the regression database for MySQL.
create-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-mysql

# Install the regression database for Oracle.
create-oracle:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-oracle

# Install the regression database for MS SQL Server.
create-mssql:
	${HTSQL_CTL} regress -i test/regress.yaml -q create-mssql

# Drop the regression database for SQLite
drop-sqlite:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-sqlite

# Drop the regression database for PostgreSQL.
drop-pgsql:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-pgsql

# Drop the regression database for MySQL.
drop-mysql:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-mysql

# Drop the regression database for Oracle.
drop-oracle:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-oracle

# Drop the regression database for MS SQL Server.
drop-mssql:
	${HTSQL_CTL} regress -i test/regress.yaml -q drop-mssql


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

# Start an HTSQL shell on the Oracle regression database.
shell-oracle:
	${HTSQL_CTL} shell ${ORACLE_URI}

# Start an HTSQL shell on the MS SQL Server regression database.
shell-mssql:
	${HTSQL_CTL} shell ${MSSQL_URI}

# Start an HTTP/HTSQL server on the SQLite regression database.
serve-sqlite:
	${HTSQL_CTL} serve ${SQLITE_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the PostgreSQL regression database.
serve-pgsql:
	${HTSQL_CTL} serve ${PGSQL_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the MySQL regression database.
serve-mysql:
	${HTSQL_CTL} serve ${MYSQL_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the Oracle regression database.
serve-oracle:
	${HTSQL_CTL} serve ${ORACLE_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the MS SQL Server regression database.
serve-mssql:
	${HTSQL_CTL} serve ${MSSQL_URI} ${HTSQL_HOST} ${HTSQL_PORT}

# Start a native client on the SQLite regression database.
client-sqlite:
	${SQLITE_CLIENT}

# Start a native client on the PostgreSQL regression database.
client-pgsql:
	${PGSQL_CLIENT}

# Start a native client on the MySQL regression database.
client-mysql:
	${MYSQL_CLIENT}

# Start a native client on the Oracle regression database.
client-oracle:
	${ORACLE_CLIENT}

# Start a native client on the MS SQL Server regression database.
client-mssql:
	${MSSQL_CLIENT}


#
# Integration testing.
#

# Build all the test benches.
build-all:
	./test/buildbot/bb.sh build

# Run regression tests on all combinations of test benches.
check-all:
	./test/buildbot/bb.sh check

# Start the test bench for Python 2.5
start-py25:
	./test/buildbot/bb.sh start py25

# Start the test bench for Python 2.6
start-py26:
	./test/buildbot/bb.sh start py26

# Start the test bench for PostgreSQL 8.4
start-pgsql84:
	./test/buildbot/bb.sh start pgsql84

# Start the test bench for PostgreSQL 9.0
start-pgsql90:
	./test/buildbot/bb.sh start pgsql90

# Start the test bench for MySQL 5.1
start-mysql51:
	./test/buildbot/bb.sh start mysql51

# Start the test bench for Oracle 10g
start-oracle10g:
	./test/buildbot/bb.sh start oracle10g

# Start the test bench for MS SQL Server 2005
start-mssql2005:
	./test/buildbot/bb.sh start mssql2005

# Start the test bench for MS SQL Server 2008
start-mssql2008:
	./test/buildbot/bb.sh start mssql2008

# Stop the test bench for Python 2.5
stop-py25:
	./test/buildbot/bb.sh stop py25

# Stop the test bench for Python 2.6
stop-py26:
	./test/buildbot/bb.sh stop py26

# Stop the test bench for PostgreSQL 8.4
stop-pgsql84:
	./test/buildbot/bb.sh stop pgsql84

# Stop the test bench for PostgreSQL 9.0
stop-pgsql90:
	./test/buildbot/bb.sh stop pgsql90

# Stop the test bench for MySQL 5.1
stop-mysql51:
	./test/buildbot/bb.sh stop mysql51

# Stop the test bench for Oracle 10g
stop-oracle10g:
	./test/buildbot/bb.sh stop oracle10g

# Stop the test bench for MS SQL Server 2005
stop-mssql2005:
	./test/buildbot/bb.sh stop mssql2005

# Stop the test bench for MS SQL Server 2008
stop-mssql2008:
	./test/buildbot/bb.sh stop mssql2008


#
# Demos and examples.
#

# Start the HTRAF demo.
demo-htraf:
	cd demo/htraf; ${MAKE}

# Start the SSI demo.
demo-ssi:
	cd demo/ssi; ${MAKE}


