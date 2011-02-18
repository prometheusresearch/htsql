# This makefile provides various build, installation and testing tasks.

.PHONY: default build install develop doc dist windist pypi clean \
	test train train-routine train-sqlite train-pgsql purge-test lint \
	create create-sqlite create-pgsql drop drop-sqlite drop-pgsql \
	demo-htraf demo-ssi


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
	@echo "  purge-test: to purge stale test output data"
	@echo "  lint: detect errors in the source code"
	@echo "  create: to install the regression databases"
	@echo "	 create-sqlite: to install the test database for SQLite"
	@echo "  create-pgsql: to install the test database for PostgreSQL"
	@echo "  drop: to drop users and databases deployed by regression tests"
	@echo "  drop-sqlite: to delete the test database for SQLite"
	@echo "  drop-pgsql: to delete the test database for PostgreSQL"
	@echo
	@echo "  *** Shell and Server ***"
	@echo "  shell-sqlite: to start an HTSQL shell on the SQLite test database"
	@echo "  shell-pgsql: to start an HTSQL shell on the PostgreSQL test database"
	@echo "  serve-sqlite: to start an HTTP server on the SQLite test database"
	@echo "  serve-pgsql: to start an HTTP server on the PostgreSQL test database"
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
	python setup.py build

# Install the HTSQL packages.
install:
	python setup.py install

# Install the HTSQL packages in the development mode.
develop:
	python setup.py develop

# Build the HTSQL documentation.
doc:
	sphinx-build -b html doc build/doc

# Build a source and an EGG distributions.
# FIXME: include HTML documentation; `dist_dir` is broken for `--bdist-deb`.
# Note that `bdist_deb` requires `stdeb` package.
dist:
	rm -rf build
	python setup.py sdist
	python setup.py bdist_egg
	#python setup.py --command-packages=stdeb.command bdist_deb 

# Register and upload the package to PyPI.
# FIXME: include HTML documentation.
pypi:
	rm -rf build
	python setup.py register sdist bdist_egg upload

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
	htsql-ctl regress -i test/regress.yaml -q

# Run HTSQL regression tests in the train mode.
train:
	htsql-ctl regress -i test/regress.yaml --train

# Run regression tests for htsql-ctl tool in the train mode.
train-routine:
	htsql-ctl regress -i test/regress.yaml --train routine

# Run SQLite-specific regression tests in the train mode.
train-sqlite:
	htsql-ctl regress -i test/regress.yaml --train sqlite

# Run PostgreSQL-specific regression tests in the train mode.
train-pgsql:
	htsql-ctl regress -i test/regress.yaml --train pgsql

# Purge stale output records from HTSQL regression tests.
purge-test:
	htsql-ctl regress -i test/regress.yaml -q --train --purge

# Detect errors in the source code (requires PyFlakes)
lint:
	pyflakes src/htsql src/htsql_pgsql src/htsql_sqlite

# Install the regression databases.
create:
	htsql-ctl regress -i test/regress.yaml -q create-sqlite create-pgsql

# Install the regression database for SQLite.
create-sqlite:
	htsql-ctl regress -i test/regress.yaml -q create-sqlite

# Install the regression database for PostgreSQL.
create-pgsql:
	htsql-ctl regress -i test/regress.yaml -q create-pgsql

# Drop any users and databases deployed by the regression tests.
drop:
	htsql-ctl regress -i test/regress.yaml -q drop-sqlite drop-pgsql

# Drop the regression database for SQLite
drop-sqlite:
	htsql-ctl regress -i test/regress.yaml -q drop-sqlite

# Drop the regression database for PostgreSQL.
drop-pgsql:
	htsql-ctl regress -i test/regress.yaml -q drop-pgsql


#
# Shell and server tasks.
#

# Start an HTSQL shell on the SQLite regression database.
shell-sqlite:
	htsql-ctl shell ${SQLITE_REGRESS_DB}

# Start an HTSQL shell on the PostgreSQL regression database.
shell-pgsql:
	htsql-ctl shell ${PGSQL_REGRESS_DB}

# Start an HTTP/HTSQL server on the SQLite regression database.
serve-sqlite:
	htsql-ctl serve ${SQLITE_REGRESS_DB} ${HTSQL_HOST} ${HTSQL_PORT}

# Start an HTTP/HTSQL server on the PostgreSQL regression database.
serve-pgsql:
	htsql-ctl serve ${PGSQL_REGRESS_DB} ${HTSQL_HOST} ${HTSQL_PORT}


#
# Demos and examples.
#

# Start the HTRAF demo.
demo-htraf:
	cd demo/htraf; ${MAKE}

# Start the SSI demo.
demo-ssi:
	cd demo/ssi; ${MAKE}


