# This makefile provides various build, installation and testing tasks.

.PHONY: default build install develop doc \
	test test-ctl test-sqlite test-pgsql \
	train train-ctl train-sqlite train-pgsql purge-test


#
# Help.
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
	@echo
	@echo "  *** Regression Testing ***"
	@echo "  test: to run HTSQL regression tests"
	@echo "  train: to run all HTSQL tests in the train mode"
	@echo "  train-ctl: to run tests for htsql-ctl routines in the train mode"
	@echo "  train-sqlite: to run SQLite-specific tests in the train mode"
	@echo "  train-pgsql: to run PostgreSQL-specific tests in the train mode"
	@echo "  purge-test: to purge state test output data"


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


#
# Regression testing tasks.
#

# Run HTSQL regression tests.
test:
	htsql-ctl regress -i test/regress.yaml -q

# Run HTSQL regression tests in the train mode.
train:
	htsql-ctl regress -i test/regress.yaml --train

# Run regression tests for htsql-ctl routines in the train mode.
train-ctl:
	htsql-ctl regress -i test/regress.yaml --train ctl

# Run SQLite-specific regression tests in the train mode.
train-sqlite:
	htsql-ctl regress -i test/regress.yaml --train sqlite

# Run PostgreSQL-specific regression tests in the train mode.
train-pgsql:
	htsql-ctl regress -i test/regress.yaml --train pgsql

# Purge stale output records from HTSQL regression tests.
purge-test:
	htsql-ctl regress -i test/regress.yaml -q --train --purge


