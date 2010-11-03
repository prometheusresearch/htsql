.PHONY: setup serve

# The connection URI for regression databases.
SQLITE_REGRESS_DB?=sqlite:///build/regress/regress-sqlite/htsql_regress.sqlite
PGSQL_ADDRESS?=${PGSQL_HOST}$(if ${PGSQL_PORT},:${PGSQL_PORT})
PGSQL_REGRESS_DB?=pgsql://htsql_regress:secret@${PGSQL_ADDRESS}/htsql_regress

# The HTTP server address.
HTTP_HOST?=localhost
HTTP_PORT?=8080

setup-pgsql:
	cd ../../ && htsql-ctl regress -i test/regress.yaml cleanup-pgsql setup-pgsql 

serve-pgsql: setup-pgsql
	python serve.py "${PGSQL_REGRESS_DB}" ${HTTP_HOST} ${HTTP_PORT}
