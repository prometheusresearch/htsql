update: regress-pgsql.sql
	epio upload -a htsql-demo

install: update
	epio run_command -a htsql-demo python install.py

regress-pgsql.sql:
	cp ../../test/sql/regress-pgsql.sql regress-pgsql.sql    
