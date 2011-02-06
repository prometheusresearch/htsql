.PHONY: update install 
IMAGE = htsql-demo

upload: regress-pgsql.sql 
	epio upload -a $(IMAGE)

install: upload
	epio run_command -a $(IMAGE) python install.py

regress-pgsql.sql:
	cp ../../test/sql/regress-pgsql.sql regress-pgsql.sql    

