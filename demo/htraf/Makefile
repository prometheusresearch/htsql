.PHONY: serve build clean

include ../../Makefile.common

serve: build
	${PYTHON} serve.py ${PGSQL_REGRESS_DB} ${HTSQL_HOST} ${HTSQL_PORT}

build: static/htraf

clean:
	rm -rf static/htraf

static/htraf:
	rm -rf build
	hg clone https://bitbucket.org/prometheus/htraf build
	sed s/\\.\\./htraf/ build/htraf/examples20/index.html >static/index.html
	cp -R build/htraf static
	rm -rf static/htraf/examples10 static/htraf/index.html 
	cp static/htraf/index20.html static/htraf/index.html
	rm -rf build
