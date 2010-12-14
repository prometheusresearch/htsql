.PHONY: serve build clean

BUILD = \
	static/htraf/js/jquery-1.4.3.min.js \
	static/htraf/js/jquery.jqplot.min.js \
	static/htraf/js/excanvas.min.js \
	static/htraf/js/jqplot.pieRenderer.min.js \
	static/htraf/css/jquery.jqplot.min.css

include ../../Makefile.common

serve: build
	${PYTHON} serve.py ${PGSQL_REGRESS_DB} ${HTSQL_HOST} ${HTSQL_PORT}

build: static/external

clean:
	rm -rf static/external

static/external:
	rm -rf build
	mkdir build
	wget -P build -q http://code.jquery.com/jquery-1.4.3.min.js
	wget -P build -q http://bitbucket.org/cleonello/jqplot/downloads/jquery.jqplot.0.9.7r629.tar.gz
	tar -xz -C build -f build/jquery.jqplot.0.9.7r629.tar.gz
	mkdir static/external
	mkdir static/external/jquery
	cp build/jquery-1.4.3.min.js static/external/jquery
	mkdir static/external/jqplot
	cp -a build/dist/* static/external/jqplot
	rm -rf build

