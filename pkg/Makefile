VERSION = '2.2.0b2'

clean:
	rm -rf python-htsql* HTSQL-*

deb:
	wget http://pypi.python.org/packages/source/H/HTSQL/HTSQL-$(VERSION).tar.gz
	mv HTSQL-$(VERSION).tar.gz python-htsql_2.2.0b2.orig.tar.gz
	tar xvfz python-htsql_$(VERSION).orig.tar.gz
	cp -a debian HTSQL-$(VERSION)
	cd HTSQL-$(VERSION) && dpkg-buildpackage -rfakeroot
	lintian python-htsql_$(VERSION)-1.dsc

