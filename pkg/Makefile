VERSION = 2.2.0b2
ARCHIVE = HTSQL-$(VERSION).tar.gz

$(ARCHIVE):
	wget http://pypi.python.org/packages/source/H/HTSQL/$(ARCHIVE)

debian/htsql.1:
	help2man --help-option=help --version-string=$(VERSION) \
		--no-info htsql-ctl > debian/htsql.1

deb: $(ARCHIVE) clean 
	if test -r ~/.pydistutils.cfg; then exit 1; fi
	cp -f $(ARCHIVE) htsql_$(VERSION).orig.tar.gz
	tar xvfz htsql_$(VERSION).orig.tar.gz
	cp -a debian HTSQL-$(VERSION)
	cd HTSQL-$(VERSION) && dpkg-buildpackage -rfakeroot
	lintian htsql_$(VERSION)-1.dsc

clean:
	rm -rf htsql* HTSQL-$(VERSION) python-htsql*

.PHONY: clean
