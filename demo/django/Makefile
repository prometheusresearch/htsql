test: clean serve

clean:
	-rm -f htsql_demo.sqdb
	rm -rf htsql_demo/*.pyc htsql_demo/uni/*.pyc

htsql_demo.sqdb:
	python htsql_demo/manage.py syncdb --noinput

shell: htsql_demo.sqdb
	htsql-ctl shell sqlite:htsql_demo.sqdb 

serve: htsql_demo.sqdb
	python serve.py
