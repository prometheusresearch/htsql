from os import environ
environ['DJANGO_SETTINGS_MODULE'] = "htsql_demo.settings"
from twod.wsgi import DjangoApplication
from django.core.servers.basehttp import AdminMediaHandler
from wsgiref.simple_server import make_server
httpd = make_server('', 8080, AdminMediaHandler(DjangoApplication(),''))
print "HTSQL Wrapper is at http://127.0.0.1:8080/htsql/"
httpd.serve_forever()
