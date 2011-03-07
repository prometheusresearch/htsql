from django.conf.urls.defaults import *

# Uncomment the next two lines to enable the admin:
from django.contrib import admin
admin.autodiscover()

from twod.wsgi import make_wsgi_view
from htsql import HTSQL
htsql_app = HTSQL("sqlite:htsql_demo.sqdb")

urlpatterns = patterns('',
    # Example:
    # (r'^htsql_demo/', include('htsql_demo.foo.urls')),

    (r'^htsql(/.*)$', make_wsgi_view(htsql_app)),

    # Uncomment the admin/doc line below to enable admin documentation:
    (r'^admin/doc/', include('django.contrib.admindocs.urls')),

    # Uncomment the next line to enable the admin:
    (r'^admin/', include(admin.site.urls)),
)
