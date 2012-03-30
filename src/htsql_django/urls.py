#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#

from django.conf.urls import patterns, url

urlpatterns = patterns('',
    url(r'', 'htsql_django.views.gateway'),
)

