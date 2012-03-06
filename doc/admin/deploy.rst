*******************
  Deploying HTSQL
*******************

.. contents:: Table of Contents
   :depth: 1
   :local:

.. highlight:: console

The following instructions assume you've installed HTSQL and wish
to deploy it to an existing web server.


Deployment
==========

The built-in HTSQL web server was designed for personal and testing use
and may appear inadequate for production deployment.  In particular,
it does not not provide any means for authentication and lacks SSL support.

.. index:: Apache

Integration with Apache
-----------------------

It is possible to integrate HTSQL with `Apache HTTP Server`_ using
mod_wsgi_.  Here we assume that both Apache and mod_wsgi are already
installed.

First, create a WSGI script file:

.. sourcecode:: python

   from htsql import HTSQL

   # The address of the database in the form:
   #   engine://user:pass@host:port/database
   DB = '...'

   application = HTSQL(DB)

Save this file as ``htsql.wsgi`` and place it to a directory
accessible by Apache (but do not put it below the root of the web
site so that it cannot be downloaded).

Next, add the following line to the Apache configuration file:

.. sourcecode:: apache

   WSGIScriptAlias /htsql /path/to/htsql.wsgi

This line should be added to the ``VirtualHost`` section of the respective
web site.  It associates any URL starting with ``/htsql`` with the HTSQL
server.

For more information of installing and configuring Apache and mod_wsgi,
see documentation for the respective projects, in particular,
`Quick Configuration Guide for mod_wsgi`_.

.. _Apache HTTP Server: http://httpd.apache.org/
.. _mod_wsgi: http://code.google.com/p/modwsgi/
.. _Quick Configuration Guide for mod_wsgi:
    http://code.google.com/p/modwsgi/wiki/QuickConfigurationGuide


.. index:: Security

Security
========

Giving HTSQL access is practically equivalent to giving an access to
a read-only SQL console and should be planned accordingly.

HTSQL, as a gateway between HTTP server and a database server, does
not provide any security mechanisms.  Any protection should be set
up on either the HTTP or the database layers.  On the HTTP layer,
you may put the HTSQL server behind an HTTP server or a proxy
to provide SSL, authentication and caching.  On the database layer,
you may restrict access to selected database entities using roles and
permissions.

With a proper setup, data leaks should be impossible.  Another
potential vector of attack is overloading the database server,
against which we recommend setting up an HTTP caching layer and
restricting resource usage for the HTSQL database user.


.. vim: set spell spelllang=en textwidth=72:
