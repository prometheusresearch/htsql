***********************
  HTSQL Configuration
***********************

.. highlight:: console

The following instructions assume you've installed HTSQL and wish to
configure extensions or "tweaks".

Plugin Mechanism
================

HTSQL has an extensive addon system that can be used to override almost
every aspect of server operation or query construction with an adapter.  
Extensions can live in third party modules or be included in the HTSQL
distribution as part of our supported "tweaks".  To list supported
extensions, you could type::

  $ htsql-ctl extension

Extensions are registered via "entry points".  If you update your
software distribution you will need to re-install, or for source
distributions, re-run ``make deploy`` in order to see new extensions.
To find out more about a plugin, such as ``tweak.autolimit``, write::

  $ htsql-ctl extension tweak.autolimit

Third parties can also register extensions with their own Python EGGs by
adding a ``htsql.addons`` section to ``ENTRY_POINTS`` entry in their
``setup.py``.  Each extension would implement HTSQL's addon protocol.


Using Extensions
----------------

An extension can be enabled using ``-E`` parameter on the ``htsql-ctl``
command line.  For example, to enable the ``tweak.meta`` plugin on the
PostgreSQL regression test suite, you'd write::

  $ htsql-ctl shell -E tweak.meta pgsql:htsql_regress

Then, you could use the ``/meta()`` command registered by this plugin::

  Interactive HTSQL Shell
  Type 'help' for more information, 'exit' to quit the shell.
  htsql_regress$ /meta(/table)

Some plugins have parameters which can be added to the command line.
For example, the ``autolimit`` extension truncates at ``limit`` number
of rows.  The default is 10k, but this ``limit`` can be set::

  $ htsql-ctl shell -E tweak.autolimit:limit=10 pgsql:htsql_regress

If more than one parameter is possible, use "," to separate them::

  $ htsql-ctl shell -E tweak.hello:repeat=3,address=home pgsql:htsql_regress 

Config Files
------------

Plugins and configuration parameters can also be provided by a flat YAML
(or JSON) file, and then included using ``-C`` on the command line.
Here is an example configuration file against PostgreSQL database
with most plugins enabled.

.. sourcecode:: yaml

      # demo-config.yaml
      htsql:
        db:
          engine: pgsql
          database: htsql_regress
          username: htsql_regress
          password: secret
          host: localhost
          port: 5432
      tweak.autolimit:
        limit: 1000
      tweak.timeout:
        timeout: 600
      tweak.cors:
      tweak.meta:
      tweak.shell:
        server-root: http://localhost:8080
      tweak.shell.default:

You can then start the built-in demonstration web server::

  $ htsql-ctl serve -C demo-config.yaml

For ``htsql-ctl serve`` the webserver host and port are *not* provided
via plugin mechanism and must be provided if something other than
``localhost:8080`` is desired.  If both ``-E`` and ``-C`` are used,
explicit command line options override values provided in the
configuration file.  This permits a configuration file to be used as a
default perhaps using a different database URI.

HTSQL "tweaks"
==============

The HTSQL distribution ships with several built-in extensions
we call ``htsql_tweaks``.   We list a few of them here.

``tweak.meta``
--------------

This extension module creates an in-memory SQLite database that can be
queried (using HTSQL) to return information about the system catalog.
The schema has several tables:

``table``
   all tables accessable via the attached credentials

``field``
   columns and links of a given table

``column``
   all columns accessable for the given table

``link``
   all links from one table to another

So, to enumerate links for a table, say ``course`` you could type:

.. htsql:: /meta(/link?table_name='course')

You could also run ``/meta()`` on the meta-data schema, for example:

.. htsql:: /meta(/meta(/table))


``tweak.shell``
---------------

This extension module adds a command ``/shell()`` which takes any query
and populates visual editor with syntax highlighting (using the
excellent CodeMirror_ library).  As you modify the query, the URL
changes so it can be bookmarked.

The ``tweak.shell.default`` plugin will make ``/shell()`` the 
default command, replacing the regular HTML output.

.. _CodeMirror: http://codemirror.net/

``tweak.autolimit``
-------------------

To help deployments ensure against accidental denial of
service, this plugin automatically truncates output from
a query to a given number of rows (10k default).  The 
``limit`` parameter can be customized to change the 
truncation limit.

``tweak.timeout``
-----------------

To help deployments ensure against accidental denial of
service, this plugin automatically limits all queries to
a given number of ``timeout`` seconds (the default is 60s).  
This plugin is currently only supported by PostgreSQL.

``tweak.cors``
--------------

This plugin adds CORS headers in order to enable cross
site scripting for public data servers.  This permits
modern browsers to bypass JSONP and other hacks used
to work around XSS protection.

``tweak.sqlalchemy``
--------------------

This plugin provides SQLAlchemy integration in two ways.
First, if the dburi is omitted, it attempts to use the
database connection from SQLAlchemy.  Secondly, it uses
the SQLAlchemy model instead of introspecting.

``tweak.view``
--------------

This plugin attempts to guess at various links 
between views and tables (where foreign keys are
not defined).  This is only supported in PostgreSQL.

``tweak.system``
----------------

This plugin adds the system catalog tables and links for the
database's native system catalog.  This is supported only for
PostgreSQL.

.. vim: set spell spelllang=en textwidth=72:
