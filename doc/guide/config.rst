***********************
  HTSQL Configuration
***********************

.. highlight:: console

The following instructions assume you've installed HTSQL and wish to
configure extensions or "tweaks".

.. _addon: http://htsql.org/doc/api/htsql.html#htsql-addon
.. _adapter: http://htsql.org/doc/api/htsql.html#htsql-adapter
.. _tweaks: https://bitbucket.org/prometheus/htsql/src/tip/src/htsql_tweak/

Plugin Mechanism
================

HTSQL has an extensive addon_ system that can be used to override almost
every aspect of server operation or query construction with an adapter_.  
Extensions can live in third party modules or be included in the HTSQL
distribution as part of our supported "tweaks_".  To list supported
extensions, you could type::

  $ htsql-ctl extension

Extensions are registered via "entry points".  If you update your
software distribution you will need to re-install, or for source
distributions, re-run ``make deploy`` in order to see new extensions.
To find out more about a plugin, such as ``tweak.autolimit``, write::

  $ htsql-ctl extension tweak.autolimit

Third parties can also register extensions with their own Python EGGs
by adding a ``htsql.addons`` section to ``ENTRY_POINTS`` entry in 
their ``setup.py``.  Each extension would then implement HTSQL's 
addon_ protocol. 


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

  $ htsql-ctl shell -E tweak.autolimit;limit=10 pgsql:htsql_regress

  ^ this is wrong; how do you set parameters again?


Config Files
------------

Plugins and configuration parameters can also be provided by 
a flat YAML (or JSON) file.  

.. vim: set spell spelllang=en textwidth=72:
