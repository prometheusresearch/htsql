HTSQL API
=========


:mod:`htsql`
------------

.. automodule:: htsql


:mod:`htsql.util`
-----------------

.. automodule:: htsql.util

Connection parameters
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: DB
   :members: parse, __str__

Type checking helpers
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: maybe

.. autoclass:: oneof

.. autoclass:: listof

.. autoclass:: tupleof

.. autoclass:: dictof

Text formatting
~~~~~~~~~~~~~~~

.. autofunction:: trim_doc


:mod:`htsql.validator`
----------------------

.. automodule:: htsql.validator

.. autoclass:: Val

.. autoclass:: AnyVal

.. autoclass:: StrVal

.. autoclass:: WordVal

.. autoclass:: BoolVal

.. autoclass:: IntVal

.. autoclass:: UIntVal

.. autoclass:: PIntVal

.. autoclass:: DBVal


:mod:`htsql.ctl`
----------------

.. automodule:: htsql.ctl

.. autoclass:: HTSQL_CTL


:mod:`htsql.ctl.script`
-----------------------

.. automodule:: htsql.ctl.script

.. autoclass:: Script
   :members: main, out, err, get_hint, get_help, get_copyright


:mod:`htsql.ctl.error`
----------------------

.. automodule:: htsql.ctl.error

.. autoclass:: ScriptError


:mod:`htsql.ctl.option`
-----------------------

.. automodule:: htsql.ctl.option

.. autoclass:: Option


:mod:`htsql.ctl.routine`
------------------------

.. automodule:: htsql.ctl.routine

.. autoclass:: Argument(attribute, validator, default=ARGUMENT_REQUIRED, is_list=False)

.. autoclass:: Routine
   :members: run


:mod:`htsql.ctl.default`
------------------------

.. automodule:: htsql.ctl.default

.. autoclass:: DefaultRoutine


:mod:`htsql.ctl.version`
------------------------

.. automodule:: htsql.ctl.version

.. autoclass:: VersionRoutine


