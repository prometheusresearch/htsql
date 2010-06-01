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

.. autoclass:: filelike

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
   :members: get_hint, get_signature


:mod:`htsql.ctl.routine`
------------------------

.. automodule:: htsql.ctl.routine

.. autoclass:: Argument(attribute, validator, default=MANDATORY_ARGUMENT, is_list=False, hint=None)
   :members: get_hint, get_signature

.. autoclass:: Routine
   :members: get_hint, get_help, get_signature, run


:mod:`htsql.ctl.default`
------------------------

.. automodule:: htsql.ctl.default

.. autoclass:: DefaultRoutine


:mod:`htsql.ctl.version`
------------------------

.. automodule:: htsql.ctl.version

.. autoclass:: VersionRoutine


:mod:`htsql.ctl.help`
---------------------

.. automodule:: htsql.ctl.help

.. autoclass:: HelpRoutine


:mod:`htsql.ctl.server`
-----------------------

.. automodule:: htsql.ctl.server

.. autoclass:: ServerRoutine


:mod:`htsql.ctl.get_post`
-------------------------

.. automodule:: htsql.ctl.get_post

.. autoclass:: Request
   :members: prepare, execute

.. autoclass:: Response
   :members: set, complete, dump

.. autoclass:: GetPostBaseRoutine

.. autoclass:: GetRoutine

.. autoclass:: PostRoutine


:mod:`htsql.ctl.shell`
----------------------

.. automodule:: htsql.ctl.shell

.. autoclass:: Cmd
   :members: get_hint, get_help, get_signature, execute

.. autoclass:: HelpCmd

.. autoclass:: ExitCmd

.. autoclass:: UserCmd

.. autoclass:: HeadersCmd

.. autoclass:: PagerCmd

.. autoclass:: GetPostBaseCmd

.. autoclass:: GetCmd

.. autoclass:: PostCmd

.. autoclass:: ShellState

.. autoclass:: ShellRoutine
   :members: get_help, get_intro, get_usage


