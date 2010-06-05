HTSQL API
=========

:mod:`htsql`
------------
.. automodule:: htsql

:mod:`htsql.export`
-------------------
.. automodule:: htsql.export
.. autoclass:: HTSQL_CORE

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
.. autoclass:: subclassof
.. autoclass:: filelike
.. autofunction:: aresubclasses

Text formatting
~~~~~~~~~~~~~~~
.. autofunction:: trim_doc

Topological sorting
~~~~~~~~~~~~~~~~~~~
.. autofunction:: toposort


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

:mod:`htsql.application`
------------------------
.. automodule:: htsql.application
.. autoclass:: Application
   :members: __call__, __enter__, __exit__

:mod:`htsql.context`
--------------------
.. automodule:: htsql.context
.. autoclass:: ThreadContext
   :members: switch, app

:mod:`htsql.adapter`
--------------------
.. automodule:: htsql.adapter
.. autoclass:: Adapter
   :members: __new__, realize
.. autoclass:: Utility
.. autoclass:: AdapterRegistry
   :members: specialize
.. autofunction:: adapts
.. autofunction:: dominates
.. autofunction:: dominated_by
.. autofunction:: weights
.. autofunction:: find_adapters

:mod:`htsql.addon`
------------------
.. automodule:: htsql.addon
.. autoclass:: Addon

:mod:`htsql.mark`
-----------------
.. automodule:: htsql.mark
.. autoclass:: Mark
   :members: union, pointer

:mod:`htsql.error`
------------------
.. automodule:: htsql.error
.. autoexception:: HTTPError
   :members: __call__

Generic HTTP errors
~~~~~~~~~~~~~~~~~~~
.. autoexception:: BadRequestError
.. autoexception:: ForbiddenError
.. autoexception:: NotFoundError
.. autoexception:: ConflictError
.. autoexception:: InternalServerError
.. autoexception:: NotImplementedError

Concrete HTSQL Errors
~~~~~~~~~~~~~~~~~~~~~
.. autoexception:: InvalidSyntaxError

:mod:`htsql.wsgi`
-----------------
.. automodule:: htsql.wsgi
.. autoclass:: WSGI
   :members: __call__

:mod:`htsql.connect`
--------------------
.. automodule:: htsql.connect
.. autoexception:: DBError
.. autoclass:: ErrorGuard
.. autoclass:: ConnectionProxy
   :members: cursor, commit, rollback, close
.. autoclass:: CursorProxy
   :members: description, rowcount, execute, executemany,
             fetchone, fetchmany, fetchall, __iter__, close
.. autoclass:: Connect
   :members: __call__, open_connection, translate_error

:mod:`htsql.split_sql`
----------------------
.. automodule:: htsql.split_sql
.. autoclass:: SQLToken
.. autoclass:: SplitSQL
   :members: __call__

:mod:`htsql.tr`
---------------
.. automodule:: htsql.tr

:mod:`htsql.tr.token`
---------------------
.. automodule:: htsql.tr.token
.. autoclass:: Token
   :members: unquote, quote
.. autoclass:: SpaceToken
.. autoclass:: NameToken
.. autoclass:: StringToken
.. autoclass:: NumberToken
.. autoclass:: SymbolToken
.. autoclass:: EndToken

:mod:`htsql.tr.scanner`
-----------------------
.. automodule:: htsql.tr.scanner
.. autoclass:: TokenStream
   :members: peek, pop
.. autoclass:: Scanner
   :members: scan

:mod:`htsql.tr.syntax`
----------------------
.. automodule:: htsql.tr.syntax
.. autoclass:: Syntax
   :members: __str__
.. autoclass:: QuerySyntax
.. autoclass:: SegmentSyntax
.. autoclass:: SelectorSyntax
.. autoclass:: SieveSyntax
.. autoclass:: OperatorSyntax
.. autoclass:: FunctionOperatorSyntax
.. autoclass:: FunctionCallSyntax
.. autoclass:: GroupSyntax
.. autoclass:: SpecifierSyntax
.. autoclass:: IdentifierSyntax
.. autoclass:: WildcardSyntax
.. autoclass:: LiteralSyntax
.. autoclass:: StringSyntax
.. autoclass:: NumberSyntax

:mod:`htsql.tr.parser`
----------------------
.. automodule:: htsql.tr.parser
.. autoclass:: Parser
   :members: parse, process
.. autoclass:: QueryParser
.. autoclass:: SegmentParser
.. autoclass:: ElementParser
.. autoclass:: TestParser
.. autoclass:: AndTestParser
.. autoclass:: ImpliesTestParser
.. autoclass:: UnaryTestParser
.. autoclass:: ComparisonParser
.. autoclass:: ExpressionParser
.. autoclass:: TermParser
.. autoclass:: FactorParser
.. autoclass:: PowerParser
.. autoclass:: SieveParser
.. autoclass:: SpecifierParser
.. autoclass:: AtomParser
.. autoclass:: GroupParser
.. autoclass:: SelectorParser
.. autoclass:: IdentifierParser

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
.. autoexception:: ScriptError

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

:mod:`htsql.ctl.request`
-------------------------
.. automodule:: htsql.ctl.request
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

:mod:`htsql.ctl.regress`
------------------------
.. automodule:: htsql.ctl.regress
.. autoclass:: TermStringIO
.. autoclass:: Field(attribute, validator, default=MANDATORY_FIELD, hint=None)
   :members: get_hint, get_signature
.. autoclass:: AnyField
.. autoclass:: TestData
   :members: init_attributes
.. autoclass:: TestCase
   :members: get_hint, get_help, get_suites, matches,
             ask, ask_halt, ask_record, ask_save,
             out, out_exception, out_sep, out_header,
             halted, failed, updated, passed,
             verify, train
.. autoclass:: RunAndCompareTestCase
   :members: out_lines, out_diff, render, execute, differs
.. autoclass:: SkipTestCase
.. autoclass:: AppTestCase
.. autoclass:: IncludeTestCase
.. autoclass:: SuiteTestCase
.. autoclass:: QueryTestCase
.. autoclass:: CtlTestCase
.. autoclass:: Fork
   :members: start, end
.. autoclass:: StartCtlTestCase
.. autoclass:: EndCtlTestCase
.. autoclass:: PythonCodeTestCase
.. autoclass:: SQLTestCase
   :members: load
.. autoclass:: SQLIncludeTestCase
.. autoclass:: WriteToFileTestCase
.. autoclass:: ReadFromFileTestCase
.. autoclass:: RemoveFilesTestCase
.. autoclass:: MakeDirTestCase
.. autoclass:: RemoveDirTestCase
.. autoclass:: TestState
   :members: push, pull
.. autoclass:: RegressYAMLLoader
   :members: load
.. autoclass:: RegressYAMLDumper
   :members: dump
.. autoclass:: RegressRoutine
   :members: get_help, get_feature

:mod:`htsql_sqlite`
-------------------
.. automodule:: htsql_sqlite

:mod:`htsql_sqlite.export`
--------------------------
.. automodule:: htsql_sqlite.export
.. autoclass:: ENGINE_SQLITE

:mod:`htsql_sqlite.connect`
---------------------------
.. automodule:: htsql_sqlite.connect
.. autoclass:: SQLiteError
.. autoclass:: SQLiteConnect

:mod:`htsql_sqlite.split_sql`
-----------------------------
.. automodule:: htsql_sqlite.split_sql
.. autoclass:: SplitSQLite

:mod:`htsql_pgsql`
------------------
.. automodule:: htsql_pgsql

:mod:`htsql_pgsql.export`
-------------------------
.. automodule:: htsql_pgsql.export
.. autoclass:: ENGINE_PGSQL

:mod:`htsql_pgsql.connect`
---------------------------
.. automodule:: htsql_pgsql.connect
.. autoclass:: PGSQLError
.. autoclass:: PGSQLConnect

:mod:`htsql_pgsql.split_sql`
----------------------------
.. automodule:: htsql_pgsql.split_sql
.. autoclass:: SplitPGSQL


