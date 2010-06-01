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


:mod:`htsql.mark`
-----------------

.. automodule:: htsql.mark

.. autoclass:: Mark
   :members: union, pointer


:mod:`htsql.error`
------------------

.. automodule:: htsql.error

.. autoclass:: HTTPError
   :members: __call__

Generic HTTP errors
~~~~~~~~~~~~~~~~~~~

.. autoclass:: BadRequestError

.. autoclass:: ForbiddenError

.. autoclass:: NotFoundError

.. autoclass:: ConflictError

.. autoclass:: InternalServerError

.. autoclass:: NotImplementedError

Concrete HTSQL Errors
~~~~~~~~~~~~~~~~~~~~~

.. autoclass:: InvalidSyntaxError


:mod:`htsql.application`
------------------------

.. automodule:: htsql.application

.. autoclass:: Application
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

.. autoclass:: FormatSyntax

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

.. autoclass:: SQLIncludeTestCase

.. autoclass:: WriteToFileTestCase

.. autoclass:: ReadFromFileTestCase

.. autoclass:: RemoveFilesTestCase

.. autoclass:: MakeDirTestCase

.. autoclass:: RemoveDirTestCase

.. autoclass:: TestState
   :members: clone, merge

.. autoclass:: RegressYAMLLoader
   :members: load

.. autoclass:: RegressYAMLDumper
   :members: dump

.. autoclass:: RegressRoutine
   :members: get_help, get_feature


