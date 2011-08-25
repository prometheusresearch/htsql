*****************
  HTSQL Grammar
*****************

A valid input of an HTSQL processor is called an HTSQL query.

In a regular mode of operation, an HTSQL processor is started as a web
service and accepts queries as HTTP ``GET`` requests.  However, an HTSQL
query can also be executed using a command-line utility ``htsql-ctl`` or
via internal Python API.


Encoding
========

An HTSQL query is a string of characters in UTF-8 encoding.  Octets
composing the string could be written literally or percent-encoded.  A
percent-encoded octet is serialized as three characters: ``%`` followed
by two hexdecimal digits encoding the octet value.

.. htsql:: /{'HTSQL', %27HTSQL%27, %27%48%54%53%51%4C%27}
   :query: /%7B'HTSQL',%20%27HTSQL%27,%20%27%48%54%53%51%4C%27%7D

Percent-encoding is useful for transmitting an HTSQL query via channels
that forbid certain characters in literal form.  The list of characters
that should be encoded depends on the channel type, but the percent
(``%``) character itself must always be percent-encoded.

.. htsql:: /{'%25'}
   :query: /%7B'%25'%7D

A ``NUL`` character cannot appear in an HTSQL query, neither in literal
nor in percent-encoded form.

The HTSQL processor decodes percent-encoded octets before parsing the
query.  As a consequence, a percent-encoded punctuation or operator
character still plays its syntax role.


Lexical Structure
=================

An HTSQL query is parsed into a sequence of tokens.  The following
tokens are recognized.

Name
----

A sequence of alphanumeric characters that does not start with a digit.

.. htsql:: /school
   :cut: 3

Number
------

A numeric literal: integer, decimal and exponential notations are
recognized.

.. htsql:: /{60, 2.125, 271828e-5}

String
------

A string literal enclosed in single quotes; any single quote character
should be doubled.

.. htsql:: /{'HTSQL', 'O''Reilly'}

Symbol
------

A valid symbol in the HTSQL grammar; that includes operators and
punctuation characters.  Some symbols are represented by more than one
character (e.g. ``<=``, ``!~``).

Individual tokens may be separated by whitespace characters.

See :class:`htsql.tr.scan.Scanner` for detailed description of HTSQL
tokens.


Syntax Structure
================

A sequence of HTSQL tokens must obey the HTSQL grammar.

An HTSQL query starts with ``/`` followed by a valid HTSQL expression
and is concluded with an optional query decorator.

The following table lists HTSQL operations in the order of precedence,
lowest to highest.

+----------------------+---------------------------+---------------------------+----------------------+
| Operation            | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `x :fn`              | infix function call       | ``'HTSQL':length``        | ``5``                |
+----------------------+                           +---------------------------+----------------------+
| `x :fn y`            |                           | ``1/3 :round 2``          | ``0.33``             |
+----------------------+                           +---------------------------+----------------------+
| `x :fn (y,z,...)`    |                           | ``'HTSQL':slice(1,-1)``   | ``'TSQ'``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x +`, `x -`         | sorting direction         | ``program{degree+}``      |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `T ? p`              | sieving                   | ``program?degree='ms'``   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `T ^ x`              | projection                | ``program^degree``        |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `T {x,y,...}`        | selection                 | ``school{code,name}``     |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `p | q`              | logical *OR*              | ``true()|false()``        | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `p & q`              | logical *AND*             | ``true()&false()``        | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `\! p`               | logical *NOT*             | ``!true()``               | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x = y`, `x != y`,   | comparison                | ``2+2=4``                 | ``true``             |
+----------------------+                           +---------------------------+----------------------+
| `x == y`, `x !== y`  |                           | ``'HTSQL'==null()``       | ``false``            |
+----------------------+                           +---------------------------+----------------------+
| `x ~ y`, `x !~ y`    |                           | ``'HTSQL'~'SQL'``         | ``true``             |
+----------------------+                           +---------------------------+----------------------+
| `x < y`, `x <= y`,   |                           | ``12<7``                  | ``false``            |
+----------------------+                           +---------------------------+----------------------+
| `x > y`, `x >= y`    |                           | ``12>=7``                 | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `x + y`, `x - y`     | addition, subtraction     | ``'HT'+'SQL'``            | ``'HTSQL'``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `x * y`, `x / y`     | multiplication, division  | ``12*7``                  | ``84``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `- x`                | negation                  | ``-42``                   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x -> T`             | linking                   | |link-in|                 |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `T := x`             | assignment                | |assign-in|               |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `S . T`              | composition               | ``school.program``        |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `{x,y,...}`          | list                      | ``{'bs','ms'}``           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `(...)`              | grouping                  | ``(7+4)*2``               | ``22``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `*`                  | wildcard selection        | ``school.*``              |                      |
+----------------------+                           +---------------------------+----------------------+
| `* number`           |                           | ``school.*1``             |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `^`                  | projection complement     | ``count(^)``              |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `$ name`             | reference                 | ``$code``                 |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `fn (...)`           | function call             | ``round(1/3,2)``          | ``0.33``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `name`               |                           | ``school``                |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `number`             |                           | ``60``, ``2.125``,        |                      |
|                      |                           | ``271828e-5``             |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `string`             |                           | ``'HTSQL'``               |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |link-in| replace:: ``'south' -> school{campus}``
.. |assign-in| replace:: ``num_prog := count(program)``

An optional query decorator starts with ``/`` followed ``:`` and the
decorator name.

.. htsql:: /school/:csv
   :cut: 3

See :class:`htsql.tr.parse.QueryParser` for a formal description of the
HTSQL grammar.

Next we describe individual syntax elements.

Atomic Expressions
------------------

An atomic expression is a basic syntax unit.  HTSQL recognizes the
following atoms.

Identifier
~~~~~~~~~~

An identifier is a sequence of characters which contains Latin letters,
underscores (``_``), decimal digits and those Unicode characters that
are classified as alphanumeric.  An identifier must not start with a
digit.

In HTSQL, identifiers are *case-insensitive*.

Identifiers are used to refer to database entities such as tables and
attributes, to define calculated attributes, and to call functions.

.. htsql:: /school{name, count(department)}
   :cut: 3

In this example, four identifiers ``school``, ``name``, ``count`` and
``department`` represent respectively a table, a table attribute, a
built-in function and a table link.

Literal
~~~~~~~

HTSQL supports two types of literal values: *quoted* and *unquoted*.

An unquoted (or numeric) literal is a number written in integer, decimal
or exponential notation.

.. htsql:: /{60, 2.125, 271828e-5}

The range of allowed numeric values depends on the database backend.
The type of a numeric literal is determined from notation: literals
written in integer, decimal and exponential notation are assigned to
`integer`, `decimal` and `float` data type respectively.

A quoted literal is a (possibly empty) sequence of arbitrary characters
enclosed in single quotes.  Any single quote in the value must be
doubled.

.. htsql:: /{'HTSQL', 'O''Reilly'}

The data type of a quoted literal is inferred from the context in which
the literal is used; the default data type is `string`.

Wildcard
~~~~~~~~

A wildcard selection (``*``) selects all output columns of the table.

.. htsql:: /department{school.*, *}
   :cut: 3

.. **

When followed by an integer literal ``N``, a wildcard selects ``N``-th
output column of the table.  ``N`` starts from ``1`` and should not
exceed the number of output columns.

.. htsql:: /school{name, count(department)}?*2>=4
   :cut: 3

Complement
~~~~~~~~~~

A projection complement (``^``) represents a complement link from a
projection to the projected flow.

Do not confuse a projection complement with a binary projection
operator, which is also represented with the ``^`` character.

.. htsql:: /program^degree{*, count(^)}
   :cut: 3

.. **

In this example, the first and the second occurrences of ``^`` indicate
a projection operator and a projection complement respectively.

Grouping
~~~~~~~~

Any expression enclosed in parentheses (``(...)``) is treated
syntactically as a single atom.  Use grouping to override the default
operator precedence.

.. htsql:: /(7+4)*2

Do not confuse a grouping operation with a function call, which also
uses parentheses.

List
~~~~

A comma-separated list of expressions enclosed in curly brackets
(``{...}``) is called a list expression.  Many functions and operators
accept lists as a way to specify multiple values.

.. htsql:: /school?code={'eng','ns'}

Reference
~~~~~~~~~

A reference is an identifier preceded by a dollar sign (``$``).  A
reference is used to access a value defined in a different naming scope.

.. htsql::
   :cut: 3

   /course?credits>$avg_credits
    :where $avg_credits := avg(course.credits)

In this example, a reference ``$avg_credits`` is defined in the root
scope, but accessed in the scope of ``course``.

For a more detailed description of references, see the section on
naming scopes.

Function Calls
--------------

HTSQL has a large library of built-in functions and can be extended with
user-defined functions.

A function call is represented as a function name followed by ``(``, a
comma-separated list of arguments, and ``)``.

.. htsql:: /round(1/3, 2)

A function may accept no arguments, but the parentheses are still
required.

.. htsql:: /today()

For functions with at least one argument, HTSQL supports an alternative
infix call notation.  In this notation, the expression starts with the
first argument followed by ``:`` and a function name, and then the rest
of the arguments.  The trailing arguments must be enclosed in
parentheses if their number is greater than one.

.. htsql:: /{today() :year, 1/3 :round 2, 'HTSQL' :slice(1, -1)}

This example could be equivalently expressed as

.. htsql:: /{year(today()), round(1/3, 2), slice('HTSQL', 1, -1)}

Infix function calls are composable and have the lowest precedence among
the operators.

.. htsql:: /{'h'+'t'+'t'+'p' :replace('tp', 'sql') :upper}

For a list and description of built-in functions, see :doc:`functions`.

Operators
---------

An HTSQL operator is denoted by a special character or a sequence of
characters (e.g. ``+``, ``<=``).  HTSQL has infix, prefix and postfix
operators, and some operators admit all three forms.

The current version of HTSQL does not support user-defined operators;
future versions may add this ability.

In HTSQL, the order in which operators are applied is determined by
*operator precedence*.  For example, multiplication and division
operators have a higher precedence than addition and subtraction.

Some HTSQL operators are composable (e.g. arithmetic operators) and some
are not (e.g. equality operators).  We call the former *associative* and
the latter *non-associative*.

Below we describe the syntax of HTSQL operators.  For a more
comprehensive description, see :doc:`functions`.

Logical Operators
~~~~~~~~~~~~~~~~~

HTSQL supports the following logical operators:

logical *OR*
    `p | q`
logical *AND*
    `p & q`
logical *NOT*
    `\! p`

In this list, the operators are sorted by the order of precedence, from
lowest to highest.  All logical operators are left-associative.

.. htsql:: /{true()|false(), true()&false(), !false()}

Comparison Operators
~~~~~~~~~~~~~~~~~~~~

HTSQL supports the following comparison operators:

*equality* operators
    `x = y`, `x != y`, `x == y`, `x !== y`
*containing* operators
    `x ~ y`, `x !~ y`
*ordering* operators
    `x < y`, `x <= y`, `x > y`, `x >= y`

.. htsql:: /{2+2=4, 'HTSQL'~'SQL', 12>7&7>=2}

All comparison operators have the same precedence and are not
associative.

Future versions of HTSQL may make ordering operators left-associative
to express *between* operation (e.g.  `a <= x <= b`).

Arithmetic Operators
~~~~~~~~~~~~~~~~~~~~

HTSQL supports the usual set of arithmetic operators:

*addition*
    `x + y`
*subtraction*
    `x - y`
*multiplication*
    `x * y`
*division*
    `x / y`
*negation*
    `- x`

.. htsql:: /{'HT'+'SQL', today()-1, -6*4/5}
   :hide:

Arithmetic operators have standard precedence and associativity.

Flow Operators
~~~~~~~~~~~~~~
HTSQL supports specialized operators to work with flow expressions:

*sieving*
    `T ? p`
*projection*
    `T ^ x`
*selection*
    `T {x,y,...}`

The sieving operator (`T ? p`) produces rows of `T` satisfying
condition `p`.

.. htsql:: /school?code='art'

The projection operator (`T ^ x`) produces a flow of unique values of
`x` as it ranges over `T`.  Do not confuse the projection operator with
a projection complement.

.. htsql:: /program^degree
   :cut: 3

The selection operator specifies output columns.  The operator admits
two forms: with and without the selection base.

.. htsql:: /school{code, name}
   :cut: 3

.. htsql:: /{count(school), count(school?count(department)>2)}

Sieving, projection and selection operators have the same precedence
and are left-associative.

.. htsql::

   /school?count(department)>2
          ^campus
          {campus, avg(school.count(department))}

Composition and Linking
~~~~~~~~~~~~~~~~~~~~~~~

HTSQL has two traversal operators:

*composition*
    `S . T`
*linking*
    `x -> T`

The composition operator (`S . T`) evaluates expression `T` in the
context of flow `S`.

.. htsql:: /(school?code='art').program

The composition operator is left-associative.

The linking operator (`x -> T`) generates an ad-hoc link between the
input flow and flow `T` by associating each row from the input flow with
all rows from `T` such that the values of `x` evaluated against
respective rows coincide.

.. htsql:: /student{name, dob+}?count(dob -> student)>2
   :cut: 3

Sorting Decorators
~~~~~~~~~~~~~~~~~~

The following postfix decorators indicate ascending and descending
sorting order respectively:

    `x +`, `x -`

.. htsql:: /course.sort(department_code+,credits-)
   :cut: 3

Sorting decorators have the same precedence as infix function call.

Sorting decorators are only meaningful when used as arguments of the
`sort()` function and in a selector expression.

Assignment
~~~~~~~~~~

An assignment expression has the form:

    `T := x`

The left side of an assignment expression indicates the name and formal
parameters (if any) of a calculated attribute.  It must be an
identifier, a reference or a function call and can be preceded by an
optional dot-separated sequence of identifiers.

The right side of an assignment is an arbitrary expression indicating
the value of a calculated attribute.

.. htsql::
   :cut: 3

   /school{name, num_dept}?num_dept>=4
    :where school.num_dept := count(department)

An assignment expression could be used only as an argument of functions
`define()` and `where()`, or in a selector expression.


.. vim: set spell spelllang=en textwidth=72:
