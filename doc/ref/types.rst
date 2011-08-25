**************
  Data Types
**************

Every HTSQL expression has an associated type.  The type defines the set
of valid values produced by the expression and permitted operations over
the expression.  The type also indicates how returned values are
formatted in the output.

Since HTSQL wraps an SQL database, HTSQL data types are related to SQL
data types.  Although HTSQL does not expose SQL data types directly to
the user, each SQL data type corresponds to some HTSQL data type and
vice versa.

In this section we describe what data types HTSQL supports, how HTSQL
types are mapped to SQL types, the format of input literals for each
data type, etc.


Regular and Special Types
=========================

HTSQL demands that every expression has an associated type.  For
example, in the query:

.. htsql:: /{2+2=4, count(school), date('2010-04-15')-6813}

the expressions ``2+2=4``, ``count(school)``,
``date('2010-04-15')-6813`` have the types `boolean`, `integer` and
`date` respectively.  These are *regular* types.

The following table lists the default set of supported regular data
types in HTSQL; more data types could be added by *HTSQL extensions*.

+----------------------+---------------------------+---------------------------+
| Type                 | Description               | Example Input             |
+======================+===========================+===========================+
| `boolean`            | logical data type, with   | ``true()``                |
|                      | two values: *TRUE* and    +---------------------------+
|                      | *FALSE*                   | ``false()``               |
+----------------------+---------------------------+---------------------------+
| `integer`            | binary integer type       | ``4096``                  |
+----------------------+---------------------------+---------------------------+
| `decimal`            | arbitrary-precision       | ``124.49``                |
|                      | exact numeric type        |                           |
+----------------------+---------------------------+---------------------------+
| `float`              | IEEE 754 floating-point   | ``271828e-5``             |
|                      | inexact numeric type      |                           |
+----------------------+---------------------------+---------------------------+
| `string`             | text data type            | ``string('HTSQL')``       |
+----------------------+---------------------------+---------------------------+
| `enum`               | enumeration data type,    |                           |
|                      | with predefined set of    |                           |
|                      | valid string values       |                           |
+----------------------+---------------------------+---------------------------+
| `date`               | calendar date             | ``date('2010-04-15')``    |
+----------------------+---------------------------+---------------------------+
| `time`               | time of day               | ``time('20:13:04.5')``    |
+----------------------+---------------------------+---------------------------+
| `datetime`           | date and time combined    | |datetime-in|             |
+----------------------+---------------------------+---------------------------+
| `opaque`             | unrecognized data type    |                           |
+----------------------+---------------------------+---------------------------+

.. |datetime-in| replace:: ``datetime('2010-04-15 20:13:04.5')``

Some HTSQL expressions do not produce a proper value and therefore
cannot be assigned a regular data type.  In this case, the expression is
assigned one of the *special* data types: `record`, `untyped` or `void`.

Record entities are assigned the `record` type.  This type is special
since values of this type are never displayed directly and it has no
corresponding SQL data type.

Quoted HTSQL literals have no intrinsic data type; their actual type is
determined from the context.  Until it is determined, HTSQL translator
assign them a temporary `untyped` type.

Some expressions, such as assignments, produce no values and therefore
have no meaningful data type.   In this case, the assigned type is
`void`.

The following table lists supported special data types.

+----------------------+---------------------------+---------------------------+
| Type                 | Description               | Example Input             |
+======================+===========================+===========================+
| `record`             | type of record entities   | ``school``                |
+----------------------+---------------------------+---------------------------+
| `untyped`            | initial type of quoted    | ``'HTSQL'``               |
|                      | literals                  |                           |
+----------------------+---------------------------+---------------------------+
| `void`               | type without any valid    |                           |
|                      | values                    |                           |
+----------------------+---------------------------+---------------------------+

Literal Expressions
===================

A literal expression is an atomic expression that represents a fixed
value.  HTSQL supports two types of literals: *numeric* (or unquoted) and
*quoted*.

An unquoted literal is a number written in one of the following forms:

* an integer number
* a number with a decimal point
* a number in exponential notation

.. htsql:: /{60, 2.125, 271828e-5}

Literals in these forms are assigned `integer`, `decimal` and `float`
types respectively.

A quoted literal is an arbitrary string value enclosed in single quotes.

.. htsql:: /{'', 'HTSQL', 'O''Reilly'}

In this example, three literal expressions represent an empty string,
*HTSQL* and *O'Reilly* respectively.  Note that to represent a single
quote in the value, we must duplicate it.

As opposed to numeric literals, quoted literals have no intrinsic type,
their type is determined from the context.  Specifically, the type of
a quoted literal is inferred from the innermost expression that contains
the literal.  Until the actual data type of a quoted literal is
determined, the literal is assigned an `untyped` type.

Consider a query:

.. htsql:: /2+2='4'

Here, a quoted literal ``'4'`` is a right operand of an equality
expression, and its left counterpart ``2+2`` has the type `integer`.
Therefore, HTSQL processor is able to infer `integer` for the literal
``'4'``.

There is no generic rule how to determine the type of a quoted literal;
every operator and function have different rules how to treat untyped
values.  However the content of the literal is never examined when
determining its data type.  It is possible to explicitly specify the
type of an unquoted literal by applying a *cast* function.

.. htsql:: /{string('2010-04-15'), date('2010-04-15')}

Here, the same quoted literal is converted to `string` and `date` data
types respectively.  Each data type has a set of quoted literals it
accepts; it is an error when the quoted literal does not obey the format
expected by a particular type.

.. htsql:: /{integer('HTSQL')}
   :error:

Note the error generated because ``'HTSQL'`` is not a valid format for
an integer literal.

Type Conversion
===============

Expressions of one type could be explicitly converted to another type
using a *cast* function.  A cast function is a regular function with one
argument; the name of the function coincides with the name of the target
type.

Not every conversion is permitted; for instance, an integer value could
be converted to a string, but not to a date:

.. htsql:: /string(60)

.. htsql:: /date(60)
   :error:

Implicit type conversion is called *coercion*.  In an arithmetic
formulas and other expressions that require homogeneous arguments, when
the operands are of different types, values of less generic types are
converted to the most generic type.  The order of conversion is as
follows:

* `integer`
* `decimal`
* `float`


Boolean Type
============

Type `boolean` is a logical data type with two values: *TRUE*
and *FALSE*.

.. htsql:: /{boolean('true'), boolean('false')}

.. htsql:: /{true(), false()}

The following table maps the `boolean` type to respective
native data types.

+----------------------+---------------------------+
| Backend              | Native types              |
+======================+===========================+
| *sqlite*             | ``BOOL``, ``BOOLEAN`` or  |
|                      | any type containing       |
|                      | ``BOOL`` in its name      |
+----------------------+---------------------------+
| *pgsql*              | ``BOOLEAN``               |
+----------------------+---------------------------+
| *mysql*              | ``BOOL`` aka ``BOOLEAN``  |
|                      | aka ``TINYINT(1)``        |
+----------------------+---------------------------+
| *oracle*             | |oracle-native|           |
+----------------------+---------------------------+
| *mssql*              | ``BIT``                   |
+----------------------+---------------------------+

.. |oracle-native| replace:: ``NUMBER(1) CHECK (_ IN (0, 1))``


Numeric Types
=============


String Type
===========


Enum Type
=========


Date/Time Types
===============


.. vim: set spell spelllang=en textwidth=72:
