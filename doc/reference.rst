***************************************
  Data Types, Functions and Operators
***************************************

This document describe built-in HTSQL data types, functions and
operators.


Data Types
==========

Every HTSQL expression has an associated type.  The type defines the set
of valid values produced by the expression and permitted operations over
the expression.  The type also indicates how returned values are
formatted in the output.

Since HTSQL wraps an SQL database, HTSQL data types are related to SQL
data type.  Although HTSQL does not expose SQL data types directly to
the user, each SQL data type corresponds to some HTSQL data type and
vice versa.

In this section we describe what data types HTSQL supports, how HTSQL
types are mapped to SQL types, the format of input literals for each
data type, etc.

Regular and Special Types
-------------------------

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
-------------------

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
type of an unquoted literal by applying a *cast* operator.

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
---------------

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



Boolean
-------

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

.. |oracle-native| replace:: ``NUMBER(1) CHECK (X IN (0, 1))``






Special Data Types
==================


Function Syntax
===============

A few observations about HTSQL's function and operator usage:

* For any function, "``f(x,y)``" can be written "``x :f(y)``" and
  depending upon grammatical context, abbreviated to "``x :f y``". 

* Unless annotated, functions are null-regular, that is, if any of 
  their arguments is ``null()`` then the result is ``null()``.

* HTSQL uses zero-based indexes, e.g. the 1st item in a collection is 
  indexed by ``0``, the 2nd character indexed by ``1``, and so on. 

* A single quoted string in an HTSQL request is an *untyped* literal,
  and is automatically cast depending upon the context -- it is not
  necessarily a string value.


Logical Operators
=================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `boolean(x)`         | cast *x* to Boolean       | ``boolean(true())``       | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean(false())``      | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean(1)``            | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | |boolean-from-string-in|  | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean(string(''))``   | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | |boolean-from-date-in|    | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean(null())``       | ``null()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | |boolean-from-null-s-in|  | ``false()``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `true()`             | logical *TRUE* value      | ``true()``                |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `false()`            | logical *FALSE* value     | ``false()``               |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `p & q`              | logical *AND* operator;   | ``true()&true()``         | ``true()``           |
|                      | treats nulls as *UNKNOWN* +---------------------------+----------------------+
|                      |                           | ``true()&false()``        | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()&false()``       | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``true()&null()``         | ``null()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()&null()``        | ``false()``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `p | q`              | logical *OR* operator;    | ``true()|true()``         | ``true()``           |
|                      | treats nulls as *UNKNOWN* +---------------------------+----------------------+
|                      |                           | ``true()|false()``        | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()|false()``       | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``true()|null()``         | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()|null()``        | ``null()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `\!p`                | logical *NOT* operator;   | ``!true()``               | ``false()``          |
|                      | treats nulls as *UNKNOWN* +---------------------------+----------------------+
|                      |                           | ``!false()``              | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``!null()``               | ``null()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `is_null(x)`         | *x* is null               | ``is_null(null())``       | ``true()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `x = y`              | *x* is equal to *y*       | ``'HTSQL'='QUEL'``        | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``2=null()``              | ``null()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `x != y`             | *x* is not equal to *y*   | ``'HTSQL'!='QUEL'``       | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``2!=null()``             | ``null()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `x == y`             | *x* is equal to *y*;      | ``'HTSQL'=='QUEL'``       | ``false()``          |
|                      | treats nulls as regular   +---------------------------+----------------------+
|                      | values                    | ``2==null()``             | ``false()``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `x !== y`            | *x* is not equal to *y*;  | ``'HTSQL'!=='QUEL'``      | ``true()``           |
|                      | treats nulls as regular   +---------------------------+----------------------+
|                      | values                    | ``2!==null()``            | ``true()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `x = {a,b,c,...}`    | *x* is among *a*, *b*,    | ``5={2,3,5,7}'``          | ``true()``           |
|                      | *c*, ...                  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x != {a,b,c,...}`   | *x* is not among *a*,     | ``5!={2,3,5,7}'``         | ``false()``          |
|                      | *b*, *c*, ...             |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x < y`              | *x* is less than *y*      | ``1<10``                  | ``true()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``'omega'<'alpha'``       | ``false()``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `x <= y`             | *x* is less than or equal | ``1<=10``                 | ``true()``           |
|                      | to *y*                    +---------------------------+----------------------+
|                      |                           | ``'omega'<='alpha'``      | ``false()``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `x > y`              | *x* is greater than *y*   | ``1>10``                  | ``false()``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``'omega'>'alpha'``       | ``true()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `x >= y`             | *x* is greater than or    | ``1>=10``                 | ``false()``          |
|                      | equal to *y*              +---------------------------+----------------------+
|                      |                           | ``'omega'>='alpha'``      | ``true()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| `if_null(x,y)`       | *x* if *x* is not null;   | ``if_null(1,0)``          | ``1``                |
|                      | *y* otherwise             +---------------------------+----------------------+
|                      |                           | ``if_null(null(),0)``     | ``0``                |
+----------------------+---------------------------+---------------------------+----------------------+
| `null_if(x,y)`       | *x* if *x* is not equal   | ``null_if(1,0)``          | ``1``                |
|                      | to *y*; null otherwise    +---------------------------+----------------------+
|                      |                           | ``null_if(0,0)``          | ``null()``           |
+----------------------+---------------------------+---------------------------+----------------------+
| |if-fn|              | first *ck* such that *pk* | |if-true-in|              | ``'up'``             |
+----------------------+ is *TRUE*; *o* or null    +---------------------------+----------------------+
| |if-else-fn|         | otherwise                 | |if-false-in|             | ``'down'``           |
+----------------------+---------------------------+---------------------------+----------------------+
| |switch-fn|          | first *ck* such that *x*  | |switch-1-in|             | ``'up'``             |
+----------------------+ is equal to *yk*; *o* or  +---------------------------+----------------------+
| |switch-else-fn|     | null otherwise            | |switch-0-in|             | ``'down'``           |
+----------------------+---------------------------+---------------------------+----------------------+

.. |boolean-from-string-in| replace:: ``boolean(string('HTSQL'))``
.. |boolean-from-date-in| replace:: ``boolean(date('2010-04-15'))``
.. |boolean-from-null-s-in| replace:: ``boolean(string(null()))``
.. |if-fn| replace:: `if(p1,c1,...,pn,cn)`
.. |if-else-fn| replace:: `if(p1,c1,...,pn,cn,o)`
.. |if-true-in| replace:: ``if(true(),'up','down')``
.. |if-false-in| replace:: ``if(false(),'up','down')``
.. |switch-fn| replace:: `switch(x,y1,c1,...,yn,cn)`
.. |switch-else-fn| replace:: `switch(x,y1,c1,...,yn,cn,o)`
.. |switch-1-in| replace:: ``switch(1,1,'up',0,'down')``
.. |switch-0-in| replace:: ``switch(0,1,'up',0,'down')``


Numeric Functions
=================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `integer(x)`         | cast *x* to integer       | ``integer(60)``           | ``60``               |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``integer(17.25)``        | ``17``               |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``integer(223607e-5)``    | ``2``                |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``integer(string('60'))`` | ``60``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `decimal(x)`         | cast *x* to decimal       | ``decimal(60)``           | ``60.0``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``decimal(17.25)``        | ``17.25``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``decimal(223607e-5)``    | ``2.23607``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | |decimal-from-string-in|  | ``17.25``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `float(x)`           | cast *x* to float         | ``float(60)``             | ``6e1``              |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``float(17.25)``          | ``1725e-2``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``float(223607e-5)``      | ``223607e-5``        |
|                      |                           +---------------------------+----------------------+
|                      |                           | |float-from-string-in|    | ``223607e-5``        |
+----------------------+---------------------------+---------------------------+----------------------+
| `-x`                 | negate *x*                | ``-7``                    |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x + y`              | add *x* to *y*            | ``13+7``                  | ``20``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `x - y`              | subtract *y* from *x*     | ``13-7``                  | ``6``                |
+----------------------+---------------------------+---------------------------+----------------------+
| `x * y`              | multiply *x* by *y*       | ``13*7``                  | ``91``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `x / y`              | divide *x* by *y*         | ``13/7``                  | ``1.85714285714286`` |
+----------------------+---------------------------+---------------------------+----------------------+
| `round(x)`           | round *x* to the nearest  | ``round(17.25)``          | ``17``               |
|                      | integer                   |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `round(x,n)`         | round *x* to *n* decimal  | ``round(17.25,1)``        | ``17.3``             |
|                      | places                    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |decimal-from-string-in| replace:: ``decimal(string('17.25'))``
.. |float-from-string-in| replace:: ``float(string('223607e-5'))``


String Functions
================

By convention, string functions take a string as its first parameter.
When an untyped literal, such as ``'value'`` is used and a string is
expected, it is automatically cast.  Hence, for convenience, we write
string typed values using single quotes in the output column.

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `string(x)`          | cast *x* to string        | ``string('Hello')``       | ``'Hello'``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``string(1.0)``           | ``'1.0'``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``string(null())``        | ``null()``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``string(true())``        | ``'true'``           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``string(false())``       | ``'false'``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | |string-from-date-in|     | ``'2010-04-15'``     |
+----------------------+---------------------------+---------------------------+----------------------+
| `length(s)`          | number of characters      | ``length('HTSQL')``       | ``5``                |
|                      | in *s*                    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x + y`              | concatenate *x* and *y*;  | ``'Hello' + ' World'``    | ``'Hello World'``    |
|                      | treats nulls as empty     +---------------------------+----------------------+
|                      | strings                   | ``'Hello' + null()``      | ``'Hello'``          |
|                      |                           |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x ~ y`              | *x* contains *y*;         | ``'HTSQL' ~ 'sql'``       | ``true()``           |
|                      | case-insensitive          |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x !~ y`             | *x* does not contain      | ``'HTSQL' !~ 'sql'``      | ``false()``          |
|                      | *y*; case-insensitive     |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `head(s)`            | first character of *s*    | ``head('HTSQL')``         | ``'H'``              |
+----------------------+---------------------------+---------------------------+----------------------+
| `head(s,n)`          | first *n* characters      | ``head('HTSQL',2)``       | ``'HT'``             |
|                      | of *s*                    +---------------------------+----------------------+
|                      |                           | ``head('HTSQL',-3)``      | ``'HT'``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `tail(s)`            | last character of *s*     | ``tail('HTSQL')``         | ``'L'``              |
+----------------------+---------------------------+---------------------------+----------------------+
| `tail(s,n)`          | last *n* characters       | ``tail('HTSQL',3)``       | ``'SQL'``            |
|                      | of *s*                    +---------------------------+----------------------+
|                      |                           | ``tail('HTSQL',-2)``      | ``'SQL'``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `slice(s,i,j)`       | *i*-th to *j*-th          | ``slice('HTSQL',1,4)``    | ``'TSQ'``            |
|                      | characters of *s*; null   +---------------------------+----------------------+
|                      | or missing index means    | ``slice('HTSQL',-4,-1)``  | ``'TSQ'``            |
|                      | the beginning or the end  +---------------------------+----------------------+
|                      | of the string             | |slice-start-in|          | ``'HT'``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | |slice-end-in|            | ``'SQL'``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `at(s,k)`            | *k*-th character of *s*   | ``at('HTSQL',2)``         | ``'S'``              |
+----------------------+---------------------------+---------------------------+----------------------+
| `at(s,k,n)`          | *n* characters of *s*     | ``at('HTSQL',1,3)``       | ``'TSQ'``            |
|                      | starting with *k*-th      +---------------------------+----------------------+
|                      | character                 | ``at('HTSQL,-4,3)``       | ``'TSQ'``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``at('HTSQL,4,-3)``       | ``'TSQ'``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `upper(s)`           | upper case of *s*         | ``upper('htsql')``        | ``'HTSQL'``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `lower(s)`           | lower case of *s*         | ``lower('HTSQL')``        | ``'htsql'``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `trim(s)`            | strip leading and         | ``trim('  HTSQL  ')``     | ``'HTSQL'``          |
|                      | trailing spaces from *s*  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `ltrim(s)`           | strip leading spaces      | ``ltrim('  HTSQL  ')``    | ``'HTSQL  '``        |
|                      | from *s*                  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `rtrim(s)`           | strips trailing spaces    | ``rtrim('  HTSQL  ')``    | ``'  HTSQL'``        |
|                      | from *s*                  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `replace(s,x,y)`     | replace all occurences    | |replace-in|              | ``'HTRAF'``          |
|                      | of *x* in *s* with *y*;   +---------------------------+----------------------+
|                      | in *s* with *y*; null *x* | |replace-null-in|         | ``'HTSQL'``          |
|                      | is treated as an empty    |                           |                      |
|                      | string                    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |string-from-date-in| replace:: ``string(date('2010-04-15'))``
.. |slice-start-in| replace:: ``slice('HTSQL',null(),2)``
.. |slice-end-in| replace:: ``slice('HTSQL',2,null())``
.. |replace-in| replace:: ``replace('HTSQL','SQL','RAF')``
.. |replace-null-in| replace:: ``replace('HTSQL',null(),'RAF')``


Date Functions
==============

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `date(x)`            | cast *x* to date          | ``date('2010-04-15')``    |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `date(yyyy,mm,dd)`   | date *yyyy-mm-dd*         | ``date(2010,4,15)``       | |date-out|           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``date(2010,3,46)``       | |date-out|           |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``date(2011,-8,15)``      | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `today()`            | current date              | ``today()``               |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `year(d)`            | year of *d*               | |year-in|                 | ``2010``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `month(d)`           | month of *d*              | |month-in|                | ``4``                |
+----------------------+---------------------------+---------------------------+----------------------+
| `day(d)`             | day of *d*                | |day-in|                  | ``15``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `d + n`              | increment *d* by *n* days | |date-inc-in|             | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `d - n`              | decrement *d* by *n* days | |date-dec-in|             | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `d1 - d2`            | number of days between    | |date-diff-in|            | ``13626``            |
|                      | *d1* and *d2*             |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |date-out| replace:: ``date('2010-04-15')``
.. |year-in| replace:: ``year(date('2010-04-15'))``
.. |month-in| replace:: ``month(date('2010-04-15'))``
.. |day-in| replace:: ``day(date('2010-04-15'))``
.. |date-inc-in| replace:: ``date('1991-08-20')+6813``
.. |date-dec-in| replace:: ``date('2028-12-09')-6813``
.. |date-diff-in| replace:: ``date('2028-12-09')-date('1991-08-20')``


Aggregate Functions
===================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `exists(ps)`         | *TRUE* if *ps* contains   | |exists-in|               |                      |
|                      | at least one *TRUE*       |                           |                      |
|                      | value; *FALSE* otherwise  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `every(ps)`          | *TRUE* if *ps* contains   | |every-in|                |                      |
|                      | only *TRUE* values;       |                           |                      |
|                      | *FALSE* otherwise         |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `count(ps)`          | number of *TRUE* values   | |count-in|                |                      |
|                      | in *ps*                   |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `min(xs)`            | smallest *x* in *sx*      | ``min(course.credits)``   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `max(xs)`            | largest *x* in *sx*       | ``max(course.credits)``   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `sum(xs)`            | sum of *x* in *xs*        | ``sum(course.credits)``   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `avg(xs)`            | average value of *x*      | ``avg(course.credits)``   |                      |
|                      | in *xs*                   |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |exists-in| replace:: ``exists(course.credits>5)``
.. |every-in| replace:: ``every(course.credits>5)``
.. |count-in| replace:: ``count(course.credits>5)``


Navigation Operations
=====================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `chain . link`       | traverse a link           | ``school.program``        |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain . attr`       | extract attribute value   | ``school.name``           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain . \*`         | extract all attributes    | ``school.*``              |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain ? p`          | records from *chain*      | ``school?code='edu'``     |                      |
|                      | satisfying condition *p*  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain.sort(x,...)`  | records from *chain*      | ``course.sort(credits-)`` |                      |
|                      | sorted by *x*, ...        |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain.limit(n)`     | first *n* records from    | ``course.limit(10)``      |                      |
|                      | *chain*                   |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain.limit(n,k)`   | *n* records from *chain*  | ``course.limit(10,20)``   |                      |
|                      | starting from *k*-th      |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `chain {x,...}`      | select *x*, ... from      | ``school{code,name}``     |                      |
|                      | *chain*                   |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `root()`             | scalar class              |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `this()`             | current chain             |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+


Decorators
==========

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `as(x,title)`        | set the column title      | ``number :as 'No.'``      |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x+`                 | sort by *x* in            | ``credits+``              |                      |
|                      | ascending order           |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x-`                 | sort by *x* in            | ``credits-``              |                      |
|                      | descending order          |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+


Formatters
==========

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `/:html`             | HTML tabular output       |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `/:txt`              | plain text tabular output |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `/:csv`              | CSV (comma-separated      |                           |                      |
|                      | values) output            |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `/:json`             | JSON-serialized output    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+


.. vim: set spell spelllang=en textwidth=72:
