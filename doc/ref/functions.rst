***************************
  Functions and Operators
***************************

This document describes built-in functions and operators.


Logical Functions and Operators
===============================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `boolean(x)`         | cast *x* to Boolean       | ``boolean('true')``       | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean('false')``      | ``false``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | |boolean-from-string-in|  | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``boolean(string(''))``   | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `true()`             | logical *TRUE* value      | ``true()``                |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `false()`            | logical *FALSE* value     | ``false()``               |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `p & q`              | logical *AND* operator    | ``true()&true()``         | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``true()&false()``        | ``false``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()&false()``       | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `p | q`              | logical *OR* operator     | ``true()|true()``         | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``true()|false()``        | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``false()|false()``       | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `\!p`                | logical *NOT* operator    | ``!true()``               | ``false``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``!false()``              | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `null(x)`            | *NULL* value              | ``null()``                |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `is_null(x)`         | *x* is null               | ``is_null(null())``       | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `if_null(x,y)`       | *x* if *x* is not null;   | ``if_null(1,0)``          | ``1``                |
|                      | *y* otherwise             +---------------------------+----------------------+
|                      |                           | ``if_null(null(),0)``     | ``0``                |
+----------------------+---------------------------+---------------------------+----------------------+
| `null_if(x,y)`       | *x* if *x* is not equal   | ``null_if(1,0)``          | ``1``                |
|                      | to *y*; null otherwise    +---------------------------+----------------------+
|                      |                           | ``null_if(0,0)``          | ``null``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `x = y`              | *x* is equal to *y*       | ``'HTSQL'='QUEL'``        | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x != y`             | *x* is not equal to *y*   | ``'HTSQL'!='QUEL'``       | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `x == y`             | *x* is equal to *y*;      | ``'HTSQL'=='QUEL'``       | ``false``            |
|                      | treats nulls as regular   +---------------------------+----------------------+
|                      | values                    | ``2==null()``             | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x !== y`            | *x* is not equal to *y*;  | ``'HTSQL'!=='QUEL'``      | ``true``             |
|                      | treats nulls as regular   +---------------------------+----------------------+
|                      | values                    | ``2!==null()``            | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `x = {a,b,c,...}`    | *x* is among *a*, *b*,    | ``5={2,3,5,7}'``          | ``true``             |
|                      | *c*, ...                  |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x != {a,b,c,...}`   | *x* is not among *a*,     | ``5!={2,3,5,7}'``         | ``false``            |
|                      | *b*, *c*, ...             |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `x < y`              | *x* is less than *y*      | ``1<10``                  | ``true``             |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``'omega'<'alpha'``       | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x <= y`             | *x* is less than or equal | ``1<=10``                 | ``true``             |
|                      | to *y*                    +---------------------------+----------------------+
|                      |                           | ``'omega'<='alpha'``      | ``false``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `x > y`              | *x* is greater than *y*   | ``1>10``                  | ``false``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``'omega'>'alpha'``       | ``true``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `x >= y`             | *x* is greater than or    | ``1>=10``                 | ``false``            |
|                      | equal to *y*              +---------------------------+----------------------+
|                      |                           | ``'omega'>='alpha'``      | ``true``             |
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
.. |if-fn| replace:: `if(p1,c1,...,pn,cn)`
.. |if-else-fn| replace:: `if(p1,c1,...,pn,cn,o)`
.. |if-true-in| replace:: ``if(true(),'up','down')``
.. |if-false-in| replace:: ``if(false(),'up','down')``
.. |switch-fn| replace:: `switch(x,y1,c1,...,yn,cn)`
.. |switch-else-fn| replace:: `switch(x,y1,c1,...,yn,cn,o)`
.. |switch-1-in| replace:: ``switch(1,1,'up',0,'down')``
.. |switch-0-in| replace:: ``switch(0,1,'up',0,'down')``

Boolean Cast
------------

`boolean(x)`
    Convert `x` to Boolean.

The result of the conversion depends on the type of the argument:

`untyped`
    The literal ``'false'`` is converted to *FALSE*, the literal
    ``'true'`` is converted to *TRUE*, any other literals generate an
    error.
`boolean`
    The value is unchanged.
`string`
    *NULL* and an empty string are converted to *FALSE*, other values
    are converted to *TRUE*.
other data types
    `null` values are converted to *FALSE*, other values are converted
    to *TRUE*.

.. htsql:: /{boolean('false'), boolean('true')}

.. htsql:: /{boolean(null()), boolean(false()), boolean(true())}

.. htsql:: /{boolean(string(null())), boolean(string('')),
             boolean(string('HTSQL'))}

.. htsql:: /{boolean(integer(null())), boolean(0.0),
             boolean(date('2010-04-15'))}

Logical Values
--------------

`true()`
    Logical *TRUE* value.

`false()`
    Logical *FALSE* value.

.. htsql:: /{true(), false()}

Logical Operators
-----------------

`p | q`
    Logical *OR* operator.

`p & q`
    Logical *AND* operator.

`\! p`
    Logical *NOT* operator.

Arguments of a logical operators that are not of a Boolean type
automatically converted to Boolean (see `boolean()` function).

.. htsql:: /{true()|true(), true()|false(),
             false()|true(), false()|false()}

.. htsql:: /{true()&true(), true()&false(),
             false()&true(), false()&false()}

.. htsql:: /{!true(), !false()}

.. htsql::

   /{true()&null(), false()&null(), null()&null(),
     true()|null(), false()|null(), null()|null(),
     !null()}

.. htsql:: /school?exists(program)&exists(department)|!campus
   :cut: 3

NULL Checking
-------------

`null()`
    Untyped *NULL* value.
`is_null(x)`
    *TRUE* if `x` is *NULL*, *FALSE* otherwise.
`if_null(x,y)`
    `x` if `x` is not *NULL*, `y` otherwise.
`null_if(x,y)`
    `x` if `x` is not equal to `y`, *NULL* otherwise.

The arguments of `if_null()` and `null_if()` should be of the same type;
if not, the arguments are coerced to the most general type.

.. htsql:: /{null()}

.. htsql:: /{is_null(null()), is_null(0)}

.. htsql:: /{if_null('SQL','HTSQL'), if_null(null(),'HTSQL')}

.. htsql:: /{null_if('HTSQL','SQL'), null_if('SQL','SQL')}

.. htsql:: /course{title, credits}?is_null(credits)

.. htsql:: /course{title, credits}?(credits :if_null 0)=0

.. htsql:: /course{title, credits}?!(credits :null_if 0)

Equality Operators
------------------

`x = y`
    *TRUE* if `x` is equal to `y`, *FALSE* otherwise.  Returns *NULL* if
    any of the operands is *NULL*.
`x != y`
    *TRUE* if `x` is not equal to `y`, *FALSE* otherwise.  Returns
    *NULL* if any of the operands is *NULL*.
`x == y`
    *TRUE* if `x` is equal to `y`, *FALSE* otherwise.  Treats *NULL* as
    a regular value.
`x !== y`
    *TRUE* if `x` is not equal to `y`, *FALSE* otherwise.  Treats *NULL*
    as a regular value.
`x = {a,b,c,...}`
    *TRUE* if `x` is equal to *some* value among `a,b,c,...`, *FALSE*
    otherwise.
`x != {a,b,c,...}`
    *TRUE* if `x` is not equal to *all* values among `a,b,c,...`,
    *FALSE* otherwise.

The form `x = {a,b,c,...}` is a short-cut syntax for `x=a|x=b|x=c|...`.
Similarly, the form `x != {a,b,c,...}` is a short-cut syntax for
`x!=a|x!=b|x!=c|...`.

The operands of equality operators are expected to be of the same time.
If the types of the operands are different, the operands are coerced to
the most general type; it is an error if the operand types are not
compatible to each other.

.. htsql:: /{1=1.0, 'HTSQL'!='SQUARE'}

.. htsql:: /{0!=null(), null()=null(), 0!==null(), null()==null()}

.. htsql:: /'HTSQL'!={'ISBL','SQUARE','QUEL'}

.. htsql:: /school?campus='old'
   :cut: 3

.. htsql:: /school?campus!={'north','south'}
   :cut: 3

.. htsql:: /school{code, campus=='old', campus=='north', campus=='south'}
   :cut: 3

Comparison Operators
--------------------

`x < y`
    *TRUE* if `x` is less than `y`, *FALSE* otherwise.
`x <= y`
    *TRUE* if `x` is less than or equal to `y`, *FALSE* otherwise.
`x > y`
    *TRUE* if `x` is greater than `y`, *FALSE* otherwise.
`x >= y`
    *TRUE* if `x` is greater than or equal to `y`, *FALSE* otherwise.

The result is *NULL* if any of the operands is *NULL*.

An operand of a comparison operator must be of a string, numeric,
enumeration, or date/time type.  Both operands are expected to be of
the same type; if not, the operands are coerced to the most general
type.

.. htsql:: /{23<=17.5, 'HTSQL'<'SQUARE',
             date('2010-04-15')>=date('1991-08-20')}

.. htsql:: /school?count(department)>=4
   :cut: 3

Branching Functions
-------------------

`if(p1,c1,p2,c2,...,pn,cn[,o])`
    This function takes *N* logical expressions `p1,p2,...,pN`
    interleaved with *N* values `c1,c2,...,cN`, followed by an optional
    value `o`.  The function returns the value `ck` corresponding to the
    first predicate `pk` evaluated to *TRUE*.  If none of the predicates
    are evaluated to *TRUE*, the value of `o` is returned, or *NULL* if
    `o` is not specified.
`switch(x,y1,c1,y2,c2,...,yn,cn[,o])`
    This function takes a control expression `x` followed by *N* variant
    values `y1,y2,...,yN` interleaved with *N* resulting values
    `c1,c2,...,cN`, and concluded with an optional default value `o`.
    The function returns the value `ck` corresponding to the first
    variant `yk` equal to `x`.  If none of the variants are equal to the
    control value, `o` is returned, or *NULL* if `o` is not specified.

These functions expect all the resulting values `c1,c2,...,cN` as well
as the default value `o` to be of the same type.  If the value types
are different, all values are coerced to the most general type.  Same
is true for the control expression `x` and variant values `y1,y2,...,yN`
of the function `switch()`.

.. htsql::
   :cut: 3

   /course{title, if(credits>=5, 'hard',
                     credits>=3, 'medium',
                                 'easy') :as level}
          ?department.code='astro'

.. htsql::
   :cut: 3

   /student{name, switch(gender, 'm', 1,
                                 'f', -1) :as sex_code}
           ?program.code='gedu'


Numeric Functions
=================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `integer(x)`         | cast *x* to integer       | ``integer('60')``         | ``60``               |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``integer(17.25)``        | ``17``               |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``integer(string('60'))`` | ``60``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `decimal(x)`         | cast *x* to decimal       | ``decimal('17.25')``      | ``17.25``            |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``decimal(223607e-5)``    | ``2.23607``          |
|                      |                           +---------------------------+----------------------+
|                      |                           | |decimal-from-string-in|  | ``17.25``            |
+----------------------+---------------------------+---------------------------+----------------------+
| `float(x)`           | cast *x* to float         | ``float('223607e-5')``    | ``223607e-5``        |
|                      |                           +---------------------------+----------------------+
|                      |                           | ``float(60)``             | ``6e1``              |
|                      |                           +---------------------------+----------------------+
|                      |                           | |float-from-string-in|    | ``223607e-5``        |
+----------------------+---------------------------+---------------------------+----------------------+
| `+ x`                | *x*                       | ``+60``                   |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `- x`                | negate *x*                | ``-7``                    |                      |
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
| `trunc(x)`           | round *x* to an integer,  | ``trunc(17.25)``          | ``17``               |
|                      | towards zero              |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `trunc(x,n)`         | round *x* to *n* decimal  | ``trunc(17.25,1)``        | ``17.2``             |
|                      | places, towards zero      |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |decimal-from-string-in| replace:: ``decimal(string('17.25'))``
.. |float-from-string-in| replace:: ``float(string('223607e-5'))``

Numeric Cast
------------

`integer(x)`
    Convert `x` to integer.
`decimal(x)`
    Convert `x` to decimal.
`float(x)`
    Convert `x` to float.

The argument of a conversion function can be of one of the following
types:

*untyped*
    An untyped literal must be a valid number.  The `integer()` function
    accepts only integer literals, `decimal()` and `float()` accepts
    untyped literals written in integer, decimal or scientific notation.
*numeric*
    Numeric cast functions convert numbers between different storage
    forms.  Behavior on range overflow and rounding rules are
    backend-dependent.
*string*
    A string value must contain a valid number.  The set of allowed
    input values depends on the backend.

.. htsql:: /{integer(2.125), decimal('271828e-5'), float(string(60))}

Arithmetic Expressions
----------------------

`+ x`
    Return `x`.
`- x`
    Negate `x`.
`x + y`
    Add `x` to `y`.
`x - y`
    Subtract `y` from `x`.
`x * y`
    Multiply `x` by `y`.
`x / y`
    Divide `x` by `y`.

Arithmetic operators expect operands of a numeric type.  If the operands
are of different types, they are coerced to the most general type, in
the order: *integer*, *decimal*, *float*.  For instance, adding an
integer value to a decimal value converts the integer operand to
decimal; multiplying a decimal value to a float value converts the
decimal operand to float.

In general, the type of the result coincides with the type of the
operands.  The only exception is the division operator: when applied to
integer operands, division produces a decimal value.

The behavior of arithmetic expressions on range overflow or division by
zero is backend-dependent: different backends may raise an error, return
a *NULL* value or generate an incorrect result.

Note that some arithmetic operators are also defined for *string*
and *date* values; they are described in respective sections.

.. htsql:: /{(2+4)*7, -(98-140), 21/5}

Rounding Functions
------------------

`round(x)`
    Round `x` to the nearest integer value.
`round(x,n)`
    Round `x` to `n` decimal places.
`trunc(x)`
    Round `x` to an integer, towards zero.
`trunc(x,n)`
    Round `x` to `n` decimal places, towards zero.

If called with one argument, the functions accept values of *decimal* or
*float* types and return a value of the same type.

When called with two arguments, the functions expects a *decimal* argument
and produces a *decimal* value.  The second argument should be an integer;
negative values are permitted.

.. htsql:: /{round(3272.78125),
             round(3272.78125,2),
             round(3272.78125,-2)}

.. htsql:: /{trunc(3272.78125),
             trunc(3272.78125,2),
             trunc(3272.78125,-2)}

.. htsql:: /school{code, avg(department.count(course)) :round 2}
   :cut: 3

.. htsql::

   /department^avg_credits {avg_credits, count(department)}
    :where department.avg_credits := avg(course.credits) :trunc(1)


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
|                      |                           | |string-from-date-in|     | ``'2010-04-15'``     |
+----------------------+---------------------------+---------------------------+----------------------+
| `length(s)`          | number of characters      | ``length('HTSQL')``       | ``5``                |
|                      | in *s*                    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `s + t`              | concatenate *s* and *t*   | ``'HT' + 'SQL'``          | ``'HTSQL'``          |
+----------------------+---------------------------+---------------------------+----------------------+
| `s ~ t`              | *s* contains *t*;         | ``'HTSQL' ~ 'sql'``       | ``true``             |
|                      | case-insensitive          |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `s !~ t`             | *s* does not contain      | ``'HTSQL' !~ 'sql'``      | ``false``            |
|                      | *t*; case-insensitive     |                           |                      |
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
| `replace(s,t,r)`     | replace all occurences    | |replace-in|              | ``'HTRAF'``          |
|                      | of *t* in *s* with *r*    |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |string-from-date-in| replace:: ``string(date('2010-04-15'))``
.. |string-from-dt-in| replace:: ``string(datetime('2010-04-15 20:13'))``
.. |string-from-dt-out| replace:: ``'2010-04-15 20:13'``
.. |slice-start-in| replace:: ``slice('HTSQL',null(),2)``
.. |slice-end-in| replace:: ``slice('HTSQL',2,null())``
.. |replace-in| replace:: ``replace('HTSQL','SQL','RAF')``

String Cast
-----------

`string(x)`
    Convert `x` to a string.

HTSQL permits any value to be converted to a string; the conversion
respects the format for literals of the original type.

.. htsql:: /{string('HTSQL'), string(true()), string(2.125),
             string(datetime('2010-04-15 20:13'))}

.. htsql::
   :cut: 3

   /department{'Department of '+name+' offers '
               +string(count(course))+' courses' :as text}
              ?exists(course)

String Length
-------------

`length(s)`
    Number of characters in `s`.

The exact meaning of a string length depends on the backend and the
underlying SQL type.  The function returns ``0`` if the argument is
*NULL*.

.. htsql:: /{length('HTSQL'), length(''), length(null())}

Concatenation
-------------

`s + t`
    Concatenate `s` and `t`.

The concatenation operator treats a *NULL* operand as an empty string.

.. htsql:: /{'HT'+'SQL', null()+'SQL'}

.. htsql:: /course{department_code+'.'+string(no) :as code, title}
   :cut: 3

Substring Search
----------------

`s ~ t`
    *TRUE* if `t` is a substring of `s`, *FALSE* otherwise.
`s !~ t`
    *TRUE* if `t` is a substring of `s`, *FALSE* otherwise.

The search functions are case-insensitive; exact rules for
case-insensitivity depend on the backend.

.. htsql:: /{'HTSQL'~'sql', 'sql'!~'HTSQL'}

.. htsql:: /school?code~'art'

Substring Extraction
--------------------

`head(s)`
    The first character of `s`.
`head(s,n)`
    The first `n` characters of `s`.
`tail(s)`
    The last character of `s`.
`tail(s,n)`
    The last `n` characters of `s`.
`slice(s,i,j)`
    The `i`-th to `j`-th (exclusive) characters of `s`.
`at(s,k)`
    The `k`-th character of `s`.
`at(s,k,n)`
    `n` characters of `s` starting from the `k`-th.

In HTSQL, characters of a string are indexed from `0`.

Extraction functions permit negative or *NULL* indexes.  `head()`
(`tail()`), when given a negative `n`, produces all but the last (first)
`-n` characters of `s`; if `n` is *NULL*, it is assumed to be ``1``.

For `slice()`, a negative index `i` or `j` indicates to count
`(-i-1)`-th (`(-j-1)`-th) character from the end of `s`.  *NULL* value
for `i` or `j` indicates the beginning (the end) of the string.

For `at()`, a negative `n` produces `-n` characters of `s`
ending at the `k`-th character; if `n` is *NULL*, it is assumed to
be ``1``.

.. htsql:: /{'HTSQL' :head, 'HTSQL' :head(2), 'HTSQL' :head(-3)}

.. htsql:: /{'HTSQL' :tail, 'HTSQL': tail(3), 'HTSQL': tail(-2)}

.. htsql:: /{'HTSQL' :slice(1,-1), 'HTSQL' :slice(1,null()),
             'HTSQL' :slice(null(),-1)}

.. htsql:: /{'HTSQL' :at(2), 'HTSQL' :at(1,3), 'HTSQL': at(-1,-3)}

Case Conversion
---------------

`upper(s)`
    Convert `s` to upper case.
`lower(s)`
    Convert `s` to lower case.

The conversion semantics is backend-dependent.

.. htsql:: /{'htsql' :upper, 'HTSQL' :lower}

String Trimming
---------------

`trim(s)`
    Strip leading and trailing spaces from `s`.
`ltrim(s)`
    Strip leading spaces from `s`.
`rtrim(s)`
    Strip trailing spaces from `s`.

.. htsql::

   /{'  HTSQL  ' :trim :replace(' ','!'),
     '  HTSQL  ' :ltrim :replace(' ','!'),
     '  HTSQL  ' :rtrim :replace(' ','!')}

Search and Replace
------------------

`replace(s,t,r)`
    Replace all occurences of substring `t` in `s` with `r`.

Case-sensitivity of the search depends on the backend; *NULL* values for
`t` and `r` are interpreted as an empty string.

.. htsql::

   /{'HTTP' :replace('TP','SQL'),
     'HTTP' :replace(null(), 'SQL'),
     'HTTP' :replace('TP', null())}


Date/Time Functions
===================

+----------------------+---------------------------+---------------------------+----------------------+
| Function             | Description               | Example Input             | Output               |
+======================+===========================+===========================+======================+
| `date(x)`            | cast *x* to date          | ``date('2010-04-15')``    |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `time(x)`            | cast *x* to time          | ``time('20:13')``         |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `datetime(x)`        | cast *x* to datetime      | |dt-from-untyped-in|      |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `date(yyyy,mm,dd)`   | date *yyyy-mm-dd*         | ``date(2010,4,15)``       | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| |dt-cr-fn|           | datetime *yyyy-mm-dd*     | |dt-cr-in|                | |dt-out|             |
|                      | *HH:MM:SS*                |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `datetime(d,t)`      | datetime from date and    | |dt-dt-in|                | |dt-out|             |
|                      | time                      |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `today()`            | current date              | ``today()``               |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `now()`              | current date and time     | ``now()``                 |                      |
+----------------------+---------------------------+---------------------------+----------------------+
| `date(dt)`           | date of *dt*              | |date-from-dt-in|         | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `time(dt)`           | time of *dt*              | |time-from-dt-in|         | |time-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `year(d)`            | year of *d*               | |year-in|                 | ``2010``             |
+----------------------+---------------------------+---------------------------+----------------------+
| `month(d)`           | month of *d*              | |month-in|                | ``4``                |
+----------------------+---------------------------+---------------------------+----------------------+
| `day(d)`             | day of *d*                | |day-in|                  | ``15``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `hour(t)`            | hours of *t*              | ``hour(time('20:13'))``   | ``20``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `minute(t)`          | minutes of *t*            | ``minute(time('20:13'))`` | ``13``               |
+----------------------+---------------------------+---------------------------+----------------------+
| `second(t)`          | seconds of *t*            | ``second(time('20:13'))`` | ``0.0``              |
+----------------------+---------------------------+---------------------------+----------------------+
| `d + n`              | increment *d* by *n* days | |date-inc-in|             | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `d - n`              | decrement *d* by *n* days | |date-dec-in|             | |date-out|           |
+----------------------+---------------------------+---------------------------+----------------------+
| `d1 - d2`            | number of days between    | |date-diff-in|            | ``13626``            |
|                      | *d1* and *d2*             |                           |                      |
+----------------------+---------------------------+---------------------------+----------------------+

.. |date-out| replace:: ``date('2010-04-15')``
.. |time-out| replace:: ``time('20:13')``
.. |dt-from-untyped-in| replace:: ``datetime('2010-04-15T20:13')``
.. |dt-out| replace:: ``datetime('2010-04-15T20:13')``
.. |dt-from-string-in| replace:: ``datetime( string('2010-04-15T20:13') )``
.. |dt-cr-fn| replace:: `datetime(yyyy,mm,dd [,HH,MM,SS])`
.. |dt-cr-in| replace:: ``datetime(2010,4,15,20,13)``
.. |dt-dt-in| replace:: ``datetime( date('2010-04-15'), time('20:13') )``
.. |date-from-dt-in| replace:: ``date( datetime('2010-04-15T20:13') )``
.. |time-from-dt-in| replace:: ``time( datetime('2010-04-15T20:13') )``
.. |year-in| replace:: ``year(date('2010-04-15'))``
.. |month-in| replace:: ``month(date('2010-04-15'))``
.. |day-in| replace:: ``day(date('2010-04-15'))``
.. |date-inc-in| replace:: ``date('1991-08-20')+6813``
.. |date-dec-in| replace:: ``date('2028-12-09')-6813``
.. |date-diff-in| replace:: ``date('2028-12-09') - date('1991-08-20')``

Date/Time Cast
--------------

`date(x)`
    Convert `x` to a *date* value.
`time(x)`
    Convert `x` to a *time* value.
`datetime(x)`
    Convert `x` to a *datetime* value.

Conversion functions accept untyped literals and string expressions.
An untyped literal must obey the literal format of the respective target
type.  Conversion from a string value is backend-specific.

.. htsql:: /{date('2010-04-15'), time('20:13'),
             datetime('2010-04-15 20:13')}

.. htsql:: /student?dob<date('1982-06-01')
   :cut: 3

Date/Time Construction
----------------------

`date(yyyy,mm,dd)`
    Construct a date from the given year, month and day values.
`datetime(yyyy,mm,dd[,HH,MM,SS])`
    Construct a datetime from the given year, month, day, hour, minute
    and second values.
`datetime(d,t)`
    Construct a datetime from the given date and time.

Construction functions accept and normalize component values outside the
regular range.

.. htsql::

   /{date(2010,4,15), datetime(2010,4,15,20,13),
     datetime(date('2010-04-15'),time('20:13'))}

.. htsql:: /{date(2010,4,15), date(2010,3,46), date(2011,-8,15)}

Component Extraction
--------------------

`date(dt)`
    Date of a *datetime* value.
`time(dt)`
    Time of a *datetime* value.
`year(d)`
    Year of a *date* or a *datetime* value.
`month(d)`
    Month of a *date* or a *datetime* value.
`day(d)`
    Day of a *date* or a *datetime* value.
`hour(t)`
    Hours of a *time* or a *datetime* value.
`minute(t)`
    Minutes of a *time* or a *datetime* value.
`second(t)`
    Seconds of a *time* or a *datetime* value.

The extracted values are integers except for `second()`, where the
extracted value is a float number.

.. htsql::

   /{date($dt), time($dt),
     year($d), month($d), day($d),
     hour($t), minute($t), second($t)}
    :where ($d := date('2010-04-15'),
            $t := time('20:13'),
            $dt := datetime($d,$t))

Date/Time Arithmetics
---------------------

`d + n`
    Increment a *date* or a *datetime* value by `n` days.
`d - n`
    Decrement a *date* or a *datetime* value by `n` days.
`d1 - d2`
    Number of days between two *date* values.

.. htsql:: /{date('1991-08-20')+6813,
             datetime('1991-08-20 02:01')+6813.75833333333}

.. htsql:: /{date('2028-12-09')-6813,
             datetime('2028-12-10 14:25')-6813.75833333333}

.. htsql:: /date('2028-12-09')-date('1991-08-20')

.. htsql:: /student{name, (start_date-dob)/365 :round(1) :as age}
   :cut: 3


Aggregate Functions
===================

+----------------------+---------------------------+---------------------------+
| Function             | Description               | Example Input             |
+======================+===========================+===========================+
| `exists(ps)`         | *TRUE* if *ps* contains   | |exists-in|               |
|                      | at least one *TRUE*       |                           |
|                      | value; *FALSE* otherwise  |                           |
+----------------------+---------------------------+---------------------------+
| `every(ps)`          | *TRUE* if *ps* contains   | |every-in|                |
|                      | only *TRUE* values;       |                           |
|                      | *FALSE* otherwise         |                           |
+----------------------+---------------------------+---------------------------+
| `count(ps)`          | number of *TRUE* values   | |count-in|                |
|                      | in *ps*                   |                           |
+----------------------+---------------------------+---------------------------+
| `min(xs)`            | smallest element in *xs*  | ``min(course.credits)``   |
+----------------------+---------------------------+---------------------------+
| `max(xs)`            | largest element in *xs*   | ``max(course.credits)``   |
+----------------------+---------------------------+---------------------------+
| `sum(xs)`            | sum of elements in *xs*   | ``sum(course.credits)``   |
+----------------------+---------------------------+---------------------------+
| `avg(xs)`            | average value of elements | ``avg(course.credits)``   |
|                      | in *xs*                   |                           |
+----------------------+---------------------------+---------------------------+

.. |exists-in| replace:: ``exists(course.credits>5)``
.. |every-in| replace:: ``every(course.credits>5)``
.. |count-in| replace:: ``count(course.credits>5)``

Aggregate functions accept a plural argument, which, when evaluated,
produces a flow of values, and generates a single *aggregating* value
from it.

Boolean Aggregates
------------------

`exists(xs)`
    Produce *TRUE* if `xs` contains at least one *TRUE* value, *FALSE*
    otherwise.  The aggregate returns *FALSE* on an empty flow.
`every(xs)`
    Produce *FALSE* if `xs` contains only *TRUE* values, *FALSE*
    otherwise.  The aggregate returns *TRUE* on an empty flow.
`count(xs)`
    The number of *TRUE* values in `xs`; ``0`` if `xs` is empty.

Boolean aggregates expect a Boolean argument; a non-Boolean argument
is converted to Boolean first (see function `boolean()`).

.. htsql:: /course?department.code='astro'
   :cut: 3

.. htsql::

   /{exists(astro_course.credits>=5),
     every(astro_course.credits>=5),
     count(astro_course.credits>=5)}
    :where astro_course := course?department.code='astro'

.. htsql:: /course?department.code='pia'

.. htsql::

   /{exists(pia_course.credits>=5),
     every(pia_course.credits>=5),
     count(pia_course.credits>=5)}
    :where pia_course := course?department.code='pia'

Extrema
-------

`min(xs)`
    The smallest value in `xs`.
`max(xs)`
    The largest value in `xs`.

The functions accept numeric, string, enumeration and date/time
arguments.  *NULL* values in the flow are ignored; if the flow is
empty,  *NULL* is returned.

.. htsql::

   /{min(astro_course.credits), max(astro_course.credits)}
    :where astro_course := course?department.code='astro'

.. htsql::

   /{min(pia_course.credits), max(pia_course.credits)}
    :where pia_course := course?department.code='pia'

Sum and Average
---------------

`sum(xs)`
    The sum of values in `xs`; returns ``0`` if `xs` is empty.
`avg(xs)`
    The average of values in `xs`.

The functions accept a numeric argument.  `sum()` returns a
result of the same type as the argument, `avg()` returns
a *decimal* result for an *integer* or a *decimal* argument,
and *float* result for a *float* argument.

.. htsql::

   /{sum(astro_course.credits), avg(astro_course.credits)}
    :where astro_course := course?department.code='astro'

.. htsql::

   /{sum(pia_course.credits), avg(pia_course.credits)}
    :where pia_course := course?department.code='pia'


Flow Operations
===============

+----------------------+---------------------------+---------------------------+
| Function             | Description               | Example Input             |
+======================+===========================+===========================+
| `flow ? p`           | records from *flow*       | ``school?code='edu'``     |
+----------------------+ satisfying condition *p*  +---------------------------+
| `filter(p)`          |                           | |filter-out|              |  
+----------------------+---------------------------+---------------------------+
| `flow ^ x`           | unique values of *x* as   | ``school^campus``         |
+----------------------+ it runs over *flow*       +---------------------------+
| `distinct(flow{x})`  |                           | |distinct-out|            |
+----------------------+---------------------------+---------------------------+
| `flow {x,...}`       | select output columns     | ``school{code,name}``     |
+----------------------+ *x*, ... for *flow*       +---------------------------+
| `select(x,...)`      |                           | |select-out|              |
+----------------------+---------------------------+---------------------------+
| `sort(x,...)`        | reorder records in *flow* | ``course.sort(credits-)`` |
|                      | by *x*, ...               |                           |
+----------------------+---------------------------+---------------------------+
| `limit(n)`           | first *n* records from    | ``course.limit(10)``      |
|                      | *flow*                    |                           |
+----------------------+---------------------------+---------------------------+
| `limit(n,k)`         | *n* records from *flow*   | ``course.limit(10,20)``   |
|                      | starting from *k*-th      |                           |
+----------------------+---------------------------+---------------------------+
| `x -> xs`            | traverse an ad-hoc link   | |link-in|                 |
+----------------------+---------------------------+---------------------------+
| `fork([x])`          | traverse a                | ``course.fork(credits)``  |
|                      | self-referential link     |                           |
+----------------------+---------------------------+---------------------------+

.. |filter-out| replace:: ``school.filter(code='edu')``
.. |distinct-out| replace:: ``distinct(school{campus})``
.. |select-out| replace:: ``school.select(code,name)``
.. |link-in| replace:: ``school.(campus -> school)``

Sieving
-------

`flow ? p`
    Emit records from `flow` that satisfy condition `p`.
`filter(p)`
    Emit records from the input flow that satisfy condition `p`.

The condition is expected to be of Boolean type.  If the argument `p`
is not Boolean, it is implicitly converted to Boolean (see `boolean()`).

.. htsql:: /school?campus='south'

.. htsql:: /school.filter(campus='south')

Projection
----------

`flow ^ x`
    Emit all unique values of `x` as it ranges over `flow`.  *NULL*
    values are ignored.
`flow ^ {x,...}`
    Emit all unique values of the expressions `x,...`.  *NULL* values
    are ignored.
`distinct(flow{x,...})`
    Emit all unique values of the output columns of `flow{x,...}`.
    *NULL* values are ignored.

The projection operation `flow ^ x` creates a new naming scope, which
may contain the following names:

`flow`
    If `flow` is an identifier, then it is used to denote the plural
    link associating each value of `x` with respective records from the
    original flow.  It is called the complement link of the projection.
    The symbol `^` is an alias for a complement link and could be used
    when `flow` is not an identifier and so cannot be used as a name.
`x`
    If `x` is an identifier, then it refers to the value of `x`.
    It is called the kernel of the projection.  When `x` is not an
    identifier, but an arbitrary expression, one may assign it a name
    using in-place selector assignment syntax.

.. htsql:: /school{code, name, campus, count(department)}
   :cut: 3

.. htsql:: /school^campus {campus, count(school)}

.. htsql:: /school^campus {*, count(^)}

.. **

.. htsql:: /distinct(school{campus}) {campus, count(school)}

.. htsql::
   :cut: 3

   /school^{num_dept := count(department)}
    {num_dept, count(school)}

.. htsql::
   :cut: 3

   /school^{campus :if_null '', count(department)}
    {*, count(school)}

.. **

Selection
---------

`{x,...}`
    Define output columns in the input flow.
`flow{x,...}`
    Define output columns in the given flow.
`select(x,...)`
    Define output columns in the input flow.

The selector expression admits two forms of short-cut syntax:

*in-place assignment*
    If an element of a selector is an assignment expression,
    the name defined by the assignment is added to the current scope.
    Only unqualified attribute and reference assignments are allowed.
*sorting decorators*
    If an element of a selector contains a sort order indicators,
    the expression is used to reorder elements in the input flow.

.. htsql:: /{count(school), count(program), count(department)}

.. htsql:: /select(count(school), count(program),
                   count(department))

.. htsql:: /school{code, count(program)}
   :cut: 3

.. htsql:: /school.select(code, count(program))
   :cut: 3

.. htsql:: /school{code, count(program)-}
   :cut: 3

.. htsql:: /school{code, num_prog := count(program)}?num_prog<4
   :cut: 3

.. htsql::
   :cut: 3

   /department{code, $avg_credits := avg(course.credits),
               count(course?credits>$avg_credits)}


Scope Operations
================

+----------------------+---------------------------+---------------------------+
| Function             | Description               | Example Input             |
+======================+===========================+===========================+
| `define(x:=...)`     | add names to the current  | |define-in|               |
|                      | scope                     |                           |
+----------------------+---------------------------+---------------------------+
| `where(expr,x:=...)` | evaluate an expression    | |where-in|                |
|                      | with extra names in the   |                           |
|                      | current scope             |                           |
+----------------------+---------------------------+---------------------------+
| `root()`             | root scope                |                           |
+----------------------+---------------------------+---------------------------+
| `this()`             | current scope             |                           |
+----------------------+---------------------------+---------------------------+

.. |define-in| replace:: ``define(num_prog:=count(program))``
.. |where-in| replace:: ``count(course?credits>$c) :where $c:=avg(course.credits)``

Calculated Attributes
---------------------

`define(x:=...)`
    Add a calculated attribute to the current scope.
`where(expr,x:=...)`
    Evaluate an expression in a current scope with a calculated
    attribute.

These functions add calculated attributes and references to the current
scope.

Scopes
------

`root()`
    The root scope.
`this()`
    The current scope.


Decorators
==========

+----------------------+---------------------------+---------------------------+
| Function             | Description               | Example Input             |
+======================+===========================+===========================+
| `as(x,title)`        | set the column title      | |as-in|                   |
+----------------------+---------------------------+---------------------------+
| `x +`                | indicate ascending order  | ``credits+``              |
+----------------------+---------------------------+---------------------------+
| `x -`                | indicate descending order | ``credits-``              |
+----------------------+---------------------------+---------------------------+

.. |as-in| replace:: ``count(program) :as '# of programs'``

Title
-----

`as(x,title)`
    Specifies the title of the output column.

The title could be either an identifier or a quoted literal.  This
function should be used only when specifying output columns using a
selection operator.

.. htsql:: /school{code :as ID, count(program) :as '# of Programs'}
   :cut: 3

Direction Decorators
--------------------

`x +`
    Specifies ascending direction, *NULL* first.
`x -`
    Specifies descending direction, *NULL* last.

This decorators should be used only on arguments of `sort()` or in a
selection operator.

.. htsql:: /school.sort(campus+)
   :cut: 3


Formatters
==========

+----------------------+---------------------------+
| Function             | Description               |
+======================+===========================+
| `/:html`             | HTML tabular output       |
+----------------------+---------------------------+
| `/:txt`              | plain text tabular output |
+----------------------+---------------------------+
| `/:csv`              | CSV (comma-separated      |
|                      | values) output            |
+----------------------+---------------------------+
| `/:tsv`              | TSV (tab-separated        |
|                      | values) output            |
+----------------------+---------------------------+
| `/:json`             | JSON-serialized output    |
+----------------------+---------------------------+

These functions specify the format of the output data.

.. htsql:: /school/:csv
   :cut: 3


.. vim: set spell spelllang=en textwidth=72:
