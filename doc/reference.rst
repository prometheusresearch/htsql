====================
  HTSQL Reference
====================

Grammar
=======

This is the HTSQL grammar::

        query        ::= '/' segment? format?
        segment      ::= selector | specifier selector? filter?
        filter       ::= '?' test
        format       ::= '/' ':' identifier

        test         ::= test direction | test application | or_test
        direction    ::= ( '+' | '-' )
        application  ::= ':' identifier ( or_test | call )?
        or_test      ::= and_test ( '|' and_test )*
        and_test     ::= implies_test ( '&' implies_test )*
        implies_test ::= unary_test ( '->' unary_test )?
        unary_test   ::= '!' unary_test | comparison

        comparison   ::= expression ( ( '=~~' | '=~' | '^~~' | '^~' |
                                        '$~~' | '$~' | '~~' | '~' |
                                        '!=~~' | '!=~' | '!^~~' | '!^~' |
                                        '!$~~' | '!$~' | '!~~' | '!~' |
                                        '<=' | '<' | '>=' |  '>' |
                                        '==' | '=' | '!==' | '!=' )
                                      expression )?

        expression   ::= term ( ( '+' | '-' ) term )*
        term         ::= factor ( ( '*' | '/' ) factor )*
        factor       ::= ( '+' | '-' ) factor | power
        power        ::= sieve ( '^' power )?

        sieve        ::= specifier selector? filter?
        specifier    ::= atom ( '.' identifier call? )* ( '.' '*' )?
        atom         ::= '*' | selector | group | identifier call? | literal

        group        ::= '(' test ')'
        call         ::= '(' tests? ')'
        selector     ::= '{' tests? '}'
        tests        ::= test ( ',' test )* ','?

        identifier   ::= NAME
        literal      ::= STRING | NUMBER


Function Summary
================

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


String Functions
----------------

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

