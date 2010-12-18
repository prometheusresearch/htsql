==============================
  HTSQL 2.0 Function Summary
==============================

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
================

By convention, string functions take a string as its first parameter.
When an untyped literal, such as ``'value'`` is used and a string is
expected, it is automatically cast.  Hence, for convenience, we write
string typed values using single quotes in the output column.

+----------------+------------------+----------------------+---------------+
| Function       | Description      | Example Input        | Output        |
+================+==================+======================+===============+
| string(x)      | cast to string   | string('Hello')      | 'Hello'       |
|                |                  +----------------------+---------------+
|                |                  | string(1.0)          | '1.0'         |
|                +------------------+----------------------+---------------+
|                | cast to string   | 'Hello' :string()    | 'Hello'       |
|                | using postfix    +----------------------+---------------+
|                | call notation    | 'Hello' :string      | 'Hello'       |
|                |                  +----------------------+---------------+
|                |                  | ``null()`` :string   | ``null()``    |
|                |                  +----------------------+---------------+
|                |                  | ``true()`` :string   | 'true'        |
|                |                  +----------------------+---------------+
|                |                  | ``false()`` :string  | 'false'       |
+----------------+------------------+----------------------+---------------+
| x + y          | concatenation    | 'Hello' + ' World'   | 'Hello World' |
|                | (treats nulls as +----------------------+---------------+
|                |  empty strings)  | 'Hello' + ``null()`` | 'Hello'       |
+----------------+------------------+----------------------+---------------+
| x ~ y          | case-insensitive | 'HTSQL' ~ 'sql'      | ``true()``    |
|                | string contains  |                      |               |
+----------------+------------------+----------------------+---------------+
| x !~ y         | case-insensitive | 'HTSQL' !~ 'sql'     | ``false()``   |
|                | not contains     |                      |               |
+----------------+------------------+----------------------+---------------+
| head(s,n=1)    | returns first n  | head('HTSQL', 2)     | 'HT'          |
|                | characters of s  +----------------------+---------------+
|                |                  | head('HTSQL')        | 'H'           |
+----------------+------------------+----------------------+---------------+
| tail(s,n=1)    | returns last n   | tail('HTSQL', 3)     | 'SQL'         |
|                | characters of s  +----------------------+---------------+
|                |                  | tail('HTSQL')        | 'L'           |
+----------------+------------------+----------------------+---------------+
| slice(s,i,     | chars i to j     | slice('HTSQL',2,5)   | 'SQL'         |
|       j=-1)    | characters of s  +----------------------+---------------+
|                |                  | slice('HTSQL',2)     | 'SQL'         |
|                |                  +----------------------+---------------+
|                |                  | slice('HTSQL',-3)    | 'SQL'         |
+----------------+------------------+----------------------+---------------+
| at(s,k,n=1)    | return n chars   | at('HTSQL',2)        | 'S'           |
|                | starting with k  +----------------------+---------------+
|                |                  | at('HTSQL,2,3)       | 'SQL'         |
|                |                  +----------------------+---------------+
|                |                  | at('HTSQL,-3,3)      | 'SQL'         |
+----------------+------------------+----------------------+---------------+
| upper(s)       | returns upper    | upper('htsql')       | 'HTSQL'       |
|                | case of s        +----------------------+---------------+
|                |                  | 'htsql' :upper()     | 'HTSQL'       |
+----------------+------------------+----------------------+---------------+
| lower(s)       | returns lower    | lower('HTSQL')       | 'htsql'       |
|                | case of s        +----------------------+---------------+
|                |                  | 'HTSQL' :lower()     | 'htsql'       |
+----------------+------------------+----------------------+---------------+
| replace(s,x,y) | replaces all     | replace('HTSQL',     | 'HTRAF'       |
|                | occurrences of x |         'SQL','RAF') |               |
|                | in s with y      |                      |               |
+----------------+------------------+----------------------+---------------+
| ltrim(s)       | strips leading   | ltrim('  HTSQL  ')   | 'HTSQL  '     |
|                | spaces from s    |                      |               |
+----------------+------------------+----------------------+---------------+
| rtrim(s)       | strips trailing  | rtrim('  HTSQL  ')   | '  HTSQL'     |
|                | spaces from s    |                      |               |
+----------------+------------------+----------------------+---------------+
| trim(s)        | strips leading   | trim('  HTSQL  ')    | 'HTSQL'       |
|                | and trailing     +----------------------+---------------+
|                | spaces from s    | 'HTSQL' :trim()      | 'HTSQL'       |
+----------------+------------------+----------------------+---------------+

