*****************
  Naming Scopes
*****************

.. contents:: Table of Contents
   :depth: 1
   :local:

An *identifier* is a sequence of characters which contains Latin
letters, underscores (``_``), decimal digits and those Unicode
characters which are classified as alphanumeric.  An identifier must not
start with a digit.  In HTSQL, identifiers are used to refer to classes,
attributes, links, functions, constants, predefined expressions, and
other objects.

Each identifier in the input query is *resolved* to determine the object
denoted by it.  How an identifier is resolved depends on the form of the
identifier itself and the position of the identifier in the input query.
In this section, we describe in detail how HTSQL processor resolves
identifiers.


Identifiers
===========

In HTSQL, an identifier could be used in three different forms: plain,
functional and referential.

Plain Identifiers
-----------------

We call an identifier *plain* when it does not appear in a function or
a reference form.  Plain identifiers are used to refer to data model
objects such as classes, attributes and links, and also to global
constants.

.. htsql:: /school{name}?campus==null

This query contains four plain identifiers: ``school``, ``name``,
``campus`` and ``null``, which refer respectively to a class ``school``,
class attributes ``school.name`` and ``school.campus``, and a constant
``null``.

Function Calls
--------------

An identifier is said to be in a *functional* form (or just a
*function*) when it is a part of a function call expression.  HTSQL
supports two notations for function calls: prefix (``F(x,y,...)``) and
infix (``x :F (y,...)``); the choice of calling notation has no effect
on name resolution.

It is convenient to treat unary and binary operators as functions with
one or two arguments.  In HTSQL, operators use the same mechanism for
name resolution as regular functions.

.. htsql:: /school{name, count(department) :as '# of Depts'}?campus='south'/:csv

This query contains:

* ``count()``, ``csv()``: functions with one argument;
* ``as()``: a function with two arguments;
* ``/``: an unary operator (two occurrences);
* ``?``, ``=``: binary operators.

The query uses both prefix and infix call notation.  Rewritten to use
the prefix notation only, the query takes the form:

.. htsql:: /csv(/school{name, as(count(department), '# of Depts')}?campus='north')
   :hide:

References
----------

An identifier appears in a *reference* form if it is preceded by ``$``
symbol.  References are used to pass values between different parts of
the query.  Often, references serve as parameters of predefined
expressions.

.. htsql::
   :cut: 3

   /define($avg_credits := avg(course.credits))
    .course?credits>$avg_credits

Here, reference ``$avg_credits`` denotes the average number of credits
across all courses.

.. htsql::
   :cut: 3

   /department.define(course_with_credits($c) := course?credits=$c)
              {name, count(course_with_credits(2)),
                     count(course_with_credits(3))}

In this query, reference ``$c`` is a parameter of a predefined
expression ``course_with_credits()``.


Scopes
======

A *naming scope* is a mapping of names to associated objects.  HTSQL
distinguishes two types of scopes: global and local.

Global Scope
------------

The *global* scope contains built-in functions, operators and constants.

.. htsql:: /count(school?campus==null)/:csv

In this query, functions ``count()`` and ``csv()``, operators ``?`` and
``=``, and constant ``null`` are found in the global scope.

Local Scope
-----------

In the model graph, each node together with all outgoing arrows forms a
*local* scope.

A unit node induces a *unit scope*.  This scope contains the names of
all classes in the database model.

.. htsql:: /{count(school), count(department)}

In this example, identifiers ``school`` and ``department`` are found in
the unit scope.

A class node induces a *class scope*.  A class scope contains names of
all class attributes and links.

.. htsql:: /school{name, count(department)}?exists(program)
   :cut: 4

In this example, identifiers ``name``, ``department`` and ``program``
are from the scope of class ``school``.

A domain node induces a *domain scope*, which is generally empty because
domain nodes have no outgoing arrows.

This following diagram demonstrates local scopes associated with the
unit node and class node ``school``.

.. diagram:: ../dia/local-scopes.tex
   :align: center

Quotient Scope
--------------

The *quotient class* is a special type of a node in the model graph
formed by the projection operator (``^``).  The quotient class is a
*derived node*, that is, a node which does not come from the original
database model, but is constructed dynamically.

The projection operator has the form ``T ^ x``, where ``T`` is called
the *base* of the projection and ``x`` is called the *kernel* of the
projection.  The quotient class consists of all unique values of ``x``
as it runs over ``T``.

Each quotient class ``T ^ x`` has a natural link back to the base node
``T``; it relates each kernel value to all entities of the base class
that produced this value.  This link is called a *complement* link.
Attributes of the quotient class are values of the kernel expression.

.. diagram:: ../dia/quotient-class.tex
   :align: center

*Quotient scope* is a local scope associated with a quotient class.
HTSQL processor assigns the name of the base class to the complement
link.  In cases when HTSQL is unable to deduce the link name, one may
use a *complement* indicator ``^``.

Thus the following two queries produce identical results.  The first
query uses explicit attribute and link names while the second one uses a
wildcard (``*``) and complement (``^``) indicators to refer to the same
objects:

.. htsql:: /program^degree {degree, count(program)}
   :cut: 3

.. htsql:: /program^degree {*, count(^)}
   :hide:

.. **


Resolution Rules
================

In an HTSQL query, each expression is associated with a collection of
naming scopes, or a *naming context*.  A naming context consists of the
global scope and a stack of local scopes.  When HTSQL processor resolves
identifiers in an expression, it seeks for the matching name and the
corresponding object in the naming context of the expression.

The naming context of the query itself consists of just one local scope:
the unit scope.  Some functions and operators modify the naming scope by
adding a new local scope to the stack or augmenting the top local scope.

Context-Altering Operators
--------------------------

Some operators alter the naming context before evaluating the right
operand.  The following operators evaluate and add the left operand to
the naming context before evaluating the right operand:

* sieve (``T ? p``);
* projection (``T ^ x``);
* selection (``T {x,y,...}``);
* composition (``T . S``).

The following operators adds the unit scope to the naming context before
evaluating the right operand:

* attachment (``x -> T``);
* detachment (``@ T``).

Scope-Augmenting Functions
--------------------------

Functions ``define()`` and ``where()`` allows you to add new names to
the current scope.

Function ``define()`` takes one or more assignment and adds the names
and associated expressions to the top local scope.

.. htsql::
   :cut: 3

   /school.define(num_prog := count(program))
          {name, num_prog}

In this example, we add a calculated attribute ``num_prog`` to the scope
of ``school``.

Function ``define()`` could also be used to add functions and
references to the top local scope:

.. htsql::
   :cut: 3

   /department.define(course_by_credits($c) := course?credits=$c)
              {name, count(course_by_credits(2))}

.. htsql::
   :cut: 3

   /define($avg_credits := avg(course.credits))
    .course?credits>$avg_credits

Function ``where()`` takes an expression as the first parameter, a list
of assignments as subsequent parameters and evaluates the expression in
an augmented scope.  Function ``where()`` is typically used in infix
notation:

.. htsql::
   :cut: 3

   /department{name, count(course?credits>$avg_credits)
                     :where $avg_credits := avg(course.credits)}

Resolving Plain Identifiers
---------------------------

When HTSQL processor translates a plain identifier, it uses the
following rules to find the corresponding object.

1. Search the top local scope for a matching name; done if found.
2. Search the global scope for a matching name; done if found.
3. Otherwise, report an error.

Note that only the top scope in the local scope stack is consulted, the
other scopes are completely shadowed.

.. htsql:: /school{name}?campus==null

The following table summarizes naming contexts used in the query above.

+----------------------+--------------------------------+
| Scope                | Content                        |
+======================+================================+
| *global*             | ``true``, ``false``, `null`    |
+----------------------+--------------------------------+
| *unit*               | `school`, ``department``,      |
|                      | ``program``, ``course``        |
+----------------------+--------------------------------+
| scope of ``school``  | ``code``, `name`, `campus`,    |
|                      | ``program``, ``department``    |
+----------------------+--------------------------------+

The next query shows that attribute ``campus`` from the scope of
``school`` is not available when ``school`` is shadowed by another
scope.

.. htsql:: /school[ns].department{name, campus}
   :error:


Resolving Function Calls
------------------------

Rules for resolving identifiers in functional form mostly coincide with
rules for plain identifiers.  The only difference is that both the name
and the number of arguments must coincide.

Compare the following three queries.  The first and the third queries
match functions ``date()`` with 1 and 3 arguments respectively.  Note
that those are different functions even though they share the same name,
they are distinguished by the number of arguments.

.. htsql:: /date('2010-04-15')

.. htsql:: /date(2010, 4)
   :error:

.. htsql:: /date(2010, 4, 15)

In the example above, function ``date()`` was found in the global scope.
You can use function ``define()`` to add a function to the top local
scope.

.. htsql::
   :cut: 3

   /school.define(num_prog_by_degree($d) := count(program?degree=$d))
          {name, num_prog_by_degree('ba'), num_prog_by_degree('bs')}


Resolving References
--------------------

The following rules are used for resolving references:

1. Search for the matching name in every scope in the stack of local
   scopes; done if found.
2. Otherwise, report an error.

Note that as opposed to plain identifiers, reference lookup uses all
local scopes in the current naming context.

.. htsql::
   :cut: 3

   /department.define($avg_credits := avg(course.credits))
              {name, count(course?credits>$avg_credits)}

In this example, reference ``$avg_credits`` is defined in the scope of
``department``, but used in a nested scope of ``course``.


.. vim: set spell spelllang=en textwidth=72:
