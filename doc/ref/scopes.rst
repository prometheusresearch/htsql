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

An identifier is said to be in a *function* form when it is a part of
a function call expression.  HTSQL supports two notations for function
calls: prefix (``F(x,y,...)``) and infix (``x :F (y,...)``).  The choice
of calling notation has no effect on name resolution.

It is convenient to treat unary and binary operators as functions with
one or two arguments.  In HTSQL, operators use the same mechanism for
name resolution as regular functions.

.. htsql:: /school{name, count(department) :as '# of Depts'}?campus='south'/:csv

This query contains two functions with one argument: ``count()`` and
``csv()``, a function with two arguments: ``as()``, and two binary
operators: ``?`` and ``=``.  This query could be rewritten to use only
prefix function calls:

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

This query uses a reference ``$avg_credits`` to produce courses with the
number of credits larger than the average.

.. htsql::
   :cut: 3

   /department.define(course_with_credits($c) := course?credits=$c)
              {name, count(course_with_credits(2)),
                     count(course_with_credits(3))}

This query uses reference ``$c`` as a parameter of a predefined
expression ``course_with_credits()``.


Scopes
======

A *naming scope* is a mapping of names to associated objects.  Each
position in an HTSQL query is associated with a *naming context*, or a
collection of naming scopes.  HTSQL resolves an identifier by seeking
through available scopes in the current context to find a name that
matches the identifier and fetches the object associated with this name.

HTSQL distinguishes two types of scopes: global and local.  The global
scopes is shared by every 

Each naming context contains a global scope, and a sequence of local
scopes.


Global Scope
------------

The *global* scope contains built-in functions, operators and constants.

.. htsql:: /count(school?campus==null)/:csv

In this query, functions ``count()`` and ``csv()``, operators ``?`` and
``=``, and constant ``null`` are found in the global scope.

Local Scope
-----------

Each node in the model graph together with all outgoing arrows forms a
*local* scope.  A unit node induces a *unit* scope, which contains all
the classes in the model.  A class node induces a *class* scope with
associated attributes and links.  A domain node induces a *domain*
scope, which is, in general, empty because domain nodes have no outgoing
arrows.

.. htsql:: /department{name}?school.campus='old'
   :cut: 3

In this query, ``department`` is a class name which found in the unit scope,
``name`` and ``school`` are respectively an attribute and a link in the
scope of class ``department``, and ``campus`` is an attribute in the
scope of class ``school``.


----

Each expression in an HTSQL query has an
associated sequence of nested scopes


Resolution
==========

Let's deconstruct name resolution in the following example:

.. htsql:: /school.filter(code='eng').department{name}

`/`
    The query starts in a unit scope, which contains all the classes in
    the model.
`school.filter(code='eng')`
     By choosing ``school``, we changed the scope to ``school`` class.
     This scope contains all attributes of school entities (``code``,
     ``name``, ``campus``) and outgoing links (``program``,
     ``department``).



Resolving Plain Identifiers
---------------------------

Resolving Function Calls
------------------------

Resolving References
--------------------


Scope and Syntax
================

Some binary operators evaluate the right operand in the scope of the
left operand.  These operators include:

* sieve (``T ? p``);
* projection (``T ^ x``);
* selection (``T {x,y,...}``);
* composition (``T . S``).

Some operators evaluate their operand in the unit scope:

* attachment (``x -> T``);
* detachment (``@ T``).


----

Root Scope
==========

The root scope is the top level scope in the scope stack -- it is the
scope where the query is evaluated.  This scope contains the names of
all classes (tables) in the database.

.. htsql:: /{count(school), count(department)}

In this example, identifiers ``school`` and ``department`` belong to the
root scope and are associated with the respective classes.


Class Scope
===========

The class scope is associated with some class (table) of the database.
The scope contains names of all class attributes and links to other
classes.

.. htsql:: /school{code, count(department)}?exists(program)
   :cut: 4

In this example, ``school`` belongs to the root scope while identifiers
``code``, ``department`` and ``program`` belong to the scope of `school`
class.  ``school.code`` is the attribute of `school`,
``school.department`` and ``school.program`` are links to the respective
classes.


Projection Scope
================

The projection scope is associated with a projection expression.

Projection is an example of a derived class: its records are composed
from unique values of the kernel as it runs over the base class.  A
projection class has a natural link back to the base class: it relates
the value of the kernel to every record of the base class that produced
this value.

(diagram)

This link is called a *complement* link.  HTSQL assigns the name for the
link that coincides with the name of the base class.  In cases when
HTSQL is unable to deduce a link name, one may use a special
*complement* expression: `^`.

Attributes of the projection class are values of the kernel expression.
When possible, HTSQL automatically assigns names for attributes,
otherwise, the user may define custom attribute names.

.. htsql:: /(school^campus){campus, count(school)}

In this example, the projection scope ``(school^campus)`` has two names:
the attribute name ``campus`` and the kernel link ``school``.

.. htsql::

    /(school^{num_dept := count(department)})
        {num_dept, count(school)}

In this example, we assign the name ``num_dept`` to the projection
attribute.


Modifying Scope
===============

HTSQL allows adding new attributes to an existing scope, see
functions ``define()`` and ``where()``.


References
==========

Traversing a link changes the scope; any names defined in the previous
scope are no longer available.  To pass values between different scopes,
use references.


.. vim: set spell spelllang=en textwidth=72:
