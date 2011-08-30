*****************
  Naming Scopes
*****************

In HTSQL, identifiers are used to refer to class names, attributes,
links as so on.  A collection of available names and associated objects
is called a naming *scope*.


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
