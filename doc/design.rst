********************
  Design Rationale
********************

.. epigraph::

    From this point, I want to begin the programmer's training
    as a full-fledged navigator in an *n*-dimensional data space.

    -- Charles W. Bachman, The programmer as navigator


HTSQL is a high-level query language with a SQL backend.
In this document, we bring out motivation and design 
principles behind syntax and semantics of HTSQL.

Design Motivation
=================

HTSQL originated in 2005 with a vision -- data analysts should have
meaningful access to the information in their relational database.  The
technical barrier of providing direct database access via a HTTP gateway
turned out to be not so interesting.  The challenge lies primarily in the
design of a URI based query language that balances ease of use with
significant query power.  

Query Cognition
---------------

The motivating design principle for HTSQL is to separate *row
definition* from both *column selection* and *set filtering*. When
describing a business inquiry, a data analyst engages in three distinct
cognitive activities.  The first is row definition: specifying what each
row in the returned result set represents.  The second is column
selection: choosing which data elements should be included.  The third
is set filtering: providing criteria for which rows should be included.

Previous approaches confound these three separable cognitive activities,
much to the detriment of learn-ability, accuracy, and communication time.
In SQL, it would appear that the ``SELECT`` clause corresponds to column
selection, the ``WHERE`` clause corresponds to filtering, and the
``FROM`` clause corresponds to row definition.  However, for anything
other than trivial queries, this isn't true.

For example, the HTSQL query below defines rows as departments; and for
each department, selects its name and the corresponding school's name:

.. sourcecode:: htsql

    /department{name, school.name}

In the classic textbook SQL equivalent would be:

.. sourcecode:: sql

    SELECT s.name, d.name 
    FROM ad.department AS d 
      LEFT JOIN ad.school AS s
      ON (d.school = s.code)

In this example, the ``FROM`` clause no longer expresses set row
definition -- it is conflated with the ``JOIN`` to the school table,
for column selection.  

