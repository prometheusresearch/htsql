#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr`
====================

.. epigraph::

    Those are the hills of Hell, my love,
    Where you and I must go

    -- (Traditional)

This package implements the HTSQL-to-SQL translator.
"""


from . import (assemble, binding, bind, coerce, compile, dump, encode, space,
        fn, frame, lookup, plan, reduce, rewrite, signature, stitch, term,
        translate)


