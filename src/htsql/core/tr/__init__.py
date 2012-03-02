#
# Copyright (c) 2006-2012, Prometheus Research, LLC
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


from . import (assemble, binding, bind, coerce, compile, dump, encode, embed,
               error, flow, fn, frame, lookup, parse, plan, reduce, rewrite,
               scan, signature, stitch, syntax, term, token)


