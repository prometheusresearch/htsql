#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr`
===============

.. epigraph::

    Those are the hills of Hell, my love,
    Where you and I must go

    -- (Traditional)

This package implements the HTSQL-to-SQL translator.
"""


# Make sure all submodules in the `tr` package are imported
# so that any adapter components defined there are registered.
# Since `fn` is not explicitly imported anywhere, we force
# its import here.
from . import fn


