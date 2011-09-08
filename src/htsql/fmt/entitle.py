#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.fmt.entitle`
========================

This module implements the entitle adapter.
"""


def entitle(binding, with_strong=True, with_weak=True):
    headers = guess_title(binding)
    if headers:
        return headers[-1]
    else:
        return ""


def guess_title(binding):
    from ..tr.lookup import guess_title
    return guess_title(binding)


