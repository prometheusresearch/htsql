#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.assemble`
===========================
"""


from ...adapter import adapts
from ..assemble import EvaluateBySignature
from .signature import ConcatenateSig, WrapExistsSig


class EvaluateWrapExists(EvaluateBySignature):

    adapts(WrapExistsSig)
    is_null_regular = False
    is_nullable = False


class EvaluateConcatenate(EvaluateBySignature):

    adapts(ConcatenateSig)
    is_null_regular = False
    is_nullable = False


