#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import named, adapts
from htsql.domain import IntegerDomain
from htsql.tr.fn.signature import AddSig, SubtractSig
from htsql.tr.fn.bind import (BindCast, ComparableDomains, CorrelateFunction,
                              correlates)
from .domain import INetDomain
from .signature import INetIncrementSig, INetDecrementSig, INetDifferenceSig


class BindINetCast(BindCast):

    named('inet')
    codomain = INetDomain()


class ComparableINet(ComparableDomains):

    adapts(INetDomain)


class CorrelateINetIncrement(CorrelateFunction):

    correlates(AddSig, (INetDomain, IntegerDomain))
    signature = INetIncrementSig
    domains = [INetDomain(), IntegerDomain()]
    codomain = INetDomain()


class CorrelateINetDecrement(CorrelateFunction):

    correlates(SubtractSig, (INetDomain, IntegerDomain))
    signature = INetDecrementSig
    domains = [INetDomain(), IntegerDomain()]
    codomain = INetDomain()


class CorrelateINetDifference(CorrelateFunction):

    correlates(SubtractSig, (INetDomain, INetDomain))
    signature = INetDifferenceSig
    domains = [INetDomain(), INetDomain()]
    codomain = IntegerDomain()


