#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.adapter import call, adapt
from ...core.domain import IntegerDomain
from ...core.tr.fn.signature import AddSig, SubtractSig
from ...core.tr.fn.bind import (BindCast, ComparableDomains, CorrelateFunction,
                                match)
from .domain import INetDomain
from .signature import INetIncrementSig, INetDecrementSig, INetDifferenceSig


class BindINetCast(BindCast):

    call('inet')
    codomain = INetDomain()


class ComparableINet(ComparableDomains):

    adapt(INetDomain)


class CorrelateINetIncrement(CorrelateFunction):

    match(AddSig, (INetDomain, IntegerDomain))
    signature = INetIncrementSig
    domains = [INetDomain(), IntegerDomain()]
    codomain = INetDomain()


class CorrelateINetDecrement(CorrelateFunction):

    match(SubtractSig, (INetDomain, IntegerDomain))
    signature = INetDecrementSig
    domains = [INetDomain(), IntegerDomain()]
    codomain = INetDomain()


class CorrelateINetDifference(CorrelateFunction):

    match(SubtractSig, (INetDomain, INetDomain))
    signature = INetDifferenceSig
    domains = [INetDomain(), INetDomain()]
    codomain = IntegerDomain()


