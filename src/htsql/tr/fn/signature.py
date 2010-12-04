#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.signature`
============================
"""


from ..signature import (Signature, Slot, NullarySig, UnarySig, BinarySig,
                         NArySig, ConnectiveSig, PolarSig, CompareSig,
                         IsEqualSig, IsTotallyEqualSig, IsInSig, IsNullSig,
                         IfNullSig, NullIfSig, AndSig, OrSig, NotSig)


class ThisSig(Signature):
    pass


class RootSig(Signature):
    pass


class DirectSig(Signature):

    slots = [
            Slot('table'),
    ]


class FiberSig(Signature):

    slots = [
            Slot('table'),
            Slot('image'),
            Slot('counterimage', is_mandatory=False),
    ]


class AsSig(Signature):

    slots = [
            Slot('base'),
            Slot('title'),
    ]


class SortDirectionSig(Signature):

    slots = [
            Slot('base'),
    ]

    def __init__(self, direction):
        assert direction in [+1, -1]
        super(SortDirectionSig, self).__init__(equality_vector=(direction,))
        self.direction = direction


class LimitSig(Signature):

    slots = [
            Slot('limit'),
            Slot('offset', is_mandatory=False),
    ]


class SortSig(Signature):

    slots = [
            Slot('order', is_singular=False),
    ]


class NullSig(Signature):
    pass


class TrueSig(Signature):
    pass


class FalseSig(Signature):
    pass


class CastSig(Signature):

    slots = [
            Slot('base'),
    ]


class DateSig(Signature):

    slots = [
            Slot('year'),
            Slot('month'),
            Slot('day'),
    ]


class AddSig(BinarySig):
    pass


class ConcatenateSig(AddSig):
    pass


class DateIncrementSig(AddSig):
    pass


class SubtractSig(BinarySig):
    pass


class DateDecrementSig(SubtractSig):
    pass


class DateDifferenceSig(SubtractSig):
    pass


class MultiplySig(BinarySig):
    pass


class DivideSig(BinarySig):
    pass


class KeepPolaritySig(UnarySig):
    pass


class ReversePolaritySig(UnarySig):
    pass


class RoundSig(UnarySig):
    pass


class RoundToSig(Signature):

    slots = [
            Slot('op'),
            Slot('precision'),
    ]


class IfSig(Signature):

    slots = [
            Slot('predicates', is_singular=False),
            Slot('consequents', is_singular=False),
            Slot('alternative', is_mandatory=False),
    ]


class SwitchSig(Signature):

    slots = [
            Slot('variable'),
            Slot('variants', is_singular=False),
            Slot('consequents', is_singular=False),
            Slot('alternative', is_mandatory=False),
    ]


class LengthSig(UnarySig):
    pass


class ContainsSig(BinarySig, PolarSig):
    pass


class AggregateSig(Signature):

    slots = [
            Slot('base'),
            Slot('op'),
    ]


class QuantifySig(AggregateSig, PolarSig):
    pass


class ExistsSig(UnarySig):
    pass


class CountSig(UnarySig):
    pass


class MinMaxSig(UnarySig, PolarSig):
    pass


class SumSig(UnarySig):
    pass


class AvgSig(UnarySig):
    pass


