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
                         NArySig, ConnectiveSig, PolarSig, CompareSig)


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


class EqualSig(BinarySig, PolarSig):
    pass


class AmongSig(NArySig, PolarSig):
    pass


class TotallyEqualSig(BinarySig, PolarSig):
    pass


class AndSig(BinarySig):
    pass


class OrSig(BinarySig):
    pass


class NotSig(UnarySig):
    pass


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


class IsNullSig(UnarySig):
    pass


class NullIfSig(NArySig):
    pass


class IfNullSig(NArySig):
    pass


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


class QuantifySig(PolarSig):

    slots = [
            Slot('base'),
            Slot('op'),
    ]


class ExistsSig(QuantifySig):

    def __init__(self):
        super(ExistsSig, self).__init__(polarity=+1)


class EverySig(QuantifySig):

    def __init__(self):
        super(EverySig, self).__init__(polarity=-1)


class WrapExistsSig(UnarySig):
    pass


class AggregateSig(Signature):

    slots = [
            Slot('base'),
            Slot('op'),
    ]


class CountSig(AggregateSig):
    pass


class TakeCountSig(UnarySig):
    pass


class MinSig(AggregateSig):
    pass


class TakeMinSig(UnarySig):
    pass


class MaxSig(AggregateSig):
    pass


class TakeMaxSig(UnarySig):
    pass


class SumSig(AggregateSig):
    pass


class TakeSumSig(UnarySig):
    pass


class AvgSig(AggregateSig):
    pass


class TakeAvgSig(UnarySig):
    pass


