#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.signature`
============================
"""


from ..signature import (Signature, Slot, NullarySig, UnarySig, BinarySig,
                         PolarSig, ConnectiveSig)


class FiberSig(Signature):

    slots = [
            Slot('table'),
            Slot('image', is_mandatory=False),
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


class CastSig(Signature):

    slots = [
            Slot('base'),
    ]


class MakeDateSig(Signature):

    slots = [
            Slot('year'),
            Slot('month'),
            Slot('day'),
    ]


class ExtractSig(UnarySig):
    pass


class ExtractYearSig(ExtractSig):
    pass


class ExtractMonthSig(ExtractSig):
    pass


class ExtractDaySig(ExtractSig):
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


class LikeSig(BinarySig, PolarSig):
    pass


class ReplaceSig(Signature):

    slots = [
            Slot('op'),
            Slot('old'),
            Slot('new'),
    ]


class SubstringSig(Signature):

    slots = [
            Slot('op'),
            Slot('start'),
            Slot('length', is_mandatory=False),
    ]


class HeadSig(Signature):

    slots = [
            Slot('op'),
            Slot('length', is_mandatory=False),
    ]


class TailSig(Signature):

    slots = [
            Slot('op'),
            Slot('length', is_mandatory=False),
    ]


class SliceSig(Signature):

    slots = [
            Slot('op'),
            Slot('left', is_mandatory=False),
            Slot('right', is_mandatory=False),
    ]


class AtSig(Signature):

    slots = [
            Slot('op'),
            Slot('index'),
            Slot('length', is_mandatory=False),
    ]


class UpperSig(UnarySig):
    pass


class LowerSig(UnarySig):
    pass


class TrimSig(UnarySig):

    def __init__(self, is_left=True, is_right=True):
        assert isinstance(is_left, bool)
        assert isinstance(is_right, bool)
        assert is_left or is_right
        super(TrimSig, self).__init__(equality_vector=(is_left, is_right))
        self.is_left = is_left
        self.is_right = is_right


class TodaySig(NullarySig):
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


class QuotientSig(Signature):

    slots = [
            Slot('seed'),
            Slot('kernel', is_singular=False),
    ]


class KernelSig(NullarySig):

    slots = [
            Slot('index', is_mandatory=False),
    ]


class ComplementSig(NullarySig):
    pass


class AssignmentSig(BinarySig):
    pass


class DefineSig(ConnectiveSig):
    pass


