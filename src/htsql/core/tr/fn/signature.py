#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.fn.signature`
=================================
"""


from ..signature import (Signature, Slot, NullarySig, UnarySig, BinarySig,
                         PolarSig, ConnectiveSig, NArySig)


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


class LimitSig(Signature):

    slots = [
            Slot('limit'),
            Slot('offset', is_mandatory=False),
    ]


class SortSig(Signature):

    slots = [
            Slot('order', is_singular=False),
    ]


class SelectSig(Signature):

    slots = [
            Slot('ops', is_mandatory=False, is_singular=False),
    ]


class LinkSig(Signature):

    slots = [
            Slot('seed'),
    ]


class TopSig(Signature):

    slots = [
            Slot('seed'),
            Slot('limit', is_mandatory=False),
            Slot('offset', is_mandatory=False),
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


class MakeDateTimeSig(Signature):

    slots = [
            Slot('year'),
            Slot('month'),
            Slot('day'),
            Slot('hour', is_mandatory=False),
            Slot('minute', is_mandatory=False),
            Slot('second', is_mandatory=False),
    ]


class CombineDateTimeSig(Signature):

    slots = [
            Slot('date'),
            Slot('time'),
    ]


class ExtractSig(UnarySig):
    pass


class ExtractYearSig(ExtractSig):
    pass


class ExtractMonthSig(ExtractSig):
    pass


class ExtractDaySig(ExtractSig):
    pass


class ExtractHourSig(ExtractSig):
    pass


class ExtractMinuteSig(ExtractSig):
    pass


class ExtractSecondSig(ExtractSig):
    pass


class AddSig(BinarySig):
    pass


class ConcatenateSig(AddSig):
    pass


class DateIncrementSig(AddSig):
    pass


class DateTimeIncrementSig(AddSig):
    pass


class SubtractSig(BinarySig):
    pass


class DateDecrementSig(SubtractSig):
    pass


class DateTimeDecrementSig(SubtractSig):
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


class TruncSig(UnarySig):
    pass


class TruncToSig(Signature):

    slots = [
            Slot('op'),
            Slot('precision'),
    ]


class SquareRootSig(UnarySig):
    pass


class GuardSig(Signature):

    slots = [
            Slot('reference'),
            Slot('consequent'),
            Slot('alternative', is_mandatory=False),
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


class HasPrefixSig(BinarySig):
    pass


class LikeSig(BinarySig):

    def __init__(self, polarity, is_case_sensitive=False):
        assert polarity in [+1, -1]
        assert isinstance(is_case_sensitive, bool)
        self.polarity = polarity
        self.is_case_sensitive = is_case_sensitive

    def __basis__(self):
        return (self.polarity, self.is_case_sensitive)

    def reverse(self):
        return self.clone(polarity=-self.polarity,
                          is_case_sensitive=is_case_sensitive)

    def __str__(self):
        return "%s(%s)" % (self.__class__.__name__,
                           '+' if self.polarity > 0 else '-')


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
        self.is_left = is_left
        self.is_right = is_right

    def __basis__(self):
        return (self.is_left, self.is_right)


class TodaySig(NullarySig):
    pass


class NowSig(NullarySig):
    pass


class AggregateSig(Signature):

    slots = [
            Slot('plural_base', is_mandatory=False),
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


class AssignmentSig(BinarySig):
    pass


class DefineSig(ConnectiveSig):
    pass


class GivenSig(NArySig):
    pass


