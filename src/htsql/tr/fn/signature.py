#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.fn.signature`
============================
"""


from ..signature import Signature, Parameter


class ThisSig(Signature):
    pass


class RootSig(Signature):
    pass


class DirectSig(Signature):

    parameters = [
            Parameter('table'),
    ]


class FiberSig(Signature):

    parameters = [
            Parameter('table'),
            Parameter('image'),
            Parameter('counterimage', is_mandatory=False),
    ]


class AsSig(Signature):

    parameters = [
            Parameter('base'),
            Parameter('title'),
    ]


class SortDirectionSig(Signature):

    parameters = [
            Parameter('base'),
    ]

    def __init__(self, direction):
        assert direction in [+1, -1]
        self.direction = direction


class LimitSig(Signature):

    parameters = [
            Parameter('limit'),
            Parameter('offset', is_mandatory=False),
    ]


class SortSig(Signature):

    parameters = [
            Parameter('order', is_list=True),
    ]


class NullSig(Signature):
    pass


class TrueSig(Signature):
    pass


class FalseSig(Signature):
    pass


class CastSig(Signature):

    parameters = [
            Parameter('base'),
    ]


class DateSig(Signature):

    parameters = [
            Parameter('year'),
            Parameter('month'),
            Parameter('day'),
    ]


class UnarySig(Signature):

    parameters = [
            Parameter('op'),
    ]


class BinarySig(Signature):

    parameters = [
            Parameter('lop'),
            Parameter('rop'),
    ]


class NArySig(Signature):

    parameters = [
            Parameter('lop'),
            Parameter('rops', is_list=True),
    ]


class EqualSig(BinarySig):

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity


class AmongSig(NArySig):

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity


class TotallyEqualSig(BinarySig):

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity


class AndSig(BinarySig):
    pass


class OrSig(BinarySig):
    pass


class NotSig(UnarySig):
    pass


class CompareSig(BinarySig):

    def __init__(self, relation):
        assert relation in ['<', '<=', '>', '>=']
        self.relation = relation


class AddSig(BinarySig):
    pass


class NumericAddSig(AddSig):
    pass


class ConcatenateSig(AddSig):
    pass


class DateIncrementSig(AddSig):
    pass


class SubtractSig(BinarySig):
    pass


class NumericSubtractSig(SubtractSig):
    pass


class DateDecrementSig(SubtractSig):
    pass


class DateDifferenceSig(SubtractSig):
    pass


class MultiplySig(BinarySig):
    pass


class NumericMultiplySig(MultiplySig):
    pass


class DivideSig(BinarySig):
    pass


class NumericDivideSig(DivideSig):
    pass


class KeepPolaritySig(UnarySig):
    pass


class ReversePolaritySig(UnarySig):
    pass


class NumericKeepPolaritySig(KeepPolaritySig):
    pass


class NumericReversePolaritySig(ReversePolaritySig):
    pass


class RoundSig(UnarySig):
    pass


class RoundToSig(Signature):

    parameters = [
            Parameter('op'),
            Parameter('precision'),
    ]


class IsNullSig(UnarySig):
    pass


class NullIfSig(NArySig):
    pass


class IfNullSig(NArySig):
    pass


class IfSig(Signature):

    parameters = [
            Parameter('predicates', is_list=True),
            Parameter('consequents', is_list=True),
            Parameter('alternative', is_mandatory=False),
    ]


class SwitchSig(Signature):

    parameters = [
            Parameter('variable'),
            Parameter('variants', is_list=True),
            Parameter('consequents', is_list=True),
            Parameter('alternative', is_mandatory=False),
    ]


class LengthSig(UnarySig):
    pass


class StringLengthSig(LengthSig):
    pass


class ContainsSig(BinarySig):

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity


class StringContainsSig(ContainsSig):
    pass


class QuantifySig(Signature):

    parameters = [
            Parameter('base'),
            Parameter('op'),
    ]

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity


class ExistsSig(QuantifySig):

    def __init__(self):
        super(ExistsSig, self).__init__(+1)


class EverySig(QuantifySig):

    def __init__(self):
        super(EverySig, self).__init__(-1)


class WrapExistsSig(UnarySig):
    pass


class AggregateSig(Signature):

    parameters = [
            Parameter('base'),
            Parameter('op'),
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


