#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.tr.fn.signature import AddSig, SubtractSig


class INetIncrementSig(AddSig):
    pass


class INetDecrementSig(SubtractSig):
    pass


class INetDifferenceSig(SubtractSig):
    pass


