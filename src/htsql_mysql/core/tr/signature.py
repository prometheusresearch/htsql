#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.tr.signature import (Signature, Slot,
                                     NullarySig, UnarySig, BinarySig)


class UserVariableSig(NullarySig):

    def __init__(self, name):
        assert isinstance(name, unicode) and len(name) > 0
        self.name = name

    def __basis__(self):
        return (self.name,)


class UserVariableAssignmentSig(BinarySig):
    pass


class IfSig(Signature):

    slots = [
            Slot('condition'),
            Slot('on_true'),
            Slot('on_false'),
    ]


class NoOpConditionSig(UnarySig):
    pass


