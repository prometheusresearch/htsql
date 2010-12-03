#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.signature`
=========================
"""


from ..util import maybe, listof, Comparable


class Slot(object):

    def __init__(self, name, is_mandatory=True, is_singular=True):
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(is_mandatory, bool)
        assert isinstance(is_singular, bool)
        self.name = name
        self.is_mandatory = is_mandatory
        self.is_singular = is_singular


class Signature(Comparable):

    slots = []

    @classmethod
    def inherit(cls, other):
        assert isinstance(other, Signature)
        return cls()

    def __init__(self, equality_vector=()):
        super(Signature, self).__init__(equality_vector=equality_vector)

    def __iter__(self):
        return iter(self.slots)


class Bag(dict):

    def __init__(self, **keywords):
        self.update(keywords)

    def admits(self, kind, signature):
        assert isinstance(kind, type)
        assert (isinstance(signature, Signature) or
                issubclass(signature, Signature))
        if set(self.keys()) != set(slot.name for slot in signature.slots):
            return False
        for slot in signature.slots:
            value = self[slot.name]
            if slot.is_singular:
                if not isinstance(value, maybe(kind)):
                    return False
                if slot.is_mandatory:
                    if value is None:
                        return False
            else:
                if not isinstance(value, listof(kind)):
                    return False
                if slot.is_mandatory:
                    if not value:
                        return False
        return True

    def cells(self):
        cells = []
        for key in sorted(self.keys()):
            value = self[key]
            if value is not None:
                if isinstance(value, list):
                    cells.extend(value)
                else:
                    cells.append(value)
        return cells

    def impress(self, owner):
        for key in sorted(self.keys()):
            assert not hasattr(owner, key)
            setattr(owner, key, self[key])

    def map(self, method):
        keywords = {}
        for key in sorted(self.keys()):
            value = self[key]
            if value is not None:
                if isinstance(value, list):
                    value = [method(item) for item in value]
                else:
                    value = method(value)
            keywords[key] = value
        return self.__class__(**keywords)

    def freeze(self):
        values = []
        for key in sorted(self.keys()):
            value = self[key]
            if isinstance(value, list):
                value = tuple(value)
            values.append(value)
        return tuple(values)


class NullarySig(Signature):

    slots = []


class UnarySig(Signature):

    slots = [
            Slot('op'),
    ]


class BinarySig(Signature):

    slots = [
            Slot('lop'),
            Slot('rop'),
    ]


class NArySig(Signature):

    slots = [
            Slot('lop'),
            Slot('rops', is_singular=False),
    ]


class ConnectiveSig(Signature):

    slots = [
            Slot('ops', is_singular=False),
    ]


class PolarSig(Signature):

    @classmethod
    def inherit(cls, other):
        assert isinstance(other, PolarSig)
        return cls(polarity=other.polarity)

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        super(PolarSig, self).__init__(equality_vector=(polarity,))
        self.polarity = polarity


class IsEqualSig(BinarySig, PolarSig):
    pass


class IsTotallyEqualSig(BinarySig, PolarSig):
    pass


class IsInSig(NArySig, PolarSig):
    pass


class IsNullSig(UnarySig, PolarSig):
    pass


class IfNullSig(NArySig):
    pass


class NullIfSig(NArySig):
    pass


class CompareSig(BinarySig):

    @classmethod
    def inherit(cls, other):
        assert isinstance(other, CompareSig)
        return cls(relation=other.relation)

    def __init__(self, relation):
        assert relation in ['<', '<=', '>', '>=']
        super(CompareSig, self).__init__(equality_vector=(relation,))
        self.relation = relation


class AndSig(ConnectiveSig):
    pass


class OrSig(ConnectiveSig):
    pass


class NotSig(UnarySig):
    pass


