#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import maybe, listof, tupleof, Clonable, Printable, Hashable
from ..entity import TableEntity, ColumnEntity, Join
from ..domain import (Domain, VoidDomain, BooleanDomain, ListDomain,
        RecordDomain, EntityDomain, IdentityDomain, Profile)
from ..error import point
from .binding import Binding, VoidBinding
from .signature import Signature, Bag, Formula


class Flow(Clonable):

    def __init__(self, base, domain, binding):
        assert base is not None or isinstance(self, (RootFlow, VoidFlow))
        assert isinstance(domain, Domain)
        assert isinstance(binding, Binding)

        self.base = base
        self.domain = domain
        self.binding = binding
        point(self, binding)


class VoidFlow(Flow):

    def __init__(self):
        super(VoidFlow, self).__init__(None, VoidDomain(), VoidBinding())


class ScopeFlow(Flow):
    pass


class HomeFlow(ScopeFlow):

    def __init__(self, base, binding):
        super(HomeFlow, self).__init__(base, EntityDomain(), binding)


class RootFlow(HomeFlow):

    def __init__(self, binding):
        super(RootFlow, self).__init__(None, binding)


class TableFlow(ScopeFlow):

    def __init__(self, base, table, binding):
        assert isinstance(table, TableEntity)
        super(TableFlow, self).__init__(base, EntityDomain(), binding)
        self.table = table


class ChainFlow(TableFlow):

    def __init__(self, base, joins, binding):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        super(ChainFlow, self).__init__(base, joins[-1].target, binding)
        self.joins = joins


class ColumnFlow(ScopeFlow):

    def __init__(self, base, column, link, binding):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(Flow))
        super(ColumnFlow, self).__init__(base, column.domain, binding)
        self.column = column
        self.link = link


class QuotientFlow(ScopeFlow):

    def __init__(self, base, seed, kernels, binding):
        super(QuotientFlow, self).__init__(base, EntityDomain(), binding)
        self.seed = seed
        self.kernels = kernels


class CoverFlow(ScopeFlow):

    def __init__(self, base, seed, binding):
        super(CoverFlow, self).__init__(base, seed.domain, binding)
        self.seed = seed


class KernelFlow(CoverFlow):

    def __init__(self, base, quotient, index, binding):
        assert isinstance(quotient, QuotientFlow)
        assert isinstance(index, int)
        assert 0 <= index < len(quotient.kernels)
        seed = quotient.kernels[index]
        super(KernelFlow, self).__init__(base, seed, binding)
        self.quotient = quotient
        self.index = index


class ComplementFlow(CoverFlow):

    def __init__(self, base, quotient, binding):
        assert isinstance(quotient, QuotientFlow)
        super(ComplementFlow, self).__init__(base, quotient.seed, binding)
        self.quotient = quotient


class ForkFlow(CoverFlow):

    def __init__(self, base, kernels, binding):
        assert isinstance(kernels, listof(Flow))
        super(ForkFlow, self).__init__(base, base, binding)
        self.kernels = kernels


class AttachFlow(CoverFlow):

    def __init__(self, base, seed, images, condition, binding):
        assert isinstance(images, listof(tupleof(Flow, Flow)))
        assert isinstance(condition, maybe(Flow))
        if condition is not None:
            assert isinstance(condition.domain, BooleanDomain)
        super(AttachFlow, self).__init__(base, seed, binding)
        self.images = images
        self.condition = condition


class LocateFlow(AttachFlow):
    pass


class ClipFlow(CoverFlow):

    def __init__(self, base, seed, order, limit, offset, binding):
        assert isinstance(seed, Flow)
        assert isinstance(order, listof(tupleof(Flow, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(ClipFlow, self).__init__(base, seed, binding)
        self.order = order
        self.limit = limit
        self.offset = offset


class QueryFlow(Flow):

    def __init__(self, base, segment, profile, binding):
        assert isinstance(base, RootFlow)
        assert isinstance(segment, maybe(SegmentFlow))
        assert isinstance(profile, Profile)
        super(QueryFlow, self).__init__(base, VoidDomain(), binding)
        self.segment = segment
        self.profile = profile


class SegmentFlow(Flow):

    def __init__(self, base, seed, domain, binding):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        super(SegmentFlow, self).__init__(base, domain, binding)
        self.seed = seed


class SieveFlow(ScopeFlow):

    def __init__(self, base, filter, binding):
        assert isinstance(filter, Flow)
        assert isinstance(filter.domain, BooleanDomain)
        super(SieveFlow, self).__init__(base, base.domain, binding)
        self.filter = filter


class SortFlow(ScopeFlow):

    def __init__(self, base, order, limit, offset, binding):
        assert isinstance(order, listof(tupleof(Flow, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(SortFlow, self).__init__(base, base.domain, binding)
        self.order = order
        self.limit = limit
        self.offset = offset


class RescopingFlow(ScopeFlow):

    def __init__(self, base, scope, binding):
        assert isinstance(scope, Flow)
        super(RescopingFlow, self).__init__(base, base.domain, binding)
        self.scope = scope


class SelectionFlow(ScopeFlow):

    def __init__(self, base, elements, domain, binding):
        assert isinstance(elements, listof(Flow))
        super(SelectionFlow, self).__init__(base, domain, binding)
        self.elements = elements


class IdentityFlow(Flow):

    def __init__(self, base, elements, binding):
        assert isinstance(elements, listof(Flow))
        domain = IdentityDomain([element.domain for element in elements])
        super(IdentityFlow, self).__init__(base, domain, binding)
        self.elements = elements
        self.width = domain.width


class LiteralFlow(Flow):

    def __init__(self, base, value, domain, binding):
        super(LiteralFlow, self).__init__(base, domain, binding)
        self.value = value


class CastFlow(Flow):

    def __init__(self, base, domain, binding):
        super(CastFlow, self).__init__(base, domain, binding)


class ImplicitCastFlow(CastFlow):
    pass


class FormulaFlow(Formula, Flow):

    def __init__(self, base, signature, domain, binding, **arguments):
        assert isinstance(signature, Signature)
        arguments = Bag(**arguments)
        assert arguments.admits(Flow, signature)
        super(FormulaFlow, self).__init__(signature, arguments,
                                          base, domain, binding)


