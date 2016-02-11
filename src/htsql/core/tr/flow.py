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


class Flow(Hashable, Clonable):

    def __init__(self, base, domain, binding):
        assert base is not None or isinstance(self, (RootFlow, VoidFlow))
        assert isinstance(domain, Domain)
        assert isinstance(binding, Binding)

        self.base = base
        self.domain = domain
        self.binding = binding
        point(self, binding)

    def __basis__(self):
        return (self.base, self.domain)


class VoidFlow(Flow):

    def __init__(self):
        super(VoidFlow, self).__init__(None, VoidDomain(), VoidBinding())

    def __basis__(self):
        return ()


class ScopeFlow(Flow):
    pass


class HomeFlow(ScopeFlow):

    def __init__(self, base, binding):
        super(HomeFlow, self).__init__(base, EntityDomain(), binding)

    def __basis__(self):
        return (self.base,)


class RootFlow(HomeFlow):

    def __init__(self, binding):
        super(RootFlow, self).__init__(None, binding)

    def __basis__(self):
        return ()


class TableFlow(ScopeFlow):

    def __init__(self, base, table, binding):
        assert isinstance(table, TableEntity)
        super(TableFlow, self).__init__(base, EntityDomain(), binding)
        self.table = table

    def __basis__(self):
        return (self.base, self.table)


class ChainFlow(TableFlow):

    def __init__(self, base, joins, binding):
        assert isinstance(joins, listof(Join)) and len(joins) > 0
        super(ChainFlow, self).__init__(base, joins[-1].target, binding)
        self.joins = joins

    def __basis__(self):
        return (self.base, tuple(self.joins))


class ColumnFlow(ScopeFlow):

    def __init__(self, base, column, link, binding):
        assert isinstance(column, ColumnEntity)
        assert isinstance(link, maybe(Flow))
        super(ColumnFlow, self).__init__(base, column.domain, binding)
        self.column = column
        self.link = link

    def __basis__(self):
        return (self.base, self.column, self.link)


class QuotientFlow(ScopeFlow):

    def __init__(self, base, seed, kernels, binding):
        super(QuotientFlow, self).__init__(base, EntityDomain(), binding)
        self.seed = seed
        self.kernels = kernels

    def __basis__(self):
        return (self.base, self.seed, tuple(self.kernels))


class CoverFlow(ScopeFlow):

    def __init__(self, base, seed, binding):
        super(CoverFlow, self).__init__(base, seed.domain, binding)
        self.seed = seed

    def __basis__(self):
        return (self.base, self.seed)


class KernelFlow(CoverFlow):

    def __init__(self, base, quotient, index, binding):
        assert isinstance(quotient, QuotientFlow)
        assert isinstance(index, int)
        assert 0 <= index < len(quotient.kernels)
        seed = quotient.kernels[index]
        super(KernelFlow, self).__init__(base, seed, binding)
        self.quotient = quotient
        self.index = index

    def __basis__(self):
        return (self.base, self.quotient, self.index)


class ComplementFlow(CoverFlow):

    def __init__(self, base, quotient, binding):
        assert isinstance(quotient, QuotientFlow)
        super(ComplementFlow, self).__init__(base, quotient.seed, binding)
        self.quotient = quotient

    def __basis__(self):
        return (self.base, self.quotient)


class ForkFlow(CoverFlow):

    def __init__(self, base, kernels, binding):
        assert isinstance(kernels, listof(Flow))
        super(ForkFlow, self).__init__(base, base, binding)
        self.kernels = kernels

    def __basis__(self):
        return (self.base, tuple(self.kernels))


class AttachFlow(CoverFlow):

    def __init__(self, base, seed, images, condition, binding):
        assert isinstance(images, listof(tupleof(Flow, Flow)))
        assert isinstance(condition, maybe(Flow))
        if condition is not None:
            assert isinstance(condition.domain, BooleanDomain)
        super(AttachFlow, self).__init__(base, seed, binding)
        self.images = images
        self.condition = condition

    def __basis__(self):
        return (self.base, self.seed, tuple(self.images), self.condition)


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

    def __basis__(self):
        return (self.base, self.seed, tuple(self.order), self.limit, self.offset)


class CollectFlow(Flow):

    def __init__(self, base, seed, domain, binding):
        assert isinstance(base, Flow)
        assert isinstance(seed, Flow)
        super(CollectFlow, self).__init__(base, domain, binding)
        self.seed = seed

    def __basis__(self):
        return (self.base, self.seed, self.domain)


class SieveFlow(ScopeFlow):

    def __init__(self, base, filter, binding):
        assert isinstance(filter, Flow)
        assert isinstance(filter.domain, BooleanDomain)
        super(SieveFlow, self).__init__(base, base.domain, binding)
        self.filter = filter

    def __basis__(self):
        return (self.base, self.filter)


class SortFlow(ScopeFlow):

    def __init__(self, base, order, limit, offset, binding):
        assert isinstance(order, listof(tupleof(Flow, int)))
        assert isinstance(limit, maybe(int))
        assert isinstance(offset, maybe(int))
        super(SortFlow, self).__init__(base, base.domain, binding)
        self.order = order
        self.limit = limit
        self.offset = offset

    def __basis__(self):
        return (self.base, tuple(self.order), self.limit, self.offset)


class RescopingFlow(ScopeFlow):

    def __init__(self, base, scope, binding):
        assert isinstance(scope, Flow)
        super(RescopingFlow, self).__init__(base, base.domain, binding)
        self.scope = scope

    def __basis__(self):
        return (self.base, self.scope)


class SelectionFlow(ScopeFlow):

    def __init__(self, base, elements, domain, binding):
        assert isinstance(elements, listof(Flow))
        super(SelectionFlow, self).__init__(base, domain, binding)
        self.elements = elements

    def __basis__(self):
        return (self.base, tuple(self.elements), self.domain)


class IdentityFlow(Flow):

    def __init__(self, base, elements, binding):
        assert isinstance(elements, listof(Flow))
        domain = IdentityDomain([element.domain for element in elements])
        super(IdentityFlow, self).__init__(base, domain, binding)
        self.elements = elements
        self.width = domain.width

    def __basis__(self):
        return (self.base, tuple(self.elements))


class LiteralFlow(Flow):

    def __init__(self, base, value, domain, binding):
        super(LiteralFlow, self).__init__(base, domain, binding)
        self.value = value

    def __basis__(self):
        if not isinstance(self.value, (list, dict)):
            return (self.base, self.value, self.domain)
        else:
            return (self.base, repr(self.value), self.domain)


class CastFlow(Flow):

    def __init__(self, base, domain, binding):
        super(CastFlow, self).__init__(base, domain, binding)

    def __basis__(self):
        return (self.base, self.domain)


class ImplicitCastFlow(CastFlow):
    pass


class FormulaFlow(Formula, Flow):

    def __init__(self, base, signature, domain, binding, **arguments):
        assert isinstance(signature, Signature)
        arguments = Bag(**arguments)
        assert arguments.admits(Flow, signature)
        super(FormulaFlow, self).__init__(signature, arguments,
                                          base, domain, binding)

    def __basis__(self):
        return (self.base, self.signature, self.domain, self.arguments.freeze())


