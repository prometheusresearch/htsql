#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..adapter import Adapter, adapt, adapt_many
from ..error import Error, translate_guard
from .binding import (Binding, CollectBinding, WrappingBinding,
        DecorateBinding, SelectionBinding, HomeBinding, RootBinding,
        TableBinding, ChainBinding, ColumnBinding, QuotientBinding,
        KernelBinding, ComplementBinding, IdentityBinding, LocateBinding,
        CoverBinding, ForkBinding, AttachBinding, ClipBinding, SieveBinding,
        SortBinding, CastBinding, RescopingBinding, LiteralBinding,
        FormulaBinding)
from .flow import (Flow, CollectFlow, SelectionFlow, HomeFlow, RootFlow,
        TableFlow, ChainFlow, ColumnFlow, QuotientFlow, KernelFlow,
        ComplementFlow, IdentityFlow, LocateFlow, CoverFlow, ForkFlow,
        AttachFlow, ClipFlow, SieveFlow, SortFlow, CastFlow, RescopingFlow,
        LiteralFlow, FormulaFlow)
from .lookup import direct


class RoutingState:

    def __init__(self):
        self.cache = {}

    def route(self, binding):
        if binding in self.cache:
            return self.cache[binding]
        with translate_guard(binding):
            flow = Route.__invoke__(binding, self)
            self.cache[binding] = flow
            return flow


class Route(Adapter):

    adapt(Binding)

    def __init__(self, binding, state):
        self.binding = binding
        self.state = state

    def __call__(self):
        raise Error("Cannot route an expression")


class RouteCollect(Route):

    adapt(CollectBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        return CollectFlow(base, seed, self.binding.domain, self.binding)


class RouteRoot(Route):

    adapt(RootBinding)

    def __call__(self):
        return RootFlow(self.binding)


class RouteHome(Route):

    adapt(HomeBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        return HomeFlow(base, self.binding)


class RouteTable(Route):

    adapt(TableBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        return TableFlow(base, self.binding.table, self.binding)


class RouteChain(Route):

    adapt(ChainBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        return ChainFlow(base, self.binding.joins, self.binding)


class RouteSieve(Route):

    adapt(SieveBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        filter = self.state.route(self.binding.filter)
        return SieveFlow(base, filter, self.binding)


class RouteSort(Route):

    adapt(SortBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        order = []
        for binding in self.binding.order:
            flow = self.state.route(binding)
            direction = direct(binding)
            if direction is None:
                direction = +1
            order.append((flow, direction))
        limit = self.binding.limit
        offset = self.binding.offset
        return SortFlow(base, order, limit, offset, self.binding)


class RouteQuotient(Route):

    adapt(QuotientBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        kernels = [self.state.route(binding)
                   for binding in self.binding.kernels]
        return QuotientFlow(base, seed, kernels, self.binding)


class RouteComplement(Route):

    adapt(ComplementBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        quotient = self.state.route(self.binding.quotient)
        return ComplementFlow(base, quotient, self.binding)


class RouteCover(Route):

    adapt(CoverBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        return CoverFlow(base, seed, self.binding)


class RouteFork(Route):

    adapt(ForkBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        kernels = [self.state.route(binding)
                   for binding in self.binding.kernels]
        return ForkFlow(base, kernels, self.binding)


class RouteAttach(Route):

    adapt(AttachBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        images = [(self.state.route(lbinding), self.state.route(rbinding))
                  for lbinding, rbinding in self.binding.images]
        condition = None
        if self.binding.condition is not None:
            condition = self.state.route(self.binding.condition)
        return AttachFlow(base, seed, images, condition, self.binding)


class RouteClip(Route):

    adapt(ClipBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        order = [(self.state.route(binding), direction)
                 for binding, direction in self.binding.order]
        return ClipFlow(base, seed, order, self.binding.limit,
                        self.binding.offset, self.binding)


class RouteLocate(Route):

    adapt(LocateBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        seed = self.state.route(self.binding.seed)
        images = [(self.state.route(lop), self.state.route(rop))
                  for lop, rop in self.binding.images]
        condition = None
        if self.binding.condition is not None:
            condition = self.state.route(self.binding.condition)
        return LocateFlow(base, seed, images, condition, self.binding)


class RouteColumn(Route):

    adapt(ColumnBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        link = None
        if self.binding.link is not None:
            link = self.state.route(self.binding.link)
        return ColumnFlow(base, self.binding.column, link, self.binding)


class RouteKernel(Route):

    adapt(KernelBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        quotient = self.state.route(self.binding.quotient)
        return KernelFlow(base, quotient, self.binding.index, self.binding)


class RouteLiteral(Route):

    adapt(LiteralBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        return LiteralFlow(base, self.binding.value, self.binding.domain,
                           self.binding)


class RouteCast(Route):

    adapt(CastBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        return CastFlow(base, self.binding.domain, self.binding)


class RouteRescoping(Route):

    adapt(RescopingBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        scope = self.state.route(self.binding.scope)
        return RescopingFlow(base, scope, self.binding)


class RouteFormula(Route):

    adapt(FormulaBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        arguments = self.binding.arguments.map(self.state.route)
        return FormulaFlow(base, self.binding.signature,
                           self.binding.domain,
                           self.binding,
                           **arguments)


class RouteSelection(Route):

    adapt(SelectionBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        elements = [self.state.route(element)
                    for element in self.binding.elements]
        return SelectionFlow(base, elements, self.binding.domain, self.binding)


class RouteIdentity(Route):

    adapt(IdentityBinding)

    def __call__(self):
        base = self.state.route(self.binding.base)
        elements = [self.state.route(element)
                    for element in self.binding.elements]
        return IdentityFlow(base, elements, self.binding)


class RouteWrapping(Route):

    adapt_many(WrappingBinding,
               DecorateBinding)

    def __call__(self):
        return self.state.route(self.binding.base)


def route(binding):
    state = RoutingState()
    return state.route(binding)


