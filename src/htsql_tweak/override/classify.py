#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import adapts_none, adapts
from htsql.model import (HomeNode, TableNode, TableArc, ColumnArc, ChainArc,
                         SyntaxArc)
from htsql.classify import (Trace, TraceHome, TraceTable,
                            Call, CallTable, CallColumn, CallChain)


class OverrideTrace(Trace):

    adapts_none()

    def __call__(self):
        addon = context.app.tweak.override
        for arc in super(OverrideTrace, self).__call__():
            yield arc
        for pattern in sorted(addon.labels, key=(lambda node: str(node))):
            if pattern.matches(self.node):
                for name in sorted(addon.labels[pattern]):
                    arc_pattern = addon.labels[pattern][name]
                    arc = arc_pattern.extract(self.node)
                    if arc is not None:
                        yield arc


class OverrideTraceHome(OverrideTrace, TraceHome):

    adapts(HomeNode)


class OverrideTraceTable(OverrideTrace, TraceTable):

    adapts(TableNode)


class OverrideCall(Call):

    adapts_none()

    def __call__(self):
        addon = context.app.tweak.override
        for name, weight in super(OverrideCall, self).__call__():
            yield name, weight
        for pattern in sorted(addon.labels, key=(lambda node: str(node))):
            if pattern.matches(self.arc.origin):
                for name in sorted(addon.labels[pattern]):
                    arc_pattern = addon.labels[pattern][name]
                    if arc_pattern.matches(self.arc):
                        yield name, 20


class OverrideCallTable(OverrideCall, CallTable):

    adapts(TableArc)


class OverrideCallColumn(OverrideCall, CallColumn):

    adapts(ColumnArc)


class OverrideCallChain(OverrideCall, CallChain):

    adapts(ChainArc)


class OverrideCallSyntax(OverrideCall):

    adapts(SyntaxArc)


