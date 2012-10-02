#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.adapter import Adapter, adapt
from ....core.connect import transaction
from ....core.domain import IdentityDomain
from ....core.cmd.command import DefaultCmd
from ....core.cmd.act import Act, ProduceAction, produce
from ....core.cmd.fetch import Product
from ....core.tr.syntax import (WeakSegmentSyntax, AssignmentSyntax,
        ReferenceSyntax)
from ....core.tr.bind import BindingState
from ....core.tr.binding import (VoidBinding, QueryBinding, SegmentBinding,
        LiteralBinding, IdentityBinding, DefinitionBinding, BindingRecipe,
        ClosedRecipe)
from ....core.tr.decorate import decorate
from ....core.tr.lookup import lookup_command
from ....core.tr.error import BindError
from .command import DoCmd


class ProduceDo(Act):

    adapt(DoCmd, ProduceAction)

    def __call__(self):
        scope = self.command.scope
        root = scope
        while root.base is not None:
            root = root.base
        reference = None
        recipe = None
        product = None
        with transaction():
            for op in self.command.ops:
                state = BindingState()
                state.set_root(root)
                state.push_scope(scope)
                if reference is not None:
                    seed = state.use(recipe, reference)
                    scope = DefinitionBinding(scope, reference.identifier.value,
                                              True, None, recipe, reference)
                    state.push_scope(scope)
                    reference = None
                    recipe = None
                if isinstance(op, AssignmentSyntax):
                    if not isinstance(op.lbranch, ReferenceSyntax):
                        raise BindError("a reference is expected",
                                        op.lbranch.mark)
                    reference = op.lbranch
                    op = op.rbranch
                op = WeakSegmentSyntax(op, op.mark)
                op = state.bind(op)
                command = lookup_command(op)
                if command is None:
                    if not isinstance(op, SegmentBinding):
                        raise BindError("a segment is expected", op.mark)
                    profile = decorate(op)
                    binding = QueryBinding(state.root, op, profile, op.syntax)
                    command = DefaultCmd(binding)
                product = produce(command)
                if reference is not None:
                    if (isinstance(product.meta.domain, IdentityDomain) and
                            product.data is not None):
                        def convert(domain, data):
                            items = []
                            for element, item in zip(domain.fields, data):
                                if isinstance(element, IdentityDomain):
                                    item = convert(element, item)
                                else:
                                    item = LiteralBinding(state.scope, item,
                                                          element, reference)
                                items.append(item)
                            return IdentityBinding(state.scope, items,
                                                   reference)
                        literal = convert(product.meta.domain, product.data)
                    else:
                        literal = LiteralBinding(state.scope, product.data,
                                                 product.meta.domain, reference)
                    recipe = ClosedRecipe(BindingRecipe(literal))
        return product


