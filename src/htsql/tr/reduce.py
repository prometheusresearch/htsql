#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.tr.reduce`
======================

This module implements the reducing process.
"""


from ..adapter import Adapter, adapts
from ..domain import BooleanDomain, StringDomain
from .coerce import coerce
from .compile import ordering
from .term import PermanentTerm
from .frame import (Clause, Frame, ScalarFrame, BranchFrame, NestedFrame,
                    QueryFrame, Phrase, LiteralPhrase, NullPhrase,
                    TruePhrase, FalsePhrase, CastPhrase, FormulaPhrase,
                    ExportPhrase, ReferencePhrase, Anchor, LeadingAnchor)
from .signature import (Signature, isformula, IsEqualSig, IsTotallyEqualSig,
                        IsInSig, IsNullSig, IfNullSig, NullIfSig,
                        AndSig, OrSig, NotSig, FromPredicateSig,
                        ToPredicateSig)


class ReducingState(object):
    """
    Encapsulates the state of the reducing and collapsing processes.

    State attributes:

    `substitutes` (a mapping `tag -> list of phrases`)
        A mapping containing the `SELECT` clause of collapsed frames.
    """

    def __init__(self):
        # `SELECT` clauses of collapsed frames by the frame tag.
        self.substitutes = {}

    def flush(self):
        """
        Clears the state.
        """
        self.substitutes.clear()

    def reduce(self, clause):
        """
        Reduces (simplifies) a SQL clause.

        Returns an equivalent (possibly the same) clause.

        `clause` (:class:`htsql.tr.frame.Clause`)
            The clause to simplify.
        """
        # Realize and apply the `Reduce` adapter.
        return reduce(clause, self)

    def collapse(self, frame):
        """
        Collapses a frame.

        Returns an equivalent (possibly the same) frame.

        Note that the generated frame may contain some clauses that refer
        to subframes which no longer exist.  To fix broken references,
        apply :meth:`reduce` to the returned frame.

        `frame` (:class:`htsql.tr.frame.Frame`)
            The frame to collapse.
        """
        # Realize and apply the `Collapse` adapter.
        collapse = Collapse(frame, self)
        return collapse()

    def to_predicate(self, phrase):
        phrase = FormulaPhrase(ToPredicateSig(), phrase.domain,
                               phrase.is_nullable, phrase.expression,
                               op=phrase)
        return self.reduce(phrase)

    def from_predicate(self, phrase):
        phrase = FormulaPhrase(FromPredicateSig(), phrase.domain,
                               phrase.is_nullable, phrase.expression,
                               op=phrase)
        return self.reduce(phrase)


class Reduce(Adapter):
    """
    Reduces (simplifies) a SQL clause.

    This is an interface adapter; see subclasses for implementations.

    The :class:`Reduce` adapter has the following signature::

        Reduce: (Clause, ReducingState) -> Clause

    The adapter is polymorphic on the `Claim` argument.

    `clause` (:class:`htsql.tr.frame.Clause`)
        The clause node.

    `state` (:class:`ReducingState`)
        The current state of the reducing process.
    """

    adapts(Clause)

    def __init__(self, clause, state):
        assert isinstance(clause, Clause)
        assert isinstance(state, ReducingState)
        self.clause = clause
        self.state = state

    def __call__(self):
        # Implement in subclasses.
        raise NotImplementedError("the reduce adapter is not implemented"
                                  " for a %r node" % self.clause)


class ReduceFrame(Reduce):
    """
    Reduces a SQL frame.

    The adapter collapses the subframes of the frame node and simplifies
    its clauses.  This is an abstract adapter; see subclasses for concrete
    implementations.

    `frame` (:class:`htsql.tr.frame.Frame`)
        The frame node.

    `state` (:class:`ReducingState`)
        The current state of the reducing process.
    """

    adapts(Frame)

    def __init__(self, frame, state):
        super(ReduceFrame, self).__init__(frame, state)
        self.frame = frame

    def __call__(self):
        # Return the frame itself; this default implementation is used
        # only by a table frame.  The scalar, the nested and the segment
        # frames have more elaborate implementations.
        return self.frame


class ReduceScalar(ReduceFrame):
    """
    Reduces a scalar frame.
    """

    adapts(ScalarFrame)

    def __call__(self):
        # The databases we currently support have no notion of a `DUAL`
        # table, so we replace scalar frame with an empty `SELECT`
        # statement.  Databases with a native `DUAL` table may need
        # to override this implementation.
        select = [TruePhrase(self.frame.expression)]
        return NestedFrame(include=[], embed=[], select=select,
                           where=None, group=[], having=None,
                           order=[], limit=None, offset=None,
                           term=self.frame.term)


class ReduceBranch(ReduceFrame):
    """
    Reduces a top-level or a nested ``SELECT`` statement.
    """

    adapts(BranchFrame)

    def reduce_include(self):
        # Reduce the anchors of the subframes.  This will also
        # collapse and reduce the subframes themselves.
        return [self.state.reduce(anchor)
                for anchor in self.frame.include]

    def reduce_embed(self):
        # Collapse and reduce the embedded subframes.
        return [self.state.reduce(self.state.collapse(frame))
                for frame in self.frame.embed]

    def reduce_select(self):
        # Reduce the `SELECT` clause.
        return [self.state.reduce(phrase)
                for phrase in self.frame.select]

    def reduce_where(self):
        # Reduce the `WHERE` clause.
        if self.frame.where is None:
            return None
        where = self.state.reduce(self.frame.where)
        # Eliminate a `WHERE TRUE` clause.
        if isinstance(where, TruePhrase):
            where = None
        return where

    def reduce_group(self):
        # Reduce the `GROUP BY` clause.
        # Here we reduce all the phrases in the `GROUP BY` clause and
        # also eliminate duplicates and literals.  As a result of the latter,
        # we may produce an empty `GROUP BY` clause (for instance, for scalar
        # projections), which may confuse the frame collapser or even
        # change the semantics of the `SELECT` statement.  Because of that,
        # `collapse()` should never be applied after `reduce()`.
        group = []
        duplicates = set()
        for phrase in self.frame.group:
            phrase = self.state.reduce(phrase)
            if isinstance(phrase, LiteralPhrase):
                continue
            if phrase in duplicates:
                continue
            group.append(phrase)
            duplicates.add(phrase)
        return group

    def reduce_having(self):
        # Reduce the `HAVING` clause.
        if self.frame.having is None:
            return None
        having = self.state.reduce(self.frame.having)
        # Eliminate a `HAVING TRUE` clause.
        if isinstance(having, TruePhrase):
            having = None
        return having

    def reduce_order(self):
        # Reduce the `ORDER BY` clause.
        # Here we reduce all the phrases in the `ORDER BY` clause and
        # also eliminate duplicates and literals.
        order = []
        duplicates = set()
        for phrase, direction in self.frame.order:
            phrase = self.state.reduce(phrase)
            if isinstance(phrase, LiteralPhrase):
                continue
            if phrase in duplicates:
                continue
            order.append((phrase, direction))
            duplicates.add(phrase)
        return order

    def __call__(self):
        # Reduce a `SELECT` statement.

        # Reduce and collapse the subframes in the `FROM` clause.
        include = self.reduce_include()
        # Reduce and collapse the embedded subframes.
        embed = self.reduce_embed()
        # Reduce the `SELECT`, `WHERE`, `GROUP BY`, `HAVING`, and
        # `ORDER BY` clauses.
        select = self.reduce_select()
        where = self.reduce_where()
        group = self.reduce_group()
        having = self.reduce_having()
        order = self.reduce_order()
        # Return a frame with reduced clauses.
        return self.frame.clone(include=include, embed=embed,
                                select=select, where=where,
                                group=group, having=having,
                                order=order)


class Collapse(Adapter):
    """
    Collapses nested subframes of the given frame.

    Returns an equivalent (possibly the same) frame.

    This is an auxiliary adapter used for flattening the frame structure.
    Using this adapter may remove some frames from the frame tree and thus
    invalidate any references to these frames.  Apply the :class:`Reduce`
    adapter to fix the references.

    `frame` (:class:`htsql.tr.frame.Frame`)
        The frame node.

    `state` (:class:`ReducingState`)
        The current state of the reducing process.
    """

    adapts(Frame)

    def __init__(self, frame, state):
        assert isinstance(frame, Frame)
        assert isinstance(state, ReducingState)
        self.frame = frame
        self.state = state

    def __call__(self):
        # The default implementation does nothing; used by leaf frames.
        return self.frame


class CollapseBranch(Collapse):
    """
    Collapses a branch frame.
    """

    adapts(BranchFrame)

    def __call__(self):
        # Here we attempt to dismantle the first subframe in the `FROM`
        # clause and include its content to the outer frame.  We continue
        # this process until no further reductions could be done.
        # We must be especially careful to not change the semantics
        # of the query!
        # FIXME: we do not attempt to absorb the content of any other
        # subframe but the first one.  It may be even trickier because
        # we need to carry over the `JOIN` condition.  There are some
        # cases when we may want to absorb the second subframe
        # (see a special case in `InjectSpace` when we grow a missing
        # axis).

        # No subframes in the `FROM` clause -- nothing to do.
        if not self.frame.include:
            return self.frame

        # The first subframe; let us try to eliminate it.
        head = self.frame.include[0].frame
        # The rest of the `FROM` clause.
        tail = self.frame.include[1:]

        # A frame corresponding to a permanent term is never collapsed down.
        if isinstance(head.term, PermanentTerm):
            return self.frame

        # Any subframe, including the head frame, is one of theses:
        # - a scalar frame, we could just discard it;
        # - a table frame, there is nothing we can do;
        # - a nested `SELECT` frame, if possible, include the
        #   content of the subframe into the outer frame.

        # First, check if the head frame is scalar.
        if head.is_scalar:
            # We could only discard the head subframe if the subframe
            # next to it has no `JOIN` condition (or the `FROM` clause
            # contains a single subframe).
            # FIXME: Some databases (i.e., Oracle) forbid an empty
            # `FROM` clause.
            if tail and not tail[0].is_cross:
                return self.frame
            # Indicate that the first anchor in the tail is now
            # a leading anchor.
            if tail:
                tail[0] = tail[0].clone_to(LeadingAnchor)
            # Make a new frame with a reduced `FROM` clause.
            frame = self.frame.clone(include=tail)
            # Try to further collapse the frame.
            return self.state.collapse(frame)

        # Now, only two possibilities are left: the head frame is either
        # a nested frame or a table frame.  If it is a table frame,
        # we are done.
        if not head.is_nested:
            return self.frame

        # Here goes a long list of checks to ensure we could merge the
        # content of the head subframe without breaking the query.

        # If the head subframe by itself has no subframes, the subframe
        # next to the head leads the `FROM` clause.  It is only valid
        # if the subframe has no `JOIN` condition.
        if not head.include:
            if tail and not tail[0].is_cross:
                return self.frame

        # We cannot safely collapse the head subframe if some other
        # subframe is attached using a `RIGHT OUTER` join.  Currently,
        # the compiler never generates right joins, so this check
        # is effectively no-op.
        if any(anchor.is_right for anchor in tail):
            return self.frame

        # A tricky case, the head subframe contains a `GROUP BY` clause.
        # We collapse frames like this only when they correspond to a
        # scalar projection, like in `/count(table)`.
        if head.group:
            # Verify that the kernel of the projection contains only
            # literal phrases, and thus will be eliminated by the reducer
            # (technically, it should contain a single `TRUE` literal,
            # but checking for arbitrary literals does not hurt).
            #if not all(isinstance(phrase, LiteralPhrase)
            #           for phrase in head.group):
            #    return self.frame

            # We only collapse a projection subframe if it is the only
            # subframe in the `FROM` clause.
            if tail:
                return self.frame
            # Ensure that the outer frame has no projection clauses, i.e.,
            # `GROUP BY` and `HAVING`.
            # The outer frame could still have `WHERE` clause (to be
            # moved to `HAVING`), as well as `ORDER BY`, `LIMIT`, and `OFFSET`.
            if self.frame.group or self.frame.having is not None:
                return self.frame
            # Ensure that the subframe has no `HAVING` clause.
            # The subframe could still have `WHERE`, `ORDER BY`, `LIMIT`,
            # and `OFFSET` clauses.
            if head.having is not None:
                return self.frame
            # If the outer frame has a `WHERE` clause, make sure the
            # `GROUP BY` clause of the subframe is not trivial since
            # not every database engine can handle `HAVING` without
            # `GROUP BY` (PostgreSQL can, SQLite cannot).
            if (self.frame.where is not None and
                all(isinstance(phrase, LiteralPhrase)
                               for phrase in head.group)):
                return self.frame

        # If we reached this point, the `HAVING` clause of the head subframe
        # must be empty.  This check is no-op though since we never generate
        # `HAVING` in the first place.
        assert head.having is None

        # Another tricky case: the subframe has a `LIMIT` or an `OFFSET`
        # clause.
        if not (head.limit is None and
                head.offset is None):
            # Now we need to answer the question: could we safely merge
            # the head frame to the outer frame.  It is only safe when
            # both the outer and the inner frames produce the same
            # number of rows.  However we cannot deduce that from the
            # frame structure only, so we analyze the spaces from which
            # the frames were compiled.
            # The inner and the outer frames would produce the same number
            # of rows if they are compiled from the same space, or, at
            # least, the spaces they are compiled from conform to each
            # other.  We also verify that their baseline spaces are equal,
            # but this is a redundant check -- precense of a non-trivial
            # `LIMIT` or `OFFSET` implies that the baseline is the scalar
            # space (see `CompileOrdered` in `htsql.tr.compile`).
            # Other than that, we also need to check that the `ORDER BY`
            # clauses of the inner and outer frames coincide (if they
            # both are non-empty).  We cannot compare the clauses directly
            # since they contain different export references, but we
            # can compare the ordering of the underlying spaces.
            if not (head.space.conforms(self.frame.space) and
                    head.baseline == self.frame.baseline and
                    ordering(head.space) == ordering(self.frame.space)):
                return self.frame
            # Since the inner and the outer spaces conform to each other,
            # the outer frame cannot contain non-trivial `LIMIT` and `OFFSET`
            # clauses.
            assert self.frame.limit is None and self.frame.offset is None

        # All checks passed, now we merge the head subframe to the outer
        # frame.

        # Merge the `FROM` clause of the head with the rest of the `FROM`
        # clause of the frame.
        if not head.include and tail:
            tail[0] = tail[0].clone_to(LeadingAnchor)
        include = head.include+tail

        # Merge the embedded subframes.
        embed = head.embed+self.frame.embed

        # Now that we merged the head subframe, all references to it
        # are broken and have to be replaced with the referenced phrases.
        # It is done by `reduce()`, but here we need to save the referenced
        # phrases.
        assert head.tag not in self.state.substitutes
        self.state.substitutes[head.tag] = head.select

        # Merge the `WHERE` clauses; both the inner and the outer `WHERE`
        # clauses could be non-empty, in which case we join them with `AND`.
        where = None
        if not head.group:
            where = self.frame.where
        if head.where is not None:
            if where is None:
                where = head.where
            else:
                is_nullable = (where.is_nullable or head.where.is_nullable)
                where = FormulaPhrase(AndSig(), coerce(BooleanDomain()),
                                      is_nullable, where.expression,
                                      ops=[where, head.where])

        # Pull the `GROUP BY` clause.
        group = self.frame.group
        if head.group:
            assert not group
            group = head.group

        # If the subframe contains a `GROUP BY` clause, move the `WHERE`
        # clause of the outer frame to `HAVING`.
        having = self.frame.having
        if head.group:
            assert having is None
            having = self.frame.where

        # Merge the `ORDER BY` clauses.  There are several possibilities:
        # - the inner `ORDER BY` is empty;
        # - the outer `ORDER BY` is empty;
        # - both the inner and the outer `ORDER BY` clauses are non-empty;
        #   in this case, they must be identical (but not necessarily equal
        #   by-value).
        assert (not head.order or not self.frame.order or
                len(head.order) == len(self.frame.order))
        order = head.order
        if self.frame.order:
            order = self.frame.order

        # Merge the `LIMIT` and the `OFFSET` clauses.  Here either the
        # inner `LIMIT/OFFSET` or the outer `LIMIT/OFFSET` must be empty.
        assert ((head.limit is None and head.offset is None) or
                (self.frame.limit is None and self.frame.offset is None))
        limit = head.limit
        if self.frame.limit is not None:
            limit = self.frame.limit
        offset = head.offset
        if self.frame.offset is not None:
            offset = self.frame.offset

        # Update the frame node.
        frame = self.frame.clone(include=include, embed=embed, where=where,
                                 group=group, having=having,
                                 order=order, limit=limit, offset=offset)
        # Try to collapse the frame once again.
        return self.state.collapse(frame)


class ReduceAnchor(Reduce):
    """
    Reduces a ``JOIN`` clause.
    """

    adapts(Anchor)

    def __call__(self):
        # Collapse and reduce (in that order!) the subframe.
        frame = self.state.reduce(self.state.collapse(self.clause.frame))
        # Reduce the join condition.
        condition = (self.state.reduce(self.clause.condition)
                     if self.clause.condition is not None else None)
        # Update the anchor.
        return self.clause.clone(frame=frame, condition=condition)


class ReduceQuery(Reduce):
    """
    Reduces a top-level query frame.
    """

    adapts(QueryFrame)

    def __call__(self):
        # If there is no segment frame, there is nothing to reduce.
        if self.clause.segment is None:
            return self.clause
        # Collapse and reduce (in that order!) the segment frame.
        segment = self.clause.segment
        segment = self.state.collapse(segment)
        segment = self.state.reduce(segment)
        # Clear the state variables.
        self.state.flush()
        # Update the query frame.
        return self.clause.clone(segment=segment)


class ReducePhrase(Reduce):
    """
    Reduces a SQL phrase.
    """

    adapts(Phrase)

    # Note that we do not provide a default no-op implementation: every
    # non-leaf phrase node must at least apply `reduce()` to its subnodes
    # in order to fix broken references.

    def __init__(self, phrase, state):
        super(ReducePhrase, self).__init__(phrase, state)
        self.phrase = phrase


class ReduceLiteral(Reduce):
    """
    Reduces a literal phrase.
    """

    adapts(LiteralPhrase)

    def __call__(self):
        # We cannot really simplify a literal value; instead, we encode
        # some common literals: `NULL`, `TRUE`, `FALSE` so that they
        # could be recognized with a single `isinstance()` check.
        if self.phrase.value is None:
            return NullPhrase(self.phrase.domain, self.phrase.expression)
        if isinstance(self.phrase.domain, BooleanDomain):
            if self.phrase.value is True:
                return TruePhrase(self.phrase.expression)
            if self.phrase.value is False:
                return FalsePhrase(self.phrase.expression)
        return self.phrase


class ReduceCast(Reduce):
    """
    Reduces a ``CAST`` operator.
    """

    adapts(CastPhrase)

    def __call__(self):
        # Reduce the operand of the cast.  We do not specialize
        # on the domains here because we assume that any domain
        # specific conversion is already done by the encoder.
        return self.phrase.clone(base=self.state.reduce(self.phrase.base))


class ReduceFormula(Reduce):
    """
    Reduces a formula node.

    Reducing a formula is specific to the formula signature and is
    implemented by the :class:`ReduceBySignature` adapter.
    """

    adapts(FormulaPhrase)

    def __call__(self):
        # Delegate the reduction to the `ReduceBySignature` adapter.
        reduce = ReduceBySignature(self.phrase, self.state)
        return reduce()


class ReduceBySignature(Adapter):
    """
    Reduces a formula node.

    This is an auxiliary adapter used to reduce
    :class:`htsql.tr.frame.FormulaPhrase` nodes.  The adapter is polymorphic
    on the formula signature.

    Unless overridden, the adapter reduces the arguments of the formula
    and generates a new formula with the same signature.

    `phrase` (:class:`htsql.tr.frame.FormulaPhrase`)
        The formula node to reduce.

    `state` (:class:`ReducingState`)
        The current state of the reducing process.

    Aliases:

    `signature` (:class:`htsql.tr.signature.Signature`)
        The signature of the formula.

    `domain` (:class:`htsql.tr.domain.Domain`)
        The co-domain of the formula.

    `arguments` (:class:`htsql.tr.signature.Bag`)
        The arguments of the formula.

    `is_nullable` (Boolean)
        Indicates that the formula may produce a ``NULL`` value.
    """

    adapts(Signature)

    @classmethod
    def dispatch(interface, phrase, *args, **kwds):
        # Override the default dispatch since the adapter is polymorphic
        # not on the type of the formula, but on the type of the formula
        # signature.
        assert isinstance(phrase, FormulaPhrase)
        return (type(phrase.signature),)

    def __init__(self, phrase, state):
        assert isinstance(phrase, FormulaPhrase)
        assert isinstance(state, ReducingState)
        self.phrase = phrase
        self.state = state
        self.signature = phrase.signature
        self.domain = phrase.domain
        self.arguments = phrase.arguments
        self.is_nullable = phrase.is_nullable

    def __call__(self):
        # By default, just reduce the arguments of the formula.
        arguments = self.arguments.map(self.state.reduce)
        return FormulaPhrase(self.signature,
                             self.domain,
                             self.is_nullable,
                             self.phrase.expression,
                             **arguments)


class ReduceIsEqual(ReduceBySignature):
    """
    Reduces an (in)equality operator.
    """

    adapts(IsEqualSig)

    def __call__(self):
        # Start with reducing the operands.
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)

        # Check if both operands are `NULL`.
        if isinstance(lop, NullPhrase) and isinstance(rop, NullPhrase):
            # Reduce:
            #   null()=null(), null()!=null() => null()
            return self.state.to_predicate(
                    NullPhrase(self.phrase.domain, self.phrase.expression))

        # Now suppose one of the operands (`rop`) is `NULL`.
        if isinstance(lop, NullPhrase):
            lop, rop = rop, lop
        if isinstance(rop, NullPhrase):
            # We could reduce:
            #   lop=null(), lop!=null() => null(),
            # but that completely removes `lop` from the clause tree, and thus
            # may change the semantics of SQL (for instance, when `lop` is an
            # aggregate expression).  So we only do that when `lop` is a
            # literal.
            if isinstance(lop, LiteralPhrase):
                return self.state.to_predicate(
                        NullPhrase(self.phrase.domain, self.phrase.expression))

        # Check if both arguments are boolean literals.
        if (isinstance(lop, (TruePhrase, FalsePhrase)) and
            isinstance(rop, (TruePhrase, FalsePhrase))):
            # Reduce:
            #   true()=true() => true()
            #   ...
            # Note: we do not apply the same optimization for literals of
            # arbitrary type because we cannot precisely replicate the
            # database equality operator.
            polarity = self.signature.polarity
            if lop.value != rop.value:
                polarity = -polarity
            if polarity > 0:
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))
            else:
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))

        # None of specific optimizations were applied, just return
        # the same operator with reduced operands.  Update the `is_nullable`
        # status since it may change after reducing the arguments.
        is_nullable = (lop.is_nullable or rop.is_nullable)
        return self.phrase.clone(is_nullable=is_nullable, lop=lop, rop=rop)


class ReduceIsTotallyEqual(ReduceBySignature):
    """
    Reduces a total (in)equality operator.
    """

    adapts(IsTotallyEqualSig)

    def __call__(self):
        # The polarity of the operator: +1 for `==`, -1 for `!==`.
        polarity = self.signature.polarity

        # Start with reducing the operands.
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)

        # Check if both operands are `NULL`.
        if isinstance(lop, NullPhrase) and isinstance(rop, NullPhrase):
            # Reduce:
            #   null()==null() => true, null()!==null() => false()
            if polarity > 0:
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))
            else:
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))

        # Now suppose one of the operands (`rop`) is `NULL`.
        if isinstance(lop, NullPhrase):
            lop, rop = rop, lop
        if isinstance(rop, NullPhrase):
            # We could always reduce:
            #   lop==null() => is_null(lop)
            #   lop!==null() => !is_null(lop)
            # In addition, if we know that `lop` is not nullable, we could have
            # reduced:
            #   lop==null() => false()
            #   lop!==null() => true()
            # However that completely removes `lop` from the clause tree, which,
            # in some cases, may change the semantics of SQL (for instance, when
            # `lop` is an aggregate expression).  Therefore, we only apply this
            # optimization when `lop` is a literal.
            if isinstance(lop, LiteralPhrase):
                if polarity > 0:
                    return self.state.to_predicate(
                            FalsePhrase(self.phrase.expression))
                else:
                    return self.state.to_predicate(
                            TruePhrase(self.phrase.expression))
            return FormulaPhrase(IsNullSig(polarity), self.domain,
                                 False, self.phrase.expression, op=lop)

        # Check if both arguments are boolean literals.
        if (isinstance(lop, (TruePhrase, FalsePhrase)) and
            isinstance(rop, (TruePhrase, FalsePhrase))):
            # Reduce:
            #   true()==true() => true()
            #   ...
            # Note: we do not apply the same optimization for literals of
            # arbitrary type because we cannot precisely replicate the
            # database equality operator.
            if polarity * (-1 if (lop.value != rop.value) else +1):
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))
            else:
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))

        # When both operands are not nullable, we could replace a total
        # comparison operator with a regular one.
        if not (lop.is_nullable or rop.is_nullable):
            return self.phrase.clone(signature=IsEqualSig(polarity),
                                     lop=lop, rop=rop)

        # FIXME: we also need to replace a total comparison operator
        # with a regular one for databases that do not have a native
        # total comparison operator.

        # None of specific optimizations were applied, just return
        # the same operator with reduced operands.
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceIsIn(ReduceBySignature):
    """
    Reduces the ``IN`` and ``NOT IN`` clauses.
    """

    adapts(IsInSig)

    def __call__(self):
        # Reduce the left operand.
        lop = self.state.reduce(self.phrase.lop)
        # Reduce the right operands, eliminating duplicates.
        rops = []
        duplicates = set()
        for rop in self.phrase.rops:
            rop = self.state.reduce(rop)
            if rop in duplicates:
                continue
            rops.append(rop)
            duplicates.add(rop)

        # Reduce:
        #   null()={...} => null()
        # We could do this substitution safely only when all operands
        # on the right are literals.
        if isinstance(lop, NullPhrase):
            if all(isinstance(rop, LiteralPhrase) for rop in rops):
                return self.state.to_predicate(
                        NullPhrase(self.domain, self.phrase.expression))
        # Similarly, reduce:
        #   x={null(),null(),...}
        if all(isinstance(rop, NullPhrase) for rop in rops):
            if isinstance(lop, LiteralPhrase):
                return self.state.to_predicate(
                        NullPhrase(self.domain, self.phrase.expression))

        # Reduce:
        #   x={y} => x=y
        if len(rops) == 1:
            rop = [rops]
            signature = IsEqualSig(self.signature.polarity)
            is_nullable = (lop.is_nullable or rop.is_nullable)
            return FormulaPhrase(signature, self.domain, is_nullable,
                                 self.phrase.expression, lop=lop, rop=rop)

        # None of specific optimizations were applied, just return
        # the same operator with reduced operands.  Update the `is_nullable`
        # status since it may change after reducing the arguments.
        is_nullable = (lop.is_nullable or any(rop.is_nullable for rop in rops))
        return self.phrase.clone(is_nullable=is_nullable, lop=lop, rops=rops)


class ReduceIsNull(ReduceBySignature):
    """
    Reduces the ``IS NULL`` and ``IS NOT NULL`` clauses.
    """

    adapts(IsNullSig)

    def __call__(self):
        # Start with reducing the operand.
        op = self.state.reduce(self.phrase.op)

        # Reduce:
        #   is_null(null()) => true()
        #   !is_null(null()) => false()
        if isinstance(op, NullPhrase):
            if self.signature.polarity > 0:
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))
            else:
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))
        # If the operand is not nullable, we could reduce the operator
        # to a `TRUE` or a `FALSE` clause.  However it is only safe
        # to do for a literal operand.
        if isinstance(op, LiteralPhrase):
            if self.signature.polarity > 0:
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))
            else:
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))

        # Return the same operator with a reduced operand.
        return self.phrase.clone(op=op)


class ReduceIfNull(ReduceBySignature):
    """
    Reduces the ``IFNULL`` clause.
    """

    adapts(IfNullSig)

    def __call__(self):
        # Reduce the operands.
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)

        # If the first operand is not nullable, then the operation is no-op,
        # and we could just return the first operand discarding the second
        # one.  However discarding a clause is not safe in general, so we
        # only do that when the second operand is a literal.
        if not lop.is_nullable and isinstance(rop, LiteralPhrase):
            return lop
        # Reduce:
        #   if_null(lop,null()) => lop
        if isinstance(rop, NullPhrase):
            return lop
        # Reduce:
        #   if_null(null(),rop) => rop
        if isinstance(lop, NullPhrase):
            return rop

        # Return the same operator with reduced operands.
        is_nullable = (lop.is_nullable and rop.is_nullable)
        return self.phrase.clone(is_nullable=is_nullable, lop=lop, rop=rop)


class ReduceNullIf(ReduceBySignature):
    """
    Reduces the ``NULLIF`` clause.
    """

    adapts(NullIfSig)

    def __call__(self):
        # Reduce the operands.
        lop = self.state.reduce(self.phrase.lop)
        rop = self.state.reduce(self.phrase.rop)
        # Reduce (when it is safe, i.e., when `rop` is a literal):
        #   null_if(null(),rop) => null()
        if isinstance(lop, NullPhrase):
            if isinstance(rop, LiteralPhrase):
                return lop
        # Reduce:
        #   null_if(lop,null()) => lop
        if isinstance(rop, NullPhrase):
            return lop
        # When both operands are literals, we could determine the result
        # immediately.  We should be careful though since we cannot precisely
        # mimic the equality operator of the database.
        if isinstance(lop, LiteralPhrase) and isinstance(rop, LiteralPhrase):
            # Assume that if the literals are equal in Python, they would
            # be equal for the database too.  The reverse is not valid in
            # general, but still valid for some literals.
            if lop.value == rop.value:
                return NullPhrase(self.phrase.domain, self.phrase.expression)
            # We could safely rely on comparison for Boolean values.
            elif isinstance(self.phrase.domain, BooleanDomain):
                return lop
            # In general, we can't rely on comparison for string values,
            # but we could assume that an empty string is only equal to itself.
            elif isinstance(self.phrase.domain, StringDomain):
                if len(lop.value) > 0 and len(rop.value) == 0:
                    return lop

        # Return the same operator with reduced operands.
        return self.phrase.clone(lop=lop, rop=rop)


class ReduceAnd(ReduceBySignature):
    """
    Reduces "AND" (``&``) operator.
    """

    adapts(AndSig)

    def __call__(self):
        # Start with reducing the operands.
        ops = []
        duplicates = set()
        for op in self.phrase.ops:
            # Reduce the operand.
            op = self.state.reduce(op)
            # Weed out duplicates.
            if op in duplicates:
                continue
            # Weed out `TRUE` literals.
            if isinstance(op, TruePhrase):
                continue
            # Expand a nested `AND` operator.
            if isformula(op, AndSig):
                # The operands of the nested `AND` are already reduced,
                # but we still need to weed out duplicates.
                for nop in op.ops:
                    if nop in duplicates:
                        continue
                    ops.append(nop)
                    duplicates.add(nop)
                continue
            ops.append(op)
            duplicates.add(op)

        # Reduce:
        #   "&"() => true()
        if not ops:
            return self.state.to_predicate(TruePhrase(self.phrase.expression))
        # Reduce:
        #   "&"(op) => op
        if len(ops) == 1:
            return ops[0]
        # We could reduce:
        #   "&"(...,false(),...) => false()
        # but that means we discard the rest of the operands
        # from the clause tree, which is unsafe.  So we only
        # do that when all operands are literals.
        if any(isinstance(op, FalsePhrase) for op in ops):
            if all(isinstance(op, LiteralPhrase) for op in ops):
                return self.state.to_predicate(
                        FalsePhrase(self.phrase.expression))

        # Return the same operator with reduced operands.  Update
        # the `is_nullable` status since it could change after reducing
        # the arguments.
        is_nullable = any(op.is_nullable for op in ops)
        if any(isinstance(op, FalsePhrase) for op in ops):
            is_nullable = False
        return self.phrase.clone(is_nullable=is_nullable, ops=ops)


class ReduceOr(ReduceBySignature):
    """
    Reduces "OR" (``|``) operator.
    """

    adapts(OrSig)

    def __call__(self):
        # Start with reducing the operands.
        ops = []
        duplicates = set()
        for op in self.phrase.ops:
            # Reduce the operand.
            op = self.state.reduce(op)
            # Weed out duplicates.
            if op in duplicates:
                continue
            # Weed out `FALSE` literals.
            if isinstance(op, FalsePhrase):
                continue
            # Expand a nested `OR` operator.
            if isformula(op, OrSig):
                # The operands of the nested `OR` are already reduced,
                # but we still need to weed out duplicates.
                for nop in op.ops:
                    if nop in duplicates:
                        continue
                    ops.append(nop)
                    duplicates.add(nop)
                continue
            ops.append(op)
            duplicates.add(op)

        # Reduce:
        #   "|"() => false()
        if not ops:
            return self.state.to_predicate(FalsePhrase(self.phrase.expression))
        # Reduce:
        #   "|"(op) => op
        if len(ops) == 1:
            return ops[0]
        # We could reduce:
        #   "|"(...,true(),...) => true()
        # but that means we discard the rest of the operands
        # from the clause tree, which is unsafe.  So we only
        # do that when all operands are literals.
        if any(isinstance(op, TruePhrase) for op in ops):
            if all(isinstance(op, LiteralPhrase) for op in ops):
                return self.state.to_predicate(
                        TruePhrase(self.phrase.expression))

        return self.phrase.clone(ops=ops)
        # Return the same operator with reduced operands.  Update
        # the `is_nullable` status since it could change after reducing
        # the arguments.
        is_nullable = any(op.is_nullable for op in ops)
        if any(isinstance(op, TruePhrase) for op in ops):
            is_nullable = False
        return self.phrase.clone(is_nullable=is_nullable, ops=ops)


class ReduceNot(ReduceBySignature):
    """
    Reduces a "NOT" (``!``) operator.
    """

    adapts(NotSig)

    def __call__(self):
        # Start with reducing the operand.
        op = self.state.reduce(self.phrase.op)

        # Reduce:
        #   !null() => null()
        #   !true() => false()
        #   !false() => true()
        if isinstance(op, NullPhrase):
            return self.state.to_predicate(
                    NullPhrase(self.phrase.domain, self.phrase.expression))
        if isinstance(op, TruePhrase):
            return self.state.to_predicate(FalsePhrase(self.phrase.expression))
        if isinstance(op, FalsePhrase):
            return self.state.to_predicate(TruePhrase(self.phrase.expression))
        # Reverse polarity of equality operators:
        #   !(lop=rop) => lop!=rop
        #   ...
        if isformula(op, (IsEqualSig, IsTotallyEqualSig, IsInSig, IsNullSig)):
            return op.clone(signature=op.signature.reverse())

        # Return the same operator with a reduced operand.
        return self.phrase.clone(is_nullable=op.is_nullable, op=op)


class ReduceFromPredicate(ReduceBySignature):

    adapts(FromPredicateSig)

    def __call__(self):
        #op = self.state.reduce(self.phrase.op)
        #if isformula(op, ToPredicateSig):
        #    return op.op
        #return self.phrase.clone(is_nullable=op.is_nullable, op=op)
        return self.state.reduce(self.phrase.op)


class ReduceToPredicate(ReduceBySignature):

    adapts(ToPredicateSig)

    def __call__(self):
        #op = self.state.reduce(self.phrase.op)
        #if isformula(op, FromPredicateSig):
        #    return op.op
        #return self.phrase.clone(is_nullable=op.is_nullable, op=op)
        return self.state.reduce(self.phrase.op)


class ReduceExport(Reduce):
    """
    Reduces an export phrase.
    """

    adapts(ExportPhrase)

    def __call__(self):
        # The default implementation (used for columns and embeddings)
        # is no-op.
        return self.phrase


class ReduceReference(Reduce):
    """
    Reduce a reference phrase.
    """

    adapts(ReferencePhrase)

    def __call__(self):
        # Reduce an export reference: if the reference points to
        # a collapsed frame, replace the reference with the referenced
        # phrase.
        if self.phrase.tag not in self.state.substitutes:
            return self.phrase
        select = self.state.substitutes[self.phrase.tag]
        phrase = select[self.phrase.index]
        # Return a (reduced) referenced phrase.
        return self.state.reduce(phrase)


def reduce(clause, state=None):
    """
    Reduces (simplifies) a SQL clause.

    Returns an equivalent (possibly the same) clause.

    `clause` (:class:`htsql.tr.frame.Clause`)
        The clause to simplify.

    `state` (:class:`ReducingState` or ``None``)
        The reducing state to use.  If not set, a new reducing state
        is instantiated.
    """
    # Instantiate a new reducing state if necessary.
    if state is None:
        state = ReducingState()
    # Realize and apply the `Reduce` adapter.
    reduce = Reduce(clause, state)
    return reduce()


