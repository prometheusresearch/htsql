#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.core.tr.signature`
==============================

This module defines formula nodes and formula signatures.
"""


from ..util import maybe, listof, Comparable, Clonable, Printable


class Slot(object):
    """
    Represents a formula slot.

    A slot is a parameter of a formula.  A slot is to be filled
    with an argument value when a formula node is instantiated.

    `name` (a string)
        The name of the argument.

    `is_mandatory` (Boolean)
        Indicates that the slot requires at least one value.

    `is_singular` (Boolean)
        Indicates that the slot accepts no more than one value.
    """

    def __init__(self, name, is_mandatory=True, is_singular=True):
        # Sanity check on the arguments.
        assert isinstance(name, str) and len(name) > 0
        assert isinstance(is_mandatory, bool)
        assert isinstance(is_singular, bool)

        self.name = name
        self.is_mandatory = is_mandatory
        self.is_singular = is_singular


class Signature(Comparable, Clonable, Printable):
    """
    Represents a formula signature.

    A signature identifies the type of a formula.  In particular,
    a signature describes all slots of the formula.

    Class attributes:

    `slots` (a list of :class:`Slot`)
        The formula slots.
    """

    # Override in subclasses.
    slots = []

    def __init__(self):
        pass

    def __basis__(self):
        return ()

    def __str__(self):
        return self.__class__.__name__


class Bag(dict):
    """
    Encapsulates formula arguments.

    `arguments` (a dictionary)
        Maps slot names to argument values.

        Depending on the slot type, a value could be one of:
        - a node or ``None`` for singular slots;
        - a list of nodes for plural slots.

        A missing argument is indicated by ``None`` for a singular
        slot or by an empty list for a plural slot.  Missing
        arguments are not allowed for mandatory slots.

    :class:`Bag` provides a mapping interface to `arguments`.
    """

    # FIXME: respect the order of slots in the signature.

    def __init__(self, **arguments):
        # Initialize the underlying dictionary.
        self.update(arguments)

    def admits(self, kind, signature):
        """
        Verifies that the arguments match the given signature.

        Returns ``True`` if the arguments match the given signature,
        ``False`` otherwise.

        `kind` (a type)
            The expected type of value nodes.

        `signature` (:class:`Signature`)
            The expected signature of the arguments.
        """
        # Sanity check on the arguments.
        assert isinstance(kind, type)
        assert (isinstance(signature, Signature) or
                issubclass(signature, Signature))

        # Verify that the arguments match the slot names.
        if set(self.keys()) != set(slot.name for slot in signature.slots):
            return False

        # Check every slot.
        for slot in signature.slots:
            # The argument.
            value = self[slot.name]

            # A value of a singular slot must be a node of the given
            # type or ``None``; a value equal to ``None`` is allowed
            # only for optional slots.
            if slot.is_singular:
                if not isinstance(value, maybe(kind)):
                    return False
                if slot.is_mandatory:
                    if value is None:
                        return False
            # A value of a plural slot must be a list of nodes of the
            # given type and, unless the slot is optional, must contain
            # at least one node.
            else:
                if not isinstance(value, listof(kind)):
                    return False
                if slot.is_mandatory:
                    if not value:
                        return False

        # All checks passed.
        return True

    def cells(self):
        """
        Returns a list of all subnodes.

        This function extracts all (singular) nodes from the arguments.
        """
        # A list of nodes.
        cells = []
        # Iterate over all the arguments.
        for key in sorted(self.keys()):
            # A value: could be ``None``, a node or a list of nodes.
            value = self[key]
            if value is not None:
                if isinstance(value, list):
                    cells.extend(value)
                else:
                    cells.append(value)
        return cells

    def impress(self, owner):
        """
        Adds the arguments as attributes to the given object.

        `owner` (a node object)
            An object to update.
        """
        # Iterate through all the arguments.
        for key in sorted(self.keys()):
            # Make sure we do not override an existing attribute.
            assert not hasattr(owner, key)
            # Impress the argument to the object.
            setattr(owner, key, self[key])

    def map(self, method):
        """
        Applies the given function to all subnodes.

        Returns a new :class:`Bag` instance of the same shape
        composed from the results of the `method` application
        to every value node.

        `method` (a callable)
            A function to apply.
        """
        # The result of the `method` applications.
        arguments = {}
        # Iterate over all the arguments.
        for key in sorted(self.keys()):
            # An argument value: `None`, a node, or a list of nodes.
            value = self[key]
            # Apply `method` to `value`.
            if value is not None:
                if isinstance(value, list):
                    value = [method(item) for item in value]
                else:
                    value = method(value)
            arguments[key] = value
        # Produce a new `Bag` instance with updated arguments.
        return self.__class__(**arguments)

    def freeze(self):
        """
        Returns an immutable container with all the argument values.

        This function is useful for constructing an equality vector
        of a formula node.
        """
        # An ordered list of the (frozen) argument values.
        values = []
        # Iterate over the arguments; freeze mutable objects
        # (i.e., convert a list to a tuple).
        for key in sorted(self.keys()):
            value = self[key]
            if isinstance(value, list):
                value = tuple(value)
            values.append(value)
        # Finally freeze and return the list itself.
        return tuple(values)


class Formula(Printable):
    """
    Represents a formula node.

    This is a mixin class; it is mixed with
    :class:`htsql.core.tr.binding.Binding`, :class:`htsql.core.tr.flow.Code`
    and :class:`htsql.core.tr.frame.Phrase` to produce respective formula node
    types.

    `signature` (:class:`Signature`)
        The formula signature.

    `arguments` (:class:`Bag`)
        The formula arguments; must be compatible with the signature.

    The rest of the arguments are passed to the next base class constructor
    unchanged.
    """

    def __init__(self, signature, arguments, *args, **kwds):
        assert isinstance(signature, Signature)
        # The caller is responsible for checking that the arguments
        # are compatible with the signature.
        assert isinstance(arguments, Bag)
        super(Formula, self).__init__(*args, **kwds)
        self.signature = signature
        self.arguments = arguments
        # Add an attribute for each argument.
        arguments.impress(self)

    def __str__(self):
        # Display:
        #   Signature: ...
        return "%s: %s" % (self.signature, super(Formula, self).__str__())


def isformula(formula, signatures):
    """
    Checks if a node is a formula with the given signature.

    The function returns ``True`` if the given node is a formula
    and its signature is a subclass of the given signature class;
    ``False`` otherwise.

    `formula` (a node, possibly a :class:`Formula` node)
        A node to check.

    `signatures` (a subclass or a tuple of subclasses of :class:`Signature`)
        The expected formula signature(s).
    """
    # Normalize the signatures.
    if not isinstance(signatures, tuple):
        signatures = (signatures,)
    # Check that the given node is, indeed, a formula, and that
    # its signature is among the given signature classes.
    return (isinstance(formula, Formula) and
            any(isinstance(formula.signature, signature)
                for signature in signatures))


class NullarySig(Signature):
    """
    Represents a signature with no slots.
    """

    slots = []


class UnarySig(Signature):
    """
    Represents a signature with one singular slot.
    """

    slots = [
            Slot('op'),
    ]


class BinarySig(Signature):
    """
    Represents a signature with two singular slots.
    """

    slots = [
            Slot('lop'),
            Slot('rop'),
    ]


class NArySig(Signature):
    """
    Represents a signature with one singular slot and one plural slot.
    """

    slots = [
            Slot('lop'),
            Slot('rops', is_singular=False),
    ]


class ConnectiveSig(Signature):
    """
    Represents a signature with one plural slot.
    """

    slots = [
            Slot('ops', is_singular=False),
    ]


class PolarSig(Signature):
    """
    Denotes a formula with two forms: positive and negative.

    `polarity` (``+1`` or ``-1``)
        Indicates the form of the formula: ``+1`` for positive,
        ``-1`` for negative.
    """

    def __init__(self, polarity):
        assert polarity in [+1, -1]
        self.polarity = polarity

    def __basis__(self):
        return (self.polarity,)

    def reverse(self):
        """
        Returns the signature with the opposite polarity.
        """
        return self.clone(polarity=-self.polarity)

    def __str__(self):
        # Display:
        #   Signature(+/-)
        return "%s(%s)" % (self.__class__.__name__,
                           '+' if self.polarity > 0 else '-')


class IsEqualSig(BinarySig, PolarSig):
    """
    Denotes an equality (``=``) and an inequality (``!=``) operator.
    """


class IsTotallyEqualSig(BinarySig, PolarSig):
    """
    Denotes a total equality (``==`` and ``!==``) operator.
    """


class IsInSig(NArySig, PolarSig):
    """
    Denotes an N-ary equality (``={}`` and ``!={}``) operator.
    """


class IsNullSig(UnarySig, PolarSig):
    """
    Denotes an ``is_null()`` operator.
    """


class IfNullSig(BinarySig):
    """
    Denotes an ``if_null()`` operator.
    """


class NullIfSig(BinarySig):
    """
    Denotes a ``null_if()`` operator.
    """


class CompareSig(BinarySig):
    """
    Denotes a comparison operator.

    `relation` (one of: ``'<'``, ``'<='``, ``'>'``, ``'>='``)
        Indicates the comparison relation.
    """

    def __init__(self, relation):
        assert relation in ['<', '<=', '>', '>=']
        self.relation = relation

    def __basis__(self):
        return (self.relation,)


class AndSig(ConnectiveSig):
    """
    Denotes a Boolean "AND" (``&``) operator.
    """


class OrSig(ConnectiveSig):
    """
    Denotes a Boolean "OR" (``|``) operator.
    """


class NotSig(UnarySig):
    """
    Denotes a Boolean "NOT" (``!``) operator.
    """


class SortDirectionSig(Signature):

    slots = [
            Slot('base'),
    ]

    def __init__(self, direction):
        assert direction in [+1, -1]
        self.direction = direction

    def __basis__(self):
        return (self.direction,)


class RowNumberSig(Signature):

    slots = [
            Slot('partition', is_mandatory=False, is_singular=False),
            Slot('order', is_mandatory=False, is_singular=False),
    ]


class ToPredicateSig(UnarySig):
    pass


class FromPredicateSig(UnarySig):
    pass


