#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql.addon`
==================

This module declares HTSQL addons.
"""


class Parameter(object):

    def __init__(self, attribute, validator, default=None,
                 value_name=None, hint=None):
        assert isinstance(attribute, str)
        assert re.match(r'^[a-zA-Z_][0-9a-zA-Z_]*$', attribute)
        assert isinstance(validator, Validator)
        assert isinstance(value_name, maybe(str))
        assert isinstance(hint, maybe(str))
        self.attribute = attribute
        self.validator = validator
        self.default = default
        self.value_name = value_name
        self.hint = hint

    def get_hint(self):
        """
        Returns a short one-line description of the parameter.
        """
        return self.hint

    def get_signature(self):
        """
        Returns the parameter signature.
        """
        # Parameter signature has the form:
        #   {attribute}={VALUE_NAME}
        attribute = self.attribute.replace('_', '-')
        value_name = self.value_name
        if value_name is None:
            value_name = signature
        value_name = value_name.upper()
        return "%s=%s" % (attribute, value_name)


class Addon(object):
    """
    Implements an addon for HTSQL applications.

    This is an abstract class; to add a new addon, create a subclass
    of :class:`Addon`.
    """

    name = None
    parameters = []
    hint = None
    help = None

    @classmethod
    def get_hint(cls):
        """
        Returns a short one-line description of the addon.
        """
        return self.hint

    @classmethod
    def get_help(cls):
        """
        Returns a long description of the addon.
        """
        if cls.help is None:
            return None
        return trim_doc(cls.help)

    def __init__(self, attributes):
        for name in attributes:
            setattr(self, name, attributes[name])


