#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import bind, coerce, encode, domain, signature
from ...core.addon import Addon, addon_registry


class TweakINetAddon(Addon):

    name = 'tweak.inet'
    hint = """add support for IPv4 data type"""
    help = """
    This addon adds support for IPv4 data type.  The addon implements:

    - `inet()` constructor which accepts literals in standard IPv4
      dotted notation;
    - standard comparison operators for `inet` values;
    - conversion of `inet` values from/to `string` and `integer` values;
    - arithmetic operations: `inet-integer`, `inet+integer`,
      `inet-inet`.

    Currently, only PostgreSQL backend is supported.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                raise ImportError("%s is not implemented for %s"
                                  % (cls.name, app.htsql.db.engine))
            return name


