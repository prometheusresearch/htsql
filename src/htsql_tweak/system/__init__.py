#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.addon import Addon, addon_registry


class TweakSystemAddon(Addon):

    name = 'tweak.system'
    hint = """add access to system tables"""
    help = """
    This addon adds access to system catalog tables.

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


