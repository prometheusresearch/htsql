#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.addon import Addon, addon_registry


class TweakViewAddon(Addon):

    name = 'tweak.view'
    hint = """add constraints for views"""
    help = """
    This addon attempts to infer foreign keys and other constraints
    from a view definition.

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


