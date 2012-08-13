#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.addon import Addon, addon_registry


class TweakETLAddon(Addon):

    name = 'tweak.etl'
    hint = """ETL (extract-transform-load) operations"""
    help = None

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                #raise ImportError("%s is not implemented for %s"
                #                  % (cls.name, app.htsql.db.engine))
                return
            return name


