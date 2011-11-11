#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine`
===================

This package contains adapters for various database engines.
"""


from htsql.addon import Addon, addon_registry


class EngineAddon(Addon):

    name = 'engine'
    hint = """provides implementations of HTSQL for specific servers"""
    help = """
    This extension implements HTSQL translator for specific
    database servers.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                raise ImportError("unknown engine %r" % app.htsql.db.engine)
            return name


