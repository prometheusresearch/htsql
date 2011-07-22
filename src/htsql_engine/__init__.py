#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_engine`
===================

This package contains adapters for various database engines.
"""


from htsql.addon import Addon


class EngineAddon(Addon):

    name = 'engine'
    hint = """provides implementations of HTSQL for specific servers"""
    help = """
    This extension implements HTSQL translator for specific
    database servers.
    """

    @classmethod
    def get_extension(cls, app, attributes):
        return 'engine.%s' % app.htsql.db.engine

