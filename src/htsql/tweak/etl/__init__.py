#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ...core.addon import Addon, addon_registry
from . import cmd, tr


class TweakETLAddon(Addon):

    name = 'tweak.etl'
    hint = """ETL and CRUD commands"""
    help = """
    The extension provides the following commands:

    `insert(feed)` adds records to a table.

    `update(feed)` updates table records.

    `merge(feed)` adds or updates records in a table.

    `delete(feed)` deletes records from a table.

    `truncate(name)` truncates a table.

    `do(command, ...)` performs a series of command in a single
    transaction.
    """
    packages = ['.', '.cmd', '.tr']

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                #raise ImportError("%s is not implemented for %s"
                #                  % (cls.name, app.htsql.db.engine))
                return
            return name


