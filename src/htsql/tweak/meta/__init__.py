#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import command
from ...core.addon import Addon


class TweakMetaAddon(Addon):

    name = 'tweak.meta'
    hint = """add support for meta database"""
    help = """
    This addon provides a meta database, which describes tables,
    columns and links of the primary database.

    The meta database contains the following tables:

    - `table`: available tables;
    - `field`: table fields including columns and links;
    - `column`: table columns;
    - `link`: links between tables.

    Use function `meta()` to make queries against the meta database.
    For example, to get a list of all tables, run the query:

        /table/:meta
    """

    prerequisites = []


