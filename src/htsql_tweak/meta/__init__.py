#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from . import command
from htsql.addon import Addon


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


