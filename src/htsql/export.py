#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.export`
===================

This module exports the `htsql.core` addon.
"""


from addon import Addon
from wsgi import wsgi_adapters
from connect import connect_adapters
from split_sql import split_sql_adapters
from introspect import introspect_adapters
from translate import translate_adapters
from produce import produce_adapters
from render import render_adapters
from tr.binder import bind_adapters
from tr.lookup import lookup_adapters
from tr.encoder import encode_adapters
from tr.assembler import assemble_adapters
from tr.outliner import outline_adapters
from tr.compiler import compile_adapters
from tr.serializer import serialize_adapters


class HTSQL_CORE(Addon):
    """
    Declares the `htsql.core` addon.
    """

    # List of adapters exported by the addon.
    adapters = (wsgi_adapters +
                connect_adapters +
                split_sql_adapters +
                introspect_adapters +
                bind_adapters +
                lookup_adapters +
                encode_adapters +
                assemble_adapters +
                outline_adapters +
                compile_adapters +
                serialize_adapters +
                translate_adapters +
                produce_adapters +
                render_adapters)


