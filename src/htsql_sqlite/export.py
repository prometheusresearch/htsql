#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
This module exports the `engine.sqlite` addon.
"""


from htsql.addon import Addon


class ENGINE_SQLITE(Addon):
    """
    Declares the `engine.sqlite` addon.
    """

    adapters = []


