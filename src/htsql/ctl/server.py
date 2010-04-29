#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


from .error import ScriptError
from .engine import EngineRoutine
from .routine import Argument
from ..validator import StrVal, PIntVal


class ServerRoutine(EngineRoutine):

    name = 'server'
    aliases = ['serve', 's']
    arguments = EngineRoutine.arguments + [
            Argument('host', StrVal(), ''),
            Argument('port', PIntVal(), 8080),
    ]
    hint = u"""start an HTTP server running an HTSQL application"""


