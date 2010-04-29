#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# Authors: Kirill Simonov <xi@gamma.dn.ua>,
#          Clark C. Evans <cce@clarkevans.com>
#


from .error import ScriptError
from .engine import EngineRoutine
from .routine import Argument
from .option import OutputOption, RemoteUserOption
from ..validator import StrVal, PIntVal


class GetRoutine(EngineRoutine):

    name = 'get'
    arguments = EngineRoutine.arguments + [
            Argument('query', StrVal()),
    ]
    options = EngineRoutine.options + [
            OutputOption,
            RemoteUserOption,
    ]
    hint = u"""execute an HTSQL query"""


