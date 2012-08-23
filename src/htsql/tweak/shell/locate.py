#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.adapter import call
from ..resource.locate import LocatePackage, LocateRemote


class LocateShell(LocatePackage):

    call('/shell/')
    package = __name__
    directory = 'static'


class LocateJQuery(LocateRemote):

    call('/shell/external/jquery/')
    url = 'http://code.jquery.com/jquery-1.6.4.min.js'
    md5 = '9118381924c51c89d9414a311ec9c97f'
    cache = 'jquery-1.6.4'


class LocateCodeMirror(LocateRemote):

    call('/shell/external/codemirror/')
    url = 'https://nodeload.github.com/marijnh/CodeMirror/zipball/v2.13'
    md5 = 'b2a4f989ba45f1778b183603f78cf883'
    cache = 'codemirror-2.13'
    is_zip = True


