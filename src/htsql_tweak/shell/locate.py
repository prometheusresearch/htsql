#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.adapter import named
from ..resource.locate import LocatePackage, LocateRemote


class LocateShell(LocatePackage):

    named('/shell/')
    package = __name__
    directory = 'static'


class LocateJQuery(LocateRemote):

    named('/shell/external/jquery/')
    url = 'http://code.jquery.com/jquery-1.6.4.min.js'
    md5 = '9118381924c51c89d9414a311ec9c97f'
    cache = 'jquery-1.6.4'


class LocateCodeMirror(LocateRemote):

    named('/shell/external/codemirror/')
    url = 'https://nodeload.github.com/marijnh/CodeMirror2/zipball/v2.13'
    md5 = 'ba0a4838ecb469ed40dfc43ce042fe67'
    cache = 'codemirror-2.13'
    is_zip = True


