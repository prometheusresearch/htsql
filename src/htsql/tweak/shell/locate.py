#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.adapter import call
from ..resource.locate import LocatePackage


class LocateShell(LocatePackage):

    call('/shell/')
    package = __name__
    directory = 'static'


class LocateJQuery(LocatePackage):

    call('/shell/vendor/jquery/')
    package = __name__
    directory = 'vendor/jquery-1.6.4'


class LocateCodeMirror(LocatePackage):

    call('/shell/vendor/codemirror/')
    package = __name__
    directory = 'vendor/codemirror-2.13'


