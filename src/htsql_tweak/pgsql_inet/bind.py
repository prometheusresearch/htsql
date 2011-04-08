#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


from htsql.adapter import named, adapts
from htsql.tr.fn.bind import BindCast, ComparableDomains
from .domain import INetDomain


class BindINetCast(BindCast):

    named('inet')
    codomain = INetDomain()


class ComparableINet(ComparableDomains):

    adapts(INetDomain)


