#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.jsonex`
=======================

This module implements the JSON renderer with extra metadata.
"""


from ..adapter import Adapter, adapts
from ..domain import (Domain, BooleanDomain, NumberDomain, StringDomain,
                      EnumDomain, DateDomain)
from .format import Format
from .json import JSONRenderer, JSONFormatter, escape
from .entitle import entitle


class JSONExRenderer(JSONRenderer):

    name = 'jsonex'
    aliases = []

    def generate_body(self, product):
        titles = [escape(entitle(element.binding))
                  for element in product.profile.segment.elements]
        domains = [element.domain
                   for element in product.profile.segment.elements]
        domain_titles = [escape(entitle_domain(domain)) for domain in domains]
        tool = JSONFormatter(self)
        formats = [Format(self, domain, tool) for domain in domains]
        yield "{\n"
        yield "  \"meta\": [\n"
        items = []
        for title, domain_title in zip(titles, domain_titles):
            item = "\"title\": %s, \"domain\": %s" % (title, domain_title)
            items.append(item)
        if items:
            for item in items[:-1]:
                yield "    {%s},\n" % item
            yield "    {%s}\n" % items[-1]
        yield "  ],\n"
        yield "  \"data\": [\n"
        items = None
        for record in product:
            if items is not None:
                yield "    [%s],\n" % ", ".join(items)
            items = [format(value)
                     for format, value in zip(formats, record)]
        if items is not None:
            yield "    [%s]\n" % ", ".join(items)
        yield "  ]\n"
        yield "}\n"


class EntitleDomain(Adapter):

    adapts(Domain)
    name = "unknown"

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.name


class EntitleBoolean(EntitleDomain):

    adapts(BooleanDomain)
    name = "boolean"


class EntitleNumber(EntitleDomain):

    adapts(NumberDomain)
    name = "number"


class EntitleString(EntitleDomain):

    adapts(StringDomain)
    name = "string"


class EntitleEnum(EntitleDomain):

    adapts(EnumDomain)
    name = "enum"


class EntitleDate(EntitleDomain):

    adapts(DateDomain)
    name = "date"


def entitle_domain(domain):
    entitle = EntitleDomain(domain)
    return entitle()


