#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import maybe
from htsql.adapter import Adapter, adapts, named
from htsql.error import HTTPError
from htsql.domain import (Domain, BooleanDomain, NumberDomain, DateTimeDomain)
from htsql.cmd.command import UniversalCmd, Command
from htsql.cmd.act import Act, RenderAction, UnsupportedActionError, produce
from htsql.tr.syntax import StringSyntax, NumberSyntax
from htsql.tr.binding import CommandBinding
from htsql.tr.signature import Signature, Slot
from htsql.tr.error import BindError
from htsql.tr.fn.bind import BindCommand
from htsql.fmt.entitle import guess_title
from htsql.fmt.json import escape
import cgi


class EvaluateCmd(Command):

    def __init__(self, query, limit=None):
        assert isinstance(query, str)
        assert isinstance(limit, maybe(int))
        self.query = query
        self.limit = limit


class EvaluateSig(Signature):

    slots = [
            Slot('query'),
            Slot('limit', is_mandatory=False),
    ]


class BindEvaluate(BindCommand):

    named('evaluate')
    signature = EvaluateSig

    def expand(self, query, limit):
        if not isinstance(query, StringSyntax):
            raise BindError("a string literal is required", query.mark)
        query = query.value
        if limit is not None:
            if not isinstance(limit, NumberSyntax) and limit.is_integer:
                raise BindError("an integer literal is required", limit.mark)
            limit = int(limit.value)
        command = EvaluateCmd(query, limit)
        return CommandBinding(self.state.scope, command, self.syntax)


class RenderEvaluate(Act):

    adapts(EvaluateCmd, RenderAction)

    def __call__(self):
        status = "200 OK"
        headers = [('Content-Type', 'application/javascript')]
        try:
            product = self.evaluate()
        except UnsupportedActionError, exc:
            body = self.render_unsupported(exc)
        except HTTPError, exc:
            body = self.render_error(exc)
        else:
            if product:
                body = self.render_product(product)
            else:
                body = self.render_empty()
        return (status, headers, body)

    def evaluate(self):
        command = UniversalCmd(self.command.query)
        return produce(command)

    def render_unsupported(self, exc):
        yield "{\n"
        yield "  \"type\": \"unsupported\"\n"
        yield "}\n"

    def render_error(self, exc):
        yield "{\n"
        yield "  \"type\": \"error\",\n"
        yield "  \"message\": %s\n" % escape(cgi.escape(str(exc)))
        yield "}\n"

    def render_product(self, product):
        style = self.make_style(product)
        head = self.make_head(product)
        body = self.make_body(product)
        yield "{\n"
        yield "  \"type\": \"product\",\n"
        yield "  \"style\": %s,\n" % style
        yield "  \"head\": %s,\n" % head
        yield "  \"body\": %s\n" % body
        yield "}\n"

    def render_empty(self):
        yield "{\n"
        yield "  \"type\": \"empty\"\n"
        yield "}\n"

    def make_style(self, product):
        domains = [element.domain
                   for element in product.profile.segment.elements]
        styles = [get_style(domain) for domain in domains]
        return "[%s]" % ", ".join((escape(cgi.escape(style))
                                   if style is not None else "null")
                                  for style in styles)

    def make_head(self, product):
        rows = []
        headers = [guess_title(element.binding)
                   for element in product.profile.segment.elements]
        height = max(len(header) for header in headers)
        width = len(product.profile.segment.elements)
        for line in range(height):
            cells = []
            idx = 0
            while idx < width:
                while idx < width and len(headers[idx]) <= line:
                    idx += 1
                if idx == width:
                    break
                is_spanning = (len(headers[idx]) > line+1)
                colspan = 1
                if is_spanning:
                    while (idx+colspan < width and
                           len(headers[idx+colspan]) > line+1 and
                           headers[idx][:line+1] ==
                               headers[idx+colspan][:line+1]):
                        colspan += 1
                rowspan = 1
                if len(headers[idx]) == line+1:
                    rowspan = height-line
                title = escape(cgi.escape(headers[idx][line]))
                cell = "[%s, %s, %s]" % (title, colspan, rowspan)
                cells.append(cell)
                idx += colspan
            rows.append("[%s]" % ", ".join(cells))
        return "[%s]" % ", ".join(rows)

    def make_body(self, product):
        rows = []
        domains = [element.domain
                   for element in product.profile.segment.elements]
        formats = [get_format(domain) for domain in domains]
        for record in product:
            cells = []
            for value, format in zip(record, formats):
                if value is None:
                    cell = "null"
                else:
                    cell = escape(cgi.escape(format(value)))
                cells.append(cell)
            rows.append("[%s]" % ", ".join(cells))
        return "[%s]" % ", ".join(rows)


class GetStyle(Adapter):

    adapts(Domain)

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return None


class GetStyleForBoolean(GetStyle):

    adapts(BooleanDomain)

    def __call__(self):
        return "boolean"


class GetStyleForNumber(GetStyle):

    adapts(NumberDomain)

    def __call__(self):
        return "number"


class GetFormat(Adapter):

    adapts(Domain)

    @staticmethod
    def format(value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        else:
            value = str(value)
        try:
            value.decode('utf-8')
        except UnicodeDecodeError:
            value = repr(value)
        return value

    def __init__(self, domain):
        self.domain = domain

    def __call__(self):
        return self.format


class GetFormatForBoolean(GetFormat):

    adapts(BooleanDomain)

    @staticmethod
    def format(value):
        if value is True:
            return "true"
        if value is False:
            return "false"


class GetFormatForDateTime(GetFormat):

    adapts(DateTimeDomain)

    @staticmethod
    def format(value):
        if not value.time():
            return str(value.date())
        return str(value)


def get_style(domain):
    get_style = GetStyle(domain)
    return get_style()


def get_format(domain):
    get_format = GetFormat(domain)
    return get_format()


