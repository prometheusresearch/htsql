#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.util import maybe
from htsql.context import context
from htsql.adapter import Adapter, adapts, named
from htsql.error import HTTPError
from htsql.domain import (Domain, BooleanDomain, NumberDomain, DateTimeDomain)
from htsql.cmd.command import UniversalCmd, Command
from htsql.cmd.act import Act, RenderAction, UnsupportedActionError, produce
from htsql.tr.syntax import StringSyntax, NumberSyntax, SegmentSyntax
from htsql.tr.binding import CommandBinding
from htsql.tr.signature import Signature, Slot
from htsql.tr.error import BindError
from htsql.tr.fn.bind import BindCommand
from htsql.fmt.entitle import guess_title
from htsql.fmt.json import escape
from ..resource.locate import locate
import re
import cgi
import wsgiref.util


class ShellCmd(Command):

    def __init__(self, query=None):
        assert isinstance(query, maybe(str))
        self.query = query


class EvaluateCmd(Command):

    def __init__(self, query, limit=None):
        assert isinstance(query, str)
        assert isinstance(limit, maybe(int))
        self.query = query
        self.limit = limit


class ShellSig(Signature):

    slots = [
            Slot('query', is_mandatory=False),
    ]


class EvaluateSig(Signature):

    slots = [
            Slot('query'),
            Slot('limit', is_mandatory=False),
    ]


class BindShell(BindCommand):

    named('shell')
    signature = ShellSig

    def expand(self, query):
        if query is not None:
            if isinstance(query, StringSyntax):
                query = query.value
            elif isinstance(query, SegmentSyntax):
                query = str(query)
            else:
                raise BindError("a query is required", query.mark)
        command = ShellCmd(query)
        return CommandBinding(self.state.scope, command, self.syntax)


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


class RenderShell(Act):

    adapts(ShellCmd, RenderAction)

    def __call__(self):
        query = self.command.query
        resource = locate('/shell/index.html')
        assert resource is not None
        database_name = context.app.htsql.db.database
        server_root = context.app.tweak.shell.server_root
        if server_root is None:
            server_root = wsgiref.util.application_uri(self.action.environ)
        if server_root.endswith('/'):
            server_root = server_root[:-1]
        resource_root = (server_root + '/%s/shell/'
                         % context.app.tweak.resource.indicator)
        if query is not None and query not in ['', '/']:
            query_on_start = query
            evaluate_on_start = 'true'
        else:
            query_on_start = '/'
            evaluate_on_start = 'false'
        data = resource.data
        data = self.patch(data, 'base href', resource_root)
        data = self.patch(data, 'data-database-name', database_name)
        data = self.patch(data, 'data-server-root', server_root)
        data = self.patch(data, 'data-query-on-start', query_on_start)
        data = self.patch(data, 'data-evaluate-on-start', evaluate_on_start)
        status = '200 OK'
        headers = [('Content-Type', 'text/html; charset=UTF-8')]
        body = [data]
        return (status, headers, body)

    def patch(self, data, prefix, value):
        pattern = prefix + r'="[^"]*"'
        replacement = prefix + '="%s"' % cgi.escape(value, True)
        data, count = re.subn(pattern, replacement, data, 1)
        assert count == 1
        return data


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


