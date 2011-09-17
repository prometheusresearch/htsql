#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql import __version__
from htsql.util import maybe
from htsql.context import context
from htsql.adapter import Adapter, adapts, named
from htsql.error import HTTPError
from htsql.domain import (Domain, BooleanDomain, NumberDomain, DateTimeDomain)
from htsql.cmd.command import UniversalCmd, Command
from htsql.cmd.act import (Act, RenderAction, UnsupportedActionError,
                           produce, safe_produce, analyze)
from htsql.tr.error import TranslateError
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

    def __init__(self, query=None, is_implicit=False):
        assert isinstance(query, maybe(str))
        assert isinstance(is_implicit, bool)
        self.query = query
        self.is_implicit = is_implicit


class EvaluateCmd(Command):

    def __init__(self, query, action=None, page=None):
        assert isinstance(query, str)
        assert isinstance(action, maybe(str))
        assert isinstance(page, maybe(int))
        self.query = query
        self.action = action
        self.page = page


class ShellSig(Signature):

    slots = [
            Slot('query', is_mandatory=False),
    ]


class EvaluateSig(Signature):

    slots = [
            Slot('query'),
            Slot('action', is_mandatory=False),
            Slot('page', is_mandatory=False),
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

    def expand(self, query, action, page):
        if not isinstance(query, StringSyntax):
            raise BindError("a string literal is required", query.mark)
        query = query.value
        if action is not None:
            if not isinstance(action, StringSyntax):
                raise BindError("a string literal is required", action.mark)
            if action.value not in ['produce', 'analyze']:
                raise BindError("'produce' or 'analyze' is expected",
                                action.mark)
            action = action.value
        if page is not None:
            if not isinstance(page, NumberSyntax) and page.is_integer:
                raise BindError("an integer literal is required", page.mark)
            page = int(page.value)
        command = EvaluateCmd(query, action, page)
        return CommandBinding(self.state.scope, command, self.syntax)


class RenderShell(Act):

    adapts(ShellCmd, RenderAction)

    def __call__(self):
        query = self.command.query
        resource = locate('/shell/index.html')
        assert resource is not None
        database_name = context.app.htsql.db.database
        server_name = 'HTSQL ' + __version__
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
        implicit = str(self.command.is_implicit).lower()
        data = resource.data
        data = self.patch(data, 'base href', resource_root)
        data = self.patch(data, 'data-database-name', database_name)
        data = self.patch(data, 'data-server-name', server_name)
        data = self.patch(data, 'data-server-root', server_root)
        data = self.patch(data, 'data-query-on-start', query_on_start)
        data = self.patch(data, 'data-evaluate-on-start', evaluate_on_start)
        data = self.patch(data, 'data-implicit-shell', implicit)
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
        addon = context.app.tweak.shell
        status = "200 OK"
        headers = [('Content-Type', 'application/javascript')]
        command = UniversalCmd(self.command.query)
        limit = None
        try:
            if self.command.action == 'analyze':
                plan = analyze(command)
            else:
                page = self.command.page
                if page is not None and page > 0 and addon.limit is not None:
                    limit = page*addon.limit
                if limit is not None:
                    product = safe_produce(command, limit+1)
                else:
                    product = produce(command)
        except UnsupportedActionError, exc:
            body = self.render_unsupported(exc)
        except HTTPError, exc:
            body = self.render_error(exc)
        else:
            if self.command.action == 'analyze':
                body = self.render_sql(plan)
            else:
                if product:
                    body = self.render_product(product, limit)
                else:
                    body = self.render_empty()
        return (status, headers, body)

    def render_unsupported(self, exc):
        yield "{\n"
        yield "  \"type\": \"unsupported\"\n"
        yield "}\n"

    def render_error(self, exc):
        detail = exc.detail
        first_line = 'null'
        first_column = 'null'
        last_line = 'null'
        last_column = 'null'
        if isinstance(exc, TranslateError) and exc.mark.input:
            mark = exc.mark
            first_break = mark.input.rfind('\n', 0, mark.start)+1
            last_break = mark.input.rfind('\n', 0, mark.end)+1
            first_line = mark.input.count('\n', 0, first_break)
            last_line = mark.input.count('\n', 0, last_break)
            first_column = mark.start-first_break
            last_column = mark.end-last_break
        yield "{\n"
        yield "  \"type\": \"error\",\n"
        yield "  \"detail\": %s,\n" % escape(cgi.escape(detail))
        yield "  \"first_line\": %s,\n" % first_line
        yield "  \"first_column\": %s,\n" % first_column
        yield "  \"last_line\": %s,\n" % last_line
        yield "  \"last_column\": %s\n" % last_column
        yield "}\n"

    def render_product(self, product, limit):
        style = self.make_style(product)
        head = self.make_head(product)
        body = self.make_body(product, limit)
        more = self.make_more(product, limit)
        yield "{\n"
        yield "  \"type\": \"product\",\n"
        yield "  \"style\": %s,\n" % style
        yield "  \"head\": %s,\n" % head
        yield "  \"body\": %s,\n" % body
        yield "  \"more\": %s\n" % more
        yield "}\n"

    def render_empty(self):
        yield "{\n"
        yield "  \"type\": \"empty\"\n"
        yield "}\n"

    def render_sql(self, plan):
        sql = plan.sql
        if not sql:
            sql = ''
        yield "{\n"
        yield "  \"type\": \"sql\",\n"
        yield "  \"sql\": %s\n" % escape(cgi.escape(sql))
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

    def make_body(self, product, limit):
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
        if limit is not None:
            rows = rows[:limit]
        return "[%s]" % ", ".join(rows)

    def make_more(self, product, limit):
        if limit is not None and len(product.records) >= limit:
            return "true"
        return "false"


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


