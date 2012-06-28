

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from sphinx.util.osutil import copyfile
from pygments.lexer import RegexLexer
from pygments.token import Punctuation, Text, Operator, Name, String, Number

import re
import os, os.path
from urllib2 import quote, urlopen, Request, HTTPError, URLError
from cgi import escape
from json import loads


class HtsqlLexer(RegexLexer):

    name = 'HTSQL'
    aliases = ['htsql']
    filenames = ['*.htsql']
    mimetypes = ['text/x-htsql', 'application/x-htsql']

    escape_regexp = re.compile(r'%(?P<code>[0-9A-Fa-f]{2})')

    tokens = {
        'root': [
            (r'\s+', Text),
            (r'(?<=:)(?!\d)\w+', Name.Function),
            (r'(?!\d)\w+(?=\s*\()', Name.Function),
            (r'(?!\d)\w+', Name.Builtin),
            (r'(?:\d*\.)?\d+[eE][+-]?\d+|\d*\.\d+|\d+\.?', Number),
            (r'\'(?:[^\']|\'\')*\'', String),
            (r'~|!~|<=|<|>=|>|==|=|!==|!=|!|'
             r'&|\||->|\?|\^|/|\*|\+|-', Operator),
            (r'\.|,|\(|\)|\{|\}|:=|:|\$|@', Punctuation),
            (r'\[', Punctuation, 'locator'),
        ],
        'locator': [
            (r'\s+', Text),
            (r'\(|\[', Punctuation, '#push'),
            (r'\)|\]', Punctuation, '#pop'),
            (r'[\w-]+', String),
            (r'\'(?:[^\']|\'\')*\'', String),
            (r'\.', Punctuation),
        ],
    }

    def get_tokens_unprocessed(self, text):
        octets = text.encode('utf-8')
        quotes = []
        for match in self.escape_regexp.finditer(octets):
            quotes.append(match.start())
        octets = self.escape_regexp.sub(lambda m: chr(int(m.group('code'), 16)),
                                        octets)
        try:
            text = octets.decode('utf-8')
        except UnicodeDecodeError:
            quotes = []
        token_stream = super(HtsqlLexer, self).get_tokens_unprocessed(text)
        pos_inc = 0
        for pos, token, value in token_stream:
            pos += pos_inc
            while quotes and pos <= quotes[0] < pos+len(value):
                idx = quotes.pop(0)-pos
                octets = value[idx].encode('utf-8')
                repl = u''.join(u'%%%02X' % ord(octet) for octet in octets)
                value = value[:idx]+repl+value[idx+1:]
                pos_inc += len(repl)-1
            yield (pos, token, value)


class HTSQLServerDirective(Directive):
    required_arguments = 1
    has_content = False

    def run(self):
        env = self.state.document.settings.env
        env.htsql_server = self.arguments[0]
        return []


class HTSQLDirective(Directive):
    optional_arguments = 1
    has_content = True
    final_argument_whitespace = True
    option_spec = {
            'plain': directives.flag,
            'error': directives.flag,
            'query': directives.path,
            'hide': directives.flag,
            'cut': directives.positive_int,
    }
    htsql_safe = "~`!@$^&*()={[}]|:;\"'<,>?/"

    def run(self):
        doc = self.state.document
        env = doc.settings.env
        if self.arguments:
            if self.content:
                return [doc.reporter.error("directive cannot have both"
                                           " content and an argument",
                                           lineno=self.lineno)]
            query  = " ".join(line.strip()
                              for line in self.arguments[0].split("\n"))
        elif self.content:
            query = "\n".join(self.content).strip()
        else:
            return [doc.reporter.error("directive must have either content"
                                       " or an argument", lineno=self.lineno)]
        query_node = htsql_block(query, query)
        query_node['language'] = 'htsql'
        if not hasattr(env, 'htsql_server') or not env.htsql_server:
            return [doc.reporter.error("config option `htsql_server`"
                                       " is not set", lineno=self.lineno)]
        if 'query' not in self.options:
            query = quote(query.encode('utf-8'), safe=self.htsql_safe)
        else:
            query = self.options['query'].encode('utf-8')
        uri = env.htsql_server+query
        query_node['uri'] = uri
        query_node['hide'] = ('hide' in self.options)
        if not hasattr(env, 'htsql_uris'):
            env.htsql_uris = {}
        if uri not in env.htsql_uris:
            result = load_uri(uri, 'error' in self.options)
            if not result:
                return [doc.reporter.error("failed to load: %s" % uri,
                                           line=self.lineno)]
            env.htsql_uris[uri] = result
        htsql_container = nodes.container(classes=['htsql-io'])
        query_container = nodes.container('', query_node,
                                          classes=['htsql-input'])
        htsql_container += query_container
        if 'hide' in self.options:
            return [htsql_container]
        content_type, content = env.htsql_uris[uri]
        if 'plain' in self.options:
            content_type = 'text/plain'
        result_node = build_result(self.content_offset, content_type, content,
                                   self.options.get('cut'))
        result_container = nodes.container('', result_node,
                                           classes=['htsql-output'])
        htsql_container += result_container
        return [htsql_container]


class VSplitDirective(Directive):

    has_content = True

    def run(self):
        self.assert_has_content()
        text = '\n'.join(self.content)
        node = nodes.container(text, classes=['vsplit'])
        self.state.nested_parse(self.content, self.content_offset, node)
        if len(node) != 2:
            raise self.error("%s directive expects 2 subnodes", self.name)
        node[0]['classes'].append('vsplit-left')
        node[1]['classes'].append('vsplit-right')
        node += nodes.container(classes=['vsplit-clear'])
        return [node]


def purge_htsql_server(app, env, docname):
    if hasattr(env, 'htsql_server'):
        del env.htsql_server
    if env.config.htsql_server:
        env.htsql_server = env.config.htsql_server


class htsql_block(nodes.literal_block):
    pass


def visit_htsql_block(self, node):
    # Adapted from `visit_literal_block()`
    if node.rawsource != node.astext():
        return self.visit_literal_block(self, node)
    lang = self.highlightlang
    linenos = node.rawsource.count('\n') >= \
              self.highlightlinenothreshold - 1
    highlight_args = node.get('highlight_args', {})
    if node.has_key('language'):
        lang = node['language']
        highlight_args['force'] = True
    if node.has_key('linenos'):
        linenos = node['linenos']
    def warner(msg):
        self.builder.warn(msg, (self.builder.current_docname, node.line))
    if hasattr(self.highlighter, 'formatter'):
        self.highlighter.formatter.nowrap = True
        highlighted = self.highlighter.highlight_block(
            node.rawsource, lang, warn=warner, linenos=linenos,
            **highlight_args)
    else:
        self.highlighter.fmter[False].nowrap = True
        self.highlighter.fmter[True].nowrap = True
        highlighted = self.highlighter.highlight_block(
            node.rawsource, lang, linenos, warn=warner)
        self.highlighter.fmter[False].nowrap = False
        self.highlighter.fmter[True].nowrap = False
    if highlighted.startswith('<pre>') and highlighted.endswith('</pre>\n'):
        # may happen if the language is not detected correctly
        highlighted = highlighted[5:-7]
    if node.has_key('uri'):
        highlighted = '<a href="%s" target="_new" class="htsql-link">%s</a>' \
                % (escape(node['uri'], True), highlighted)
        highlighted = '<a href="%s" target="_new" class="htsql-arrow-link">' \
                      '&#x25E5;</a>%s' \
                % (escape(node['uri'], True), highlighted)
    highlighted = '<pre>%s</pre>' % highlighted
    highlighted = '<div class="highlight">%s</div>' % highlighted
    starttag = self.starttag(node, 'div', suffix='',
                             CLASS='highlight-%s' % lang)
    self.body.append(starttag + highlighted + '</div>\n')
    raise nodes.SkipNode


def depart_htsql_block(self, node):
    self.depart_literal_block(node)


def load_uri(uri, error=False):
    try:
        headers = { 'Accept': 'x-htsql/raw' }
        request = Request(uri, headers=headers)
        response = urlopen(request)
        content_type = response.info().gettype()
        content = response.read()
    except HTTPError, response:
        if not error:
            return None
        content_type = response.headers.gettype()
        content = response.read()
    except URLError:
        return None
    return (content_type, content)


def build_result(line, content_type, content, cut=None):
    if content_type == 'application/javascript':
        data = loads(content)
        if isinstance(data, dict):
            if isinstance(data.get('meta'), list):
                return build_result_table_old(data, cut)
            if isinstance(data.get('meta'), dict):
                return build_result_table(data, cut)
    content = content.decode('utf-8', 'replace')
    if cut and content.count('\n') > cut:
        start = 0
        while cut:
            start = content.find('\n', start)+1
            cut -= 1
        content = content[:start]+u"\u2026\n"
    result_node = nodes.literal_block(content, content)
    result_node['language'] = 'text'
    return result_node

def build_result_table_old(data, cut):
    data = [[meta['title'] for meta in data['meta']]] + data['data']
    is_cut = False
    if cut and len(data) > cut+1:
        data = data[:cut+1]
        is_cut = True
    size = len(data[0])
    widths = [1]*size
    for row in data:
        for idx, value in enumerate(row):
            widths[idx] = max(widths[idx], len(unicode(value)))
    table_node = nodes.table()
    group_node = nodes.tgroup(cols=size)
    table_node += group_node
    for width in widths:
        colspec_node = nodes.colspec(colwidth=width)
        group_node += colspec_node
    head_node = nodes.thead()
    group_node += head_node
    row_node = nodes.row()
    head_node += row_node
    for title in data[0]:
        entry_node = nodes.entry()
        row_node += entry_node
        para_node = nodes.paragraph()
        entry_node += para_node
        text_node = nodes.Text(title.replace(u' ', u'\xA0'))
        para_node += text_node
    body_node = nodes.tbody()
    group_node += body_node
    for row in data[1:]:
        row_node = nodes.row()
        body_node += row_node
        for value in row:
            entry_node = nodes.entry()
            row_node += entry_node
            para_node = nodes.paragraph()
            entry_node += para_node
            if value is None:
                text_node = nodes.Text(u"\u2014")
            elif value is True:
                text_node = nodes.emphasis()
                text_node += nodes.Text(u"true")
            elif value is False:
                text_node = nodes.emphasis()
                text_node += nodes.Text(u"false")
            else:
                text_node = nodes.Text(unicode(value))
            para_node += text_node
    if is_cut:
        row_node = nodes.row(classes=['htsql-cut'])
        body_node += row_node
        for idx in range(size):
            entry_node = nodes.entry()
            row_node += entry_node
            para_node = nodes.paragraph()
            entry_node += para_node
            text_node = nodes.Text(u"\u2026")
            para_node += text_node
    return table_node


def build_result_table(result, cut):
    meta = result.get('meta')
    data = result.get('data')
    if 'domain' not in meta:
        return
    build = get_build_by_domain(meta['domain'])
    if not build.span:
        return
    table_node = nodes.table()
    measures = build.measures(data, cut)
    group_node = nodes.tgroup(cols=build.span)
    table_node += group_node
    for measure in measures:
        colspec_node = nodes.colspec(colwidth=measure)
        group_node += colspec_node
    head_node = nodes.thead()
    group_node += head_node
    head_rows = build.head(build.head_height())
    if head_rows:
        for row in head_rows:
            row_node = nodes.row()
            head_node += row_node
            for cell, rowspan, colspan, classes in row:
                entry_node = nodes.entry(classes=classes)
                if rowspan > 1:
                    entry_node['morerows'] = rowspan-1
                if colspan > 1:
                    entry_node['morecols'] = colspan-1
                row_node += entry_node
                para_node = nodes.paragraph()
                entry_node += para_node
                text_node = nodes.Text(cell)
                para_node += text_node
    body_node = nodes.tbody()
    group_node += body_node
    body_rows = build.body(build.body_height(data, cut), data, cut)
    if body_rows:
        for row in body_rows:
            row_node = nodes.row()
            body_node += row_node
            for cell, rowspan, colspan, classes in row:
                entry_node = nodes.entry(classes=classes)
                if rowspan > 1:
                    entry_node['morerows'] = rowspan-1
                if colspan > 1:
                    entry_node['morecols'] = colspan-1
                row_node += entry_node
                para_node = nodes.paragraph()
                entry_node += para_node
                text_node = nodes.Text(cell)
                para_node += text_node
    return table_node

def get_build_by_domain(domain):
    if domain['type'] == 'list':
        return ListBuild(domain)
    elif domain['type'] == 'record':
        return RecordBuild(domain)
    else:
        return ScalarBuild(domain)


class MetaBuild(object):

    def __init__(self, profile):
        self.profile = profile
        self.header = profile.get('header')
        if not self.header:
            self.header = u""
        self.domain_build = get_build_by_domain(profile['domain'])
        self.span = self.domain_build.span

    def head_height(self):
        if not self.span:
            return 0
        height = self.domain_build.head_height()
        if self.header:
            height += 1
        return height

    def head(self, height):
        rows = [[] for idx in range(height)]
        if not self.span or not height:
            return rows
        is_last = (not self.domain_build.head_height())
        if not is_last:
            rows = [[]] + self.domain_build.head(height-1)
        rowspan = 1
        if is_last:
            rowspan = height
        colspan = self.span
        classes = []
        if not self.header:
            classes.append(u'htsql-dummy')
        rows[0].append((self.header.replace(u" ", u"\xA0"),
                        rowspan, colspan, classes))
        return rows

    def body_height(self, data, cut):
        return self.domain_build.body_height(data, cut)

    def body(self, height, data, cut):
        return self.domain_build.body(height, data, cut)

    def cut(self, height):
        return self.domain_build.cut(height)

    def measures(self, data, cut):
        measures = self.domain_build.measures(data, cut)
        if len(measures) == 1:
            measures[0] = max(measures[0], len(self.header))
        return measures


class ListBuild(object):

    def __init__(self, domain):
        self.item_build = get_build_by_domain(domain['item']['domain'])
        self.span = self.item_build.span

    def head_height(self):
        if not self.span:
            return []
        return self.item_build.head_height()

    def head(self, height):
        if not self.span or not height:
            return [[] for idx in range(height)]
        return self.item_build.head(height)

    def body_height(self, data, cut):
        if not self.span or not data:
            return 0
        height = 0
        for item in data:
            item_height = self.item_build.body_height(item, None)
            if cut and height+item_height > cut:
                return height+1
            height += item_height
        return height

    def body(self, height, data, cut):
        if not self.span or not height:
            return [[] for idx in range(height)]
        if not data:
            rows = [[] for idx in range(height)]
            rows[0].append((u"", height, self.span, [u'htsql-dummy']))
            return rows
        rows = []
        for idx, item in enumerate(data):
            item_height = self.item_build.body_height(item, None)
            if cut and len(rows)+item_height > cut:
                rows += self.item_build.cut(height)
                break
            if idx == len(data)-1 and item_height < height:
                item_height = height
            height -= item_height
            rows += self.item_build.body(item_height, item, None)
        return rows

    def cut(self, height):
        if not self.span or not height:
            return [[] for idx in range(height)]
        return self.item_build.cut(height)

    def measures(self, data, cut):
        measures = [1 for idx in range(self.span)]
        if not self.span or not data:
            return measures
        height = 0
        for idx, item in enumerate(data):
            height += self.item_build.body_height(item, None)
            if cut and height > cut:
                break
            item_measures = self.item_build.measures(item, None)
            measures = [max(measure, item_measure)
                        for measure, item_measure
                            in zip(measures, item_measures)]
        return measures


class RecordBuild(object):

    def __init__(self, domain):
        self.field_builds = [MetaBuild(field) for field in domain['fields']]
        self.span = sum(field_build.span for field_build in self.field_builds)

    def head_height(self):
        if not self.span:
            return 0
        return max(field_build.head_height()
                   for field_build in self.field_builds)

    def head(self, height):
        rows = [[] for idx in range(height)]
        if not self.span or not height:
            return rows
        for field_build in self.field_builds:
            field_rows = field_build.head(height)
            rows = [row+field_row
                    for row, field_row in zip(rows, field_rows)]
        return rows

    def body_height(self, data, cut):
        if not self.span:
            return 0
        if not data:
            data = [None]*len(self.field_builds)
        return max(field_build.body_height(item, cut)
                   for field_build, item in zip(self.field_builds, data))

    def body(self, height, data, cut):
        rows = [[] for idx in range(height)]
        if not self.span:
            return rows
        if not data:
            data = [None]*len(self.field_builds)
        for field_build, item in zip(self.field_builds, data):
            field_rows = field_build.body(height, item, cut)
            rows = [row+field_row
                    for row, field_row in zip(rows, field_rows)]
        return rows

    def cut(self, height):
        rows = [[] for idx in range(height)]
        if not self.span or not height:
            return rows
        for field_build in self.field_builds:
            field_rows = field_build.cut(height)
            rows = [row+field_row
                    for row, field_row in zip(rows, field_rows)]
        return rows

    def measures(self, data, cut):
        if not data:
            data = [None]*self.span
        measures = []
        for field_build, item in zip(self.field_builds, data):
            measures += field_build.measures(item, cut)
        return measures


class ScalarBuild(object):

    def __init__(self, domain):
        self.domain = domain
        self.span = 1

    def head_height(self):
        return 0

    def head(self, height):
        rows = [[] for idx in range(height)]
        if not height:
            return rows
        rows[0].append((u"", height, 1, [u'htsql-dummy']))
        return rows

    def body_height(self, data, cut):
        return 1

    def body(self, height, data, cut):
        rows = [[] for idx in range(height)]
        if not height:
            return rows
        classes = [u'htsql-%s-type' % self.domain['type']]
        if data is None:
            classes.append(u'htsql-null-val')
            data = u""
        elif data is True:
            classes.append(u'htsql-true-val')
            data = u"true"
        elif data is False:
            classes.append(u'htsql-false-val')
            data = u"false"
        else:
            data = unicode(data)
            if not data:
                classes.append(u'htsql-empty-val')
        rows[0].append((data, height, 1, classes))
        return rows

    def cut(self, height):
        rows = [[] for idx in range(height)]
        if not height:
            return rows
        classes = [u'htsql-%s-type' % self.domain['type'], u'htsql-cut']
        rows[0].append((u"", height, 1, classes))
        return rows

    def measures(self, data, cut):
        if data is None:
            return [1]
        return [max(1, len(unicode(data)))]


def copy_static(app, exception):
    if app.builder.name != 'html' or exception:
        return
    src_dir = os.path.join(os.path.abspath(os.path.dirname(__file__)), 'static')
    dst_dir = os.path.join(app.builder.outdir, '_static')
    for filename in os.listdir(src_dir):
        src = os.path.join(src_dir, filename)
        if not os.path.isfile(src):
            continue
        dst = os.path.join(dst_dir, filename)
        copyfile(src, dst)


def setup(app):
    app.add_config_value('htsql_server', None, 'env')
    app.add_config_value('build_website', False, 'env')
    app.add_directive('htsql-server', HTSQLServerDirective)
    app.add_directive('htsql', HTSQLDirective)
    app.add_directive('vsplit', VSplitDirective)
    app.connect('env-purge-doc', purge_htsql_server)
    app.connect('build-finished', copy_static)
    app.add_node(htsql_block,
                 html=(visit_htsql_block, depart_htsql_block))
    app.add_stylesheet('htsqldoc.css')
    app.add_lexer('htsql', HtsqlLexer())


