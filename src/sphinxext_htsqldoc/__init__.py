

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
            (r'\.|,|\(|\)|\{|\}|\[|\]|:=|:|\$', Punctuation),
        ]
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
        content_type, content = env.htsql_uris[uri]
        if 'plain' in self.options:
            content_type = 'text/plain'
        result_node = build_result(self.content_offset, content_type, content,
                                   self.options.get('cut'))
        query_container = nodes.container('', query_node,
                                          classes=['htsql-input'])
        result_container = nodes.container('', result_node,
                                           classes=['htsql-output'])
        if 'hide' in self.options:
            result_container['classes'].append('htsql-hide')
        return [query_container, result_container]


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
    if node.has_key('language'):
        lang = node['language']
    if node.has_key('linenos'):
        linenos = node['linenos']
    def warner(msg):
        self.builder.warn(msg, (self.builder.current_docname, node.line))
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
    toggle = "[-]"
    if node.has_key('hide') and node['hide']:
        toggle = "[+]"
    highlighted = '<span class="htsql-toggle">%s</span>%s' \
            % (toggle, highlighted)
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
        headers = { 'Accept': 'application/javascript' }
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
            text_node = nodes.Text(title)
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
        result_node = table_node
    else:
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
    app.add_javascript('htsqldoc.js')
    app.add_lexer('htsql', HtsqlLexer())


