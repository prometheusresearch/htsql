

from docutils import nodes
from docutils.parsers.rst import Directive, directives

from urllib2 import quote, urlopen, Request, HTTPError
from json import loads


class HTSQLDirective(Directive):
    required_arguments = 1
    final_argument_whitespace = True
    option_spec = {
            'plain': directives.flag,
            'error': directives.flag,
            'query': directives.path,
            'hide': directives.flag,
            'cut': directives.positive_int,
    }

    def run(self):
        env = self.state.document.settings.env
        query = " ".join(line.strip()
                         for line in self.arguments[0].split("\n"))
        query_node = nodes.literal_block(query, query)
        query_node['language'] = 'htsql'
        if not env.config.htsql_server:
            raise self.error("htsql_server is not set")
        if 'query' not in self.options:
            query = quote(query)
        else:
            query = self.options['query']
        uri = env.config.htsql_server+query
        if not hasattr(env, 'htsql_uris'):
            env.htsql_uris = {}
        if uri not in env.htsql_uris:
            result = load_uri(uri, 'error' in self.options)
            if not result:
                raise self.error("failed to load %s" % uri)
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


def load_uri(uri, error=False):
    try:
        headers = { 'Accept': 'application/json' }
        request = Request(uri, headers=headers)
        response = urlopen(request)
        content_type = response.info().gettype()
        content = response.read()
    except HTTPError, response:
        if not error:
            return None
        content_type = response.headers.gettype()
        content = response.read()
    return (content_type, content)


def build_result(line, content_type, content, cut=None):
    if content_type == 'application/json':
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
        result_node = nodes.literal_block(content, content)
        result_node['language'] = 'text'
    return result_node


def setup(app):
    app.add_config_value('htsql_server', None, '')
    app.add_directive('htsql', HTSQLDirective)
    app.add_directive('vsplit', VSplitDirective)
    app.add_stylesheet('htsqldoc.css')
    app.add_javascript('htsqldoc.js')


