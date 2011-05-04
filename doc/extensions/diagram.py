

from docutils import nodes
from docutils.parsers.rst import Directive, directives
from sphinx.util.osutil import ensuredir
from subprocess import Popen, PIPE
import os, os.path, tempfile, shutil


class DiagramDirective(Directive):
    required_arguments = 1
    has_content = False
    option_spec = {
        'alt': directives.unchanged,
        'align': lambda arg: directives.choice(arg, ('left', 'center', 'right')),
    }

    def run(self):
        doc = self.state.document
        env = doc.settings.env
        docdir = os.path.dirname(env.doc2path(env.docname, base=None))
        filename = self.arguments[0]
        if filename.startswith('/'):
            filename = os.path.normpath(filename[1:])
        else:
            filename = os.path.normpath(os.path.join(docdir, filename))
        env.note_dependency(filename)
        filename = os.path.join(env.srcdir, filename)
        try:
            name, data, width, height = render_diagram(env, filename)
        except DiagramError, exc:
            return [doc.reporter.error(str(exc))]
        node = diagram()
        node['name'] = name
        node['data'] = data
        node['width'] = width
        node['height'] = height
        node['alt'] = self.options.get('alt')
        node['align'] = self.options.get('align')
        return [node]


class DiagramError(Exception):
    pass


class diagram(nodes.General, nodes.Element):
    pass


def render_diagram(env, filename):
    directory = os.path.dirname(filename)
    basename = os.path.basename(filename)
    stem = os.path.splitext(basename)[0]
    name = stem + '.png'
    temp = tempfile.mkdtemp()
    try:
        texinputs = [directory]
        for texdir in env.config.diagram_texinputs:
            texdir = os.path.join(env.srcdir, texdir)
            texinputs.append(texdir)
        texinputs.append('')
        texinputs = ':'.join(texinputs)
        environ = os.environ.copy()
        environ['TEXINPUTS'] = texinputs
        cmdline = [env.config.diagram_pdflatex,
                   '-halt-on-error',
                   '-interaction', 'nonstopmode',
                   '-output-directory', temp,
                   basename]
        execute(cmdline, env=environ)
        cmdline = [env.config.diagram_pdftoppm,
                   '-r', str(env.config.diagram_resolution),
                   '-f', '1', '-l', '1',
                   os.path.join(temp, stem)+'.pdf',
                   os.path.join(temp, stem)]
        execute(cmdline)
        ppmfile = os.path.join(temp, stem)+'-1.ppm'
        if not os.path.exists(ppmfile):
            raise DiagramError("file not found: %s" % ppmfile)
        data = open(ppmfile).read()
        cmdline = [env.config.diagram_pnmcrop]
        data = execute(cmdline, data)
        line = data.splitlines()[1]
        width, height = [int(chunk) for chunk in line.split()]
        cmdline = [env.config.diagram_pnmtopng,
                   '-transparent', 'white',
                   '-compression', '9']
        data = execute(cmdline, data)
    finally:
        shutil.rmtree(temp)
    return name, data, width, height


def execute(cmdline, input=None, env=None):
    try:
        process = Popen(cmdline, stdin=PIPE, stdout=PIPE, stderr=PIPE, env=env)
    except OSError, exc:
        raise DiagramError("cannot start executable `%s`: %s"
                           % (' '.join(cmdline), exc))
    output, error = process.communicate(input)
    if process.returncode != 0:
        if not error:
            error = output
        raise DiagramError("`%s` exited with an error:\n%s"
                           % (' '.join(cmdline), error))
    return output


def visit_diagram(self, node):
    href = "%s/%s" % (self.builder.imgpath, node['name'])
    filename = os.path.join(self.builder.outdir, '_images', node['name'])
    ensuredir(os.path.dirname(filename))
    open(filename, 'wb').write(node['data'])
    if (isinstance(node.parent, nodes.TextElement) or
        (isinstance(node.parent, nodes.reference) and
         not isinstance(node.parent.parent, nodes.TextElement))):
        suffix = ''
    else:
        suffix = '\n'
    atts = {}
    atts['src'] = href
    atts['width'] = node['width']
    atts['height'] = node['height']
    if node['alt']:
        atts['alt'] = node['alt']
    atts['class'] = 'diagram'
    if node['align']:
        atts['class'] += ' align-%s' % node['align']
    self.body.append(self.emptytag(node, 'img', suffix, **atts))


def depart_diagram(self, node):
    pass


def setup(app):
    app.add_config_value('diagram_pdflatex', 'pdflatex', 'env')
    app.add_config_value('diagram_pdftoppm', 'pdftoppm', 'env')
    app.add_config_value('diagram_pnmcrop', 'pnmcrop', 'env')
    app.add_config_value('diagram_pnmtopng', 'pnmtopng', 'env')
    app.add_config_value('diagram_texinputs', [], 'env')
    app.add_config_value('diagram_resolution', 110, 'env')
    app.add_directive('diagram', DiagramDirective)
    app.add_node(diagram,
                 html=(visit_diagram, depart_diagram))


