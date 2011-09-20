#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.context import context
from htsql.adapter import adapts
from htsql.cmd.act import Act, RenderAction, act
from htsql.cmd.command import UniversalCmd, DefaultCmd
from ..command import ShellCmd
from htsql.tr.error import TranslateError
from htsql.tr.parse import parse
from htsql.tr.bind import bind
from htsql.tr.lookup import lookup_command
import re


escape_pattern = r"""%(?:(?P<code>[0-9A-Fa-f]{2})|..)"""
escape_regexp = re.compile(escape_pattern)

def unquote(query):
    def replace(match):
        code = match.group('code')
        if not code:
            return match.group()
        code = int(code, 16)
        if code == 0x00:
            return match.group()
        return chr(code)
    return escape_regexp.sub(replace, query)


class ShellRenderUniversal(Act):

    adapts(UniversalCmd, RenderAction)

    def __call__(self):
        addon = context.app.tweak.shell.default
        command = None
        accept = set()
        if 'HTTP_ACCEPT' in self.action.environ:
            for name in self.action.environ['HTTP_ACCEPT'].split(','):
                if ';' in name:
                    name = name.split(';', 1)[0]
                name = name.strip()
                accept.add(name)
        if not (('text/html' in accept or 'text/*' in accept
                 or '*/*' in accept) and 'application/json' not in accept):
            return super(ShellRenderUniversal, self).__call__()
        try:
            syntax = parse(self.command.query)
            if addon.on_root and syntax.segment.branch is None:
                command = ShellCmd(is_implicit=True)
            else:
                binding = bind(syntax)
                command = lookup_command(binding)
                if command is None:
                    if (addon.on_default and
                            syntax.segment.branch is not None):
                        query = unquote(self.command.query)
                        command = ShellCmd(query, is_implicit=True)
                    else:
                        command = DefaultCmd(binding)
            return act(command, self.action)
        except TranslateError:
            if not addon.on_error:
                raise
            query = unquote(self.command.query)
            command = ShellCmd(query, is_implicit=True)
            return act(command, self.action)


