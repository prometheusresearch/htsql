#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ....core.context import context
from ....core.adapter import adapt
from ....core.cmd.act import Act, RenderAction, act
from ....core.cmd.command import UniversalCmd, DefaultCmd
from ..command import ShellCmd
from ....core.tr.error import TranslateError
from ....core.tr.parse import parse
from ....core.tr.bind import bind
from ....core.tr.lookup import lookup_command
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

    adapt(UniversalCmd, RenderAction)

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
                        query = query.decode('utf-8', 'replace')
                        command = ShellCmd(query, is_implicit=True)
                    else:
                        command = DefaultCmd(binding)
            return act(command, self.action)
        except TranslateError:
            if not addon.on_error:
                raise
            query = unquote(self.command.query)
            query = query.decode('utf-8', 'replace')
            command = ShellCmd(query, is_implicit=True)
            return act(command, self.action)


