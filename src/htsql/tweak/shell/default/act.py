#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ....core.context import context
from ....core.adapter import adapt
from ....core.cmd.act import Act, RenderAction, act
from ....core.cmd.command import UniversalCmd, DefaultCmd
from ....core.cmd.summon import Recognize
from ..command import ShellCmd
from ....core.syn.parse import parse
from ....core.syn.syntax import SkipSyntax
from ....core.error import Error
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
        content_type = ""
        if 'HTTP_ACCEPT' in self.action.environ:
            content_types = self.action.environ['HTTP_ACCEPT'].split(',')
            if len(content_types) == 1:
                [content_type] = content_types
                if ';' in content_type:
                    content_type = content_type.split(';', 1)[0]
                    content_type = content_type.strip()
            else:
                content_type = "*/*"
        if content_type != "*/*":
            return super(ShellRenderUniversal, self).__call__()
        try:
            syntax = parse(self.command.query)
            if addon.on_root and isinstance(syntax, SkipSyntax):
                command = ShellCmd(is_implicit=True)
            else:
                command = Recognize.__invoke__(syntax)
                if command is None:
                    if (addon.on_default and
                            not isinstance(syntax, SkipSyntax)):
                        query = unquote(self.command.query)
                        command = ShellCmd(query, is_implicit=True)
                    else:
                        command = DefaultCmd(syntax)
            return act(command, self.action)
        except Error:
            if not addon.on_error:
                raise
            query = unquote(self.command.query)
            command = ShellCmd(query, is_implicit=True)
            return act(command, self.action)


