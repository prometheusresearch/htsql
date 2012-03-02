#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.extension`
==========================

This module implements the `extension` routine.
"""


from .error import ScriptError
from .routine import Routine, Argument
from ..core.validator import StrVal
from ..core.addon import addon_registry
import pkg_resources


class ExtensionRoutine(Routine):

    name = 'extension'
    aliases = ['ext']
    arguments = [
            Argument('addon', StrVal(), default=None,
                     hint="""the name of the extension"""),
    ]
    hint = """list and describe HTSQL extensions"""
    help = """
    Run '%(executable)s extension' to get a list of available HTSQL
    extensions.

    Run '%(executable)s extension <name>' to describe a specific
    extension.
    """

    def run(self):
        if self.addon is None:
            self.list_extensions()
        else:
            self.describe_extension()

    def list_extensions(self):
        entry_points = list(pkg_resources.iter_entry_points('htsql.addons'))
        entry_points.sort(key=(lambda e: e.name))
        self.ctl.out("Available extensions:")
        for name in addon_registry:
            try:
                addon_class = addon_registry.load(name)
            except Exception:
                addon_class = None
            self.ctl.out("  ", end="")
            if addon_class is not None:
                name = addon_class.name
                hint = addon_class.get_hint()
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (name, hint))
                else:
                    self.ctl.out(name)
            else:
                self.ctl.out("%-24s : [BROKEN]" % name)
        self.ctl.out()

    def describe_extension(self):
        if self.addon not in addon_registry:
            raise ScriptError("unknown extension %s" % self.addon)
        try:
            addon_class = addon_registry.load(self.addon)
        except Exception, exc:
            raise ScriptError("failed to load extension %s: %s"
                              % (self.addon, exc))
        hint = addon_class.get_hint()
        if hint is not None:
            self.ctl.out(addon_class.name.upper(), "-", hint)
        else:
            self.ctl.out(addon_class.name.upper())
        help = addon_class.get_help()
        if help is not None:
            self.ctl.out()
            self.ctl.out(help)
        if addon_class.parameters:
            self.ctl.out()
            self.ctl.out("Parameters:")
            for parameter in addon_class.parameters:
                signature = parameter.get_signature()
                hint = parameter.get_hint()
                self.ctl.out("  ", end="")
                if hint is not None:
                    self.ctl.out("%-24s : %s" % (signature, hint))
                else:
                    self.ctl.out(signature)
        self.ctl.out()

