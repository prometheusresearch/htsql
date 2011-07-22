#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


"""
:mod:`htsql_ctl.extension`
==========================

This module implements the `extension` routine.
"""


from .error import ScriptError
from .routine import Routine, Argument
from htsql.validator import StrVal
from htsql.addon import Addon
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
        for entry_point in entry_points:
            try:
                addon_class = entry_point.load()
            except Exception:
                addon_class = None
            if not (isinstance(addon_class, type) and
                    issubclass(addon_class, Addon) and
                    addon_class.name == entry_point.name):
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
                self.ctl.out("%-24s : [BROKEN]" % entry_point.name)
        self.ctl.out()

    def describe_extension(self):
        entry_points = list(pkg_resources.iter_entry_points('htsql.addons',
                                                            self.addon))
        if not entry_points:
            raise ScriptError("unknown extension %s" % self.addon)
        if len(entry_points) > 1:
            raise ScriptError("multiple implementations of extension %s"
                              % self.addon)
        [entry_point] = entry_points
        try:
            addon_class = entry_point.load()
        except Exception, exc:
            raise ScriptError("failed to load extension %s: %s"
                              % self.addon, exc)
        if not (isinstance(addon_class, type) and
                issubclass(addon_class, Addon) and
                addon_class.name == entry_point.name):
            raise ScriptError("failed to load extension %s" % self.addon)
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

