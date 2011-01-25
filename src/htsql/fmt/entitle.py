#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.fmt.entitle`
========================

This module implements the entitle adapter.
"""


from ..adapter import Adapter, adapts, adapts_many
from ..tr.binding import (Binding, RootBinding, SieveBinding, SortBinding,
                          CastBinding, WrapperBinding, TitleBinding)


class Entitle(Adapter):

    adapts(Binding)

    def __init__(self, binding, with_strong, with_weak):
        self.binding = binding
        self.with_strong = with_strong
        self.with_weak = with_weak

    def __call__(self):
        if self.with_weak:
            return str(self.binding.syntax)
        return None


class EntitleWrapper(Entitle):

    adapts_many(SieveBinding, SortBinding, WrapperBinding, CastBinding)

    def __call__(self):
        if self.with_strong:
            title = entitle(self.binding.base, with_weak=False)
            if title is not None:
                return title
        return super(EntitleWrapper, self).__call__()


class EntitleTitle(Entitle):

    adapts(TitleBinding)

    def __call__(self):
        if self.with_strong:
            return self.binding.title
        return super(EntitleTitle, self).__call__()


class EntitleRoot(Entitle):

    adapts(RootBinding)

    def __call__(self):
        if self.with_weak:
            return ""
        return super(EntitleRoot, self).__call__()


def entitle(binding, with_strong=True, with_weak=True):
    entitle = Entitle(binding, with_strong, with_weak)
    return entitle()


