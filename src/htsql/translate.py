#
# Copyright (c) 2006-2010, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.translate`
======================

This module implements the translate utility.
"""


from .adapter import Utility, find_adapters
from .tr.parser import QueryParser
from .tr.binder import Binder
from .tr.encoder import Encoder
from .tr.assembler import Assembler
from .tr.outliner import Outliner
from .tr.compiler import Compiler
from .tr.serializer import Serializer



class Translate(Utility):

    def __call__(self, uri):
        parser = QueryParser(uri)
        syntax = parser.parse()
        binder = Binder()
        binding = binder.bind_one(syntax)
        encoder = Encoder()
        code = encoder.encode(binding)
        assembler = Assembler()
        term = assembler.assemble(code)
        outliner = Outliner()
        sketch = outliner.outline(term)
        compiler = Compiler()
        frame = compiler.compile(sketch)
        serializer = Serializer()
        plan = serializer.serialize(frame)
        return plan


translate_adapters = find_adapters()


