#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
The :mod:`htsql.core.syn` package defines the grammar of HTSQL and implements
the HTSQL parser.  Call :func:`parse.parse` to convert a raw query string
to a syntax tree.  This conversion is performed in three steps:

* *Decoding*: transmission artefacts are removed from the input stream of
  characters.
* *Lexical scanning*: the input stream of characters is converted to a
  sequence of lexical tokens.
* *Syntax parsing*: a sequence of lexical tokens is converted to a syntax
  tree.
"""


from . import decode, grammar, parse, scan, syntax, token


