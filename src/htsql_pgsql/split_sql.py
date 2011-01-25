#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_pgsql.split_sql`
============================

This module implements the SQL splitter for PostgreSQL.
"""


from htsql.split_sql import SQLToken, SplitSQL


class SplitPGSQL(SplitSQL):
    """
    Implements the SQL splitter for PostgreSQL.
    """

    # Note: this is not an exact PostgreSQL tokenizer, but
    # a good approximation.  In particular, we assume here that
    # the `standard_conforming_strings` parameter is turned on.

    tokens = [
            # Whitespace between separate statements.
            SQLToken(r"""
                     # whitespaces
                     [\ \t\r\n]+
                     # or a SQL comment (FIXME: add C-style comments?)
                     | -- [^\r\n]* \r?\n
                     # or a psql command
                     | \\ [a-zA-Z_] (?: [\ \t] [^\r\n]* )? \r?\n
                     """, only_level=0, is_junk=True),

            # The beginning of an SQL statement.
            SQLToken(r""" [a-zA-Z]+ """, only_level=0, delta=+1),

            # A block of regular SQL tokens.
            SQLToken(r"""
                     (
                     # whitespaces
                     [\ \t\r\n]+
                     # or a comment
                     | -- [^\r\n]*\r?\n
                     # or a standard-conforming string literal
                     | ' (?: [^'] | '' )* '
                     # or a C-style escaped string literal
                     | [eE] ' (?: [^'\\] | \\ . )* '
                     # or a quoted identifier
                     | " (?: [^"]+ | "" )+ "
                     # or a keyword or an unquoted identifier
                     | [a-zA-Z_][0-9a-zA-Z_$]*
                     # or a number
                     | [0-9]+ (?: \. [0-9]* )? (?: [eE] [+-] [0-9]+ )?
                     # or a symbol
                     | [*/<>=~!@#%^&|`?,:.+-]
                     )+
                     """, min_level=1),

            # $-quoted string literals.
            SQLToken(r"""
                     \$ (?P<tag> [^$]* ) \$
                     (?: [^$] | \$ (?! (?P=tag) \$ ) )*
                     \$ (?P=tag) \$
                     """, min_level=1),

            # Open parentheses and brackets nest.
            SQLToken(r""" [\(\[] """, min_level=1, delta=+1),

            # Close parentheses and brackets un-nest.
            SQLToken(r""" [\)\]] """, min_level=2, delta=-1),

            # Semicolon indicates the statement ends when there is no nesting.
            SQLToken(r""" ; """, only_level=1, delta=-1),

            # Same for EOF, but it also stops the splitter.
            SQLToken(r""" $ """, only_level=1, delta=-1, is_end=True),

            # EOF outside the statement stops the splitter.
            SQLToken(r""" $ """, only_level=0, is_end=True),
    ]


