#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.split_sql import SQLToken, SplitSQL


class SplitSQLite(SplitSQL):
    """
    Implements the SQL splitter for SQLite.
    """

    # This is a simple tokenizer for SQLite.  It does not verify
    # that the statements are lexically valid and it may fail
    # to recognize some valid statements, however it works for
    # most common cases.

    tokens = [
            # Whitespace between separate statements.
            SQLToken(r"""
                     # whitespaces
                     [\ \t\r\n]+
                     # or a SQL comment
                     | -- [^\r\n]* \r?\n
                     # or a C-style comment
                     | /\* .*? \*/
                     """, only_level=0, is_junk=True),

            # The beginning of a SQL statement.
            SQLToken(r""" [a-zA-Z]+ """, only_level=0, delta=+1),

            # Start of the BEGIN/END block.
            SQLToken(r""" \b BEGIN \b """, min_level=1, delta=+1),

            # End of the BEGIN/END block.
            SQLToken(r""" \b END \b """, min_level=2, delta=-1),

            # A block of regular SQL tokens.
            SQLToken(r"""
                     (
                     # whitespaces
                     [\ \t\r\n]+
                     # or a SQL comment
                     | -- [^\r\n]*\r?\n
                     # or a C-style comment
                     | /\* .*? \*/
                     # or a string literal
                     | ' (?: [^'] | '' )* '
                     # or a quoted name
                     | " (?: [^"]+ | "" )+ "
                     # or a keyword or a name
                     | [a-zA-Z_][0-9a-zA-Z_]*
                     # or a number
                     | [0-9]+ (?: \. [0-9]* )? (?: [eE] [+-] [0-9]+ )?
                     # or a symbol
                     | [().,<>=!&|~*/%+-]
                     )+
                     """, min_level=1),

            # Semicolon at the top level indicates the statement end.
            SQLToken(r""" ; """, only_level=1, delta=-1),

            # Semicolon within BEGIN/END block is just a separator.
            SQLToken(r""" ; """, min_level=2),

            # Same for EOF, but it also stops the splitter.
            SQLToken(r""" $ """, only_level=1, delta=-1, is_end=True),

            # EOF outside the statement stops the splitter.
            SQLToken(r""" $ """, only_level=0, is_end=True),
    ]


