#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.split_sql import SQLToken, SplitSQL


class SplitOracle(SplitSQL):
    """
    Implements the SQL splitter for Oracle.
    """

    tokens = [
            # Whitespace between separate statements.
            SQLToken(r"""
                     # whitespaces
                     [\ \t\r\n]+
                     # or a SQL comment
                     | -- [^\r\n]* \r?\n
                     # or a #-comment
                     | \# [^\r\n]* \r?\n
                     # or a C-style comment
                     | /\* .*? \*/
                     """, only_level=0, is_junk=True),

            # The beginning of a SQL statement.
            SQLToken(r""" [a-zA-Z]+ """, only_level=0, delta=+1),

            # The body of function/trigger.
            SQLToken(r""" \b BEGIN \b """, only_level=1, delta=+1),

            # Semicolon in the body of a function.
            SQLToken(r""" ; """, only_level=2),

            # END followed by a semicolon ends a trigger defition.  Note
            # that the semicolon itself must be passed to the server.
            SQLToken(r""" \b END \s* ; """, only_level=2, delta=-2),

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
                     # or a keyword or a name (but skip BEGIN/END)
                     | (?!BEGIN\b) (?!END\b) [a-zA-Z_][0-9a-zA-Z_]* \b
                     # or a number
                     | [0-9]+ (?: \. [0-9]* )? (?: [eE] [+-] [0-9]+ )?
                     # or a symbol
                     | [().,:<>=!&|~*/%+-]
                     )+
                     """, min_level=1),

            # Semicolon at the top level indicates the statement end.
            # Note that the server does not accept a semicolon at the
            # end of the statement (unless it is `END;`).
            SQLToken(r""" ; """, only_level=1, delta=-1, is_junk=True),

            # Same for EOF, but it also stops the splitter.
            SQLToken(r""" $ """, only_level=1, delta=-1, is_end=True),

            # EOF outside the statement stops the splitter.
            SQLToken(r""" $ """, only_level=0, is_end=True),
    ]


