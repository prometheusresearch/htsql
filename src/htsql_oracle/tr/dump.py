#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql_oracle.tr.dump`
===========================

This module adapts the SQL serializer for Oracle.
"""


from htsql.tr.dump import DumpAnchor, DumpLeadingAnchor


class OracleDumpLeadingAnchor(DumpLeadingAnchor):

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.state.push_hook(with_aliases=True)
        self.format("{frame} {alias:name}",
                    frame=self.clause.frame, alias=alias)
        self.state.pop_hook()


class OracleDumpAnchor(DumpAnchor):

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        self.newline()
        if self.clause.is_cross:
            self.write("CROSS JOIN ")
        elif self.clause.is_inner:
            self.write("INNER JOIN ")
        elif self.clause.is_left and not self.clause.is_right:
            self.write("LEFT OUTER JOIN ")
        elif self.clause.is_right and not self.clause.is_left:
            self.write("RIGHT OUTER JOIN ")
        else:
            self.write("FULL OUTER JOIN ")
        self.indent()
        self.state.push_hook(with_aliases=True)
        self.format("{frame} {alias:name}",
                    frame=self.clause.frame, alias=alias)
        self.state.pop_hook()
        if self.clause.condition is not None:
            self.newline()
            self.format("ON {condition}",
                        condition=self.clause.condition)
        self.dedent()


