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


from htsql.adapter import adapts
from htsql.domain import (BooleanDomain, StringDomain, DateDomain, TimeDomain,
                          DateTimeDomain)
from htsql.tr.frame import ScalarFrame
from htsql.tr.dump import (SerializeSegment, Dump, DumpBranch, DumpAnchor,
                           DumpLeadingAnchor, DumpFromPredicate,
                           DumpToPredicate, DumpBoolean, DumpInteger,
                           DumpFloat, DumpTime, DumpDateTime,
                           DumpToFloat, DumpToDecimal, DumpToString,
                           DumpToDate, DumpToTime, DumpToDateTime,
                           DumpIsTotallyEqual, DumpBySignature)
from htsql.tr.fn.dump import (DumpLength, DumpSubstring, DumpDateIncrement,
                              DumpDateDecrement, DumpDateDifference,
                              DumpMakeDate, DumpCombineDateTime,
                              DumpExtractSecond)
from .signature import RowNumSig


class OracleSerializeSegment(SerializeSegment):

    max_alias_length = 30


class OracleDumpScalar(Dump):

    adapts(ScalarFrame)

    def __call__(self):
        self.write("DUAL")


class OracleDumpBranch(DumpBranch):

    def dump_group(self):
        if not self.frame.group:
            return
        self.newline()
        self.write("GROUP BY ")
        for index, phrase in enumerate(self.frame.group):
            self.format("{kernel}", kernel=phrase)
            if index < len(self.frame.group)-1:
                self.write(", ")

    def dump_order(self):
        if not self.frame.order:
            return
        self.newline()
        self.format("ORDER BY ")
        for index, (phrase, direction) in enumerate(self.frame.order):
            if phrase in self.frame.select:
                position = self.frame.select.index(phrase)+1
                self.write(str(position))
            else:
                self.format("{kernel}", kernel=phrase)
            self.format(" {direction:switch{ASC|DESC}}", direction=direction)
            if phrase.is_nullable:
                self.format(" NULLS {direction:switch{FIRST|LAST}}",
                            direction=direction)
            if index < len(self.frame.order)-1:
                self.write(", ")

    def dump_limit(self):
        assert self.frame.limit is None
        assert self.frame.offset is None


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


class OracleDumpFromPredicate(DumpFromPredicate):

    def __call__(self):
        if self.phrase.is_nullable:
            self.format("(CASE WHEN {op} THEN 1 WHEN NOT {op} THEN 0 END)",
                        self.arguments)
        else:
            self.format("(CASE WHEN {op} THEN 1 ELSE 0 END)",
                        self.arguments)


class OracleDumpToPredicate(DumpToPredicate):

    def __call__(self):
        self.format("({op} <> 0)", self.arguments)


class OracleDumpBoolean(DumpBoolean):

    def __call__(self):
        if self.value is True:
            self.write("1")
        if self.value is False:
            self.write("0")


class OracleDumpInteger(DumpInteger):

    def __call__(self):
        self.write(str(self.value))


class OracleDumpFloat(DumpFloat):

    def __call__(self):
        assert str(self.value) not in ['inf', '-inf', 'nan']
        self.write(repr(self.value)+'D')


class OracleDumpTime(DumpTime):

    def __call__(self):
        self.format("INTERVAL {value:literal} HOUR TO SECOND", value=str(self.value))


class OracleDumpDateTime(DumpDateTime):

    def __call__(self):
        self.format("TIMESTAMP {value:literal}", value=str(self.value))


class OracleDumpToFloat(DumpToFloat):

    def __call__(self):
        self.format("CAST({base} AS BINARY_DOUBLE)", base=self.base)


class OracleDumpToDecimal(DumpToDecimal):

    def __call__(self):
        self.format("CAST({base} AS NUMBER)", base=self.base)


class OracleDumpToString(DumpToString):

    def __call__(self):
        self.format("TO_CHAR({base})", base=self.base)


class OracleDumpBooleanToString(DumpToString):

    adapts(BooleanDomain, StringDomain)

    def __call__(self):
        if self.base.is_nullable:
            self.format("(CASE WHEN {base} <> 0 THEN 'true'"
                        " WHEN NOT {base} = 0 THEN 'false' END)",
                        base=self.base)
        else:
            self.format("(CASE WHEN {base} <> 0 THEN 'true' ELSE 'false' END)",
                        base=self.base)


class OracleDumpDateToString(DumpToString):

    adapts(DateDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR({base}, 'YYYY-MM-DD')", base=self.base)


class OracleDumpTimeToString(DumpToString):

    adapts(TimeDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR(TIMESTAMP '2001-01-01 00:00:00' + {base},"
                    " 'HH24:MI:SS.FF')", base=self.base)


class OracleDumpDateTimeToString(DumpToString):

    adapts(DateTimeDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR({base}, 'YYYY-MM-DD HH24:MI:SS.FF')",
                    base=self.base)


class OracleDumpStringToDate(DumpToDate):

    adapts(StringDomain, DateDomain)

    def __call__(self):
        self.format("TO_DATE({base}, 'YYYY-MM-DD')", base=self.base)


class OracleDumpDateTimeToDate(DumpToDate):

    adapts(DateTimeDomain, DateDomain)

    def __call__(self):
        self.format("TRUNC({base}, 'DD')", base=self.base)


class OracleDumpStringToTime(DumpToTime):

    adapts(StringDomain, TimeDomain)

    def __call__(self):
        self.format("TO_DSINTERVAL('0 ' || {base})", base=self.base)


class OracleDumpDateTimeToTime(DumpToTime):

    adapts(DateTimeDomain, TimeDomain)

    def __call__(self):
        self.format("({base} - TRUNC({base}, 'DD'))", base=self.base)


class OracleDumpStringToDateTime(DumpToDateTime):

    adapts(StringDomain, DateTimeDomain)

    def __call__(self):
        self.format("TO_TIMESTAMP({base}, 'YYYY-MM-DD HH24:MI:SS')",
                    base=self.base)


class OracleDumpIsTotallyEqual(DumpIsTotallyEqual):

    def __call__(self):
        self.format("((CASE WHEN ({lop} = {rop}) OR"
                    " ({lop} IS NULL AND {rop} IS NULL)"
                    " THEN 1 ELSE 0 END) {polarity:switch{<>|=}} 0)",
                    self.arguments, self.signature)


class OracleDumpRowNumber(DumpBySignature):

    adapts(RowNumSig)

    def __call__(self):
        self.write("ROWNUM")


class OracleDumpLength(DumpLength):

    template = "LENGTH({op})"


class OracleDumpSubstring(DumpSubstring):

    def __call__(self):
        if self.phrase.length is None:
            self.format("SUBSTR({op}, {start})", self.phrase)
        else:
            self.format("SUBSTR({op}, {start}, {length})", self.phrase)


class OracleDumpDateIncrement(DumpDateIncrement):

    template = "({lop} + {rop})"


class OracleDumpDateDecrement(DumpDateDecrement):

    template = "({lop} - {rop})"


class OracleDumpDateDifference(DumpDateDifference):

    template = "({lop} - {rop})"


class OracleDumpMakeDate(DumpMakeDate):

    template = ("(DATE '2001-01-01' + ({year} - 2001) * INTERVAL '1' YEAR"
                " + ({month} - 1) * INTERVAL '1' MONTH"
                " + ({day} - 1) * INTERVAL '1' DAY)")


class OracleDumpCombineDateTime(DumpCombineDateTime):

    template = "(CAST({date} AS TIMESTAMP) + {time})"


class OracleDumpExtractSecond(DumpExtractSecond):

    template = "(1D * EXTRACT(SECOND FROM {op}))"


