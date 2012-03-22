#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from htsql.core.adapter import adapt
from htsql.core.domain import (BooleanDomain, StringDomain, DateDomain,
                               TimeDomain, DateTimeDomain)
from htsql.core.tr.frame import ScalarFrame, TableFrame
from htsql.core.tr.dump import (SerializeSegment, Dump, DumpBranch, DumpAnchor,
                                DumpLeadingAnchor, DumpFromPredicate,
                                DumpToPredicate, DumpBoolean, DumpInteger,
                                DumpFloat, DumpTime, DumpDateTime,
                                DumpToFloat, DumpToDecimal, DumpToString,
                                DumpToDate, DumpToTime, DumpToDateTime,
                                DumpIsTotallyEqual, DumpBySignature,
                                DumpSortDirection)
from htsql.core.tr.fn.dump import (DumpLength, DumpSubstring, DumpDateIncrement,
                                   DumpDateDecrement, DumpDateDifference,
                                   DumpMakeDate, DumpCombineDateTime,
                                   DumpExtractSecond, DumpToday)
from .signature import RowNumSig
import math


class OracleSerializeSegment(SerializeSegment):

    max_alias_length = 30


class OracleDumpScalar(Dump):

    adapt(ScalarFrame)

    def __call__(self):
        self.write(u"DUAL")


class OracleDumpBranch(DumpBranch):

    def dump_limit(self):
        assert self.frame.limit is None
        assert self.frame.offset is None


class OracleDumpSortDirection(DumpSortDirection):

    def __call__(self):
        self.format("{base} {direction:switch{ASC|DESC}}",
                    self.arguments, self.signature)
        if self.phrase.is_nullable:
            self.format(" NULLS {direction:switch{FIRST|LAST}}",
                        self.signature)


class OracleDumpLeadingAnchor(DumpLeadingAnchor):

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        if isinstance(self.clause.frame, TableFrame):
            table = self.clause.frame.table
            if alias == table.name:
                alias = None
        self.state.push_hook(with_aliases=True)
        if alias is not None:
            self.format("{frame} {alias:name}",
                        frame=self.clause.frame, alias=alias)
        else:
            self.format("{frame}", frame=self.clause.frame)
        self.state.pop_hook()


class OracleDumpAnchor(DumpAnchor):

    def __call__(self):
        alias = self.state.frame_alias_by_tag[self.clause.frame.tag]
        if isinstance(self.clause.frame, TableFrame):
            table = self.clause.frame.table
            if alias == table.name:
                alias = None
        self.newline()
        if self.clause.is_cross:
            self.write(u"CROSS JOIN ")
        elif self.clause.is_inner:
            self.write(u"INNER JOIN ")
        elif self.clause.is_left and not self.clause.is_right:
            self.write(u"LEFT OUTER JOIN ")
        elif self.clause.is_right and not self.clause.is_left:
            self.write(u"RIGHT OUTER JOIN ")
        else:
            self.write(u"FULL OUTER JOIN ")
        self.indent()
        self.state.push_hook(with_aliases=True)
        if alias is not None:
            self.format("{frame} {alias:name}",
                        frame=self.clause.frame, alias=alias)
        else:
            self.format("{frame}", frame=self.clause.frame)
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
            self.write(u"1")
        if self.value is False:
            self.write(u"0")


class OracleDumpInteger(DumpInteger):

    def __call__(self):
        if self.value >= 0:
            self.write(unicode(self.value))
        else:
            self.write(u"(%s)" % self.value)


class OracleDumpFloat(DumpFloat):

    def __call__(self):
        assert not math.isinf(self.value) and not math.isnan(self.value)
        if self.value >= 0.0:
            self.write(u"%rD" % self.value)
        else:
            self.write(u"(%rD)" % self.value)


class OracleDumpTime(DumpTime):

    def __call__(self):
        self.format("INTERVAL {value:literal} HOUR TO SECOND",
                    value=unicode(self.value))


class OracleDumpDateTime(DumpDateTime):

    def __call__(self):
        self.format("TIMESTAMP {value:literal}", value=unicode(self.value))


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

    adapt(BooleanDomain, StringDomain)

    def __call__(self):
        if self.base.is_nullable:
            self.format("(CASE WHEN {base} <> 0 THEN 'true'"
                        " WHEN NOT {base} = 0 THEN 'false' END)",
                        base=self.base)
        else:
            self.format("(CASE WHEN {base} <> 0 THEN 'true' ELSE 'false' END)",
                        base=self.base)


class OracleDumpDateToString(DumpToString):

    adapt(DateDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR({base}, 'YYYY-MM-DD')", base=self.base)


class OracleDumpTimeToString(DumpToString):

    adapt(TimeDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR(TIMESTAMP '2001-01-01 00:00:00' + {base},"
                    " 'HH24:MI:SS.FF')", base=self.base)


class OracleDumpDateTimeToString(DumpToString):

    adapt(DateTimeDomain, StringDomain)

    def __call__(self):
        self.format("TO_CHAR({base}, 'YYYY-MM-DD HH24:MI:SS.FF')",
                    base=self.base)


class OracleDumpStringToDate(DumpToDate):

    adapt(StringDomain, DateDomain)

    def __call__(self):
        self.format("TO_DATE({base}, 'YYYY-MM-DD')", base=self.base)


class OracleDumpDateTimeToDate(DumpToDate):

    adapt(DateTimeDomain, DateDomain)

    def __call__(self):
        self.format("TRUNC({base}, 'DD')", base=self.base)


class OracleDumpStringToTime(DumpToTime):

    adapt(StringDomain, TimeDomain)

    def __call__(self):
        self.format("TO_DSINTERVAL('0 ' || {base})", base=self.base)


class OracleDumpDateTimeToTime(DumpToTime):

    adapt(DateTimeDomain, TimeDomain)

    def __call__(self):
        self.format("({base} - TRUNC({base}, 'DD'))", base=self.base)


class OracleDumpStringToDateTime(DumpToDateTime):

    adapt(StringDomain, DateTimeDomain)

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

    adapt(RowNumSig)

    def __call__(self):
        self.write(u"ROWNUM")


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


class OracleDumpToday(DumpToday):

    template = "TRUNC(CURRENT_DATE, 'DD')"


class OracleDumpMakeDate(DumpMakeDate):

    template = ("(DATE '2001-01-01' + ({year} - 2001) * INTERVAL '1' YEAR"
                " + ({month} - 1) * INTERVAL '1' MONTH"
                " + ({day} - 1) * INTERVAL '1' DAY)")


class OracleDumpCombineDateTime(DumpCombineDateTime):

    template = "(CAST({date} AS TIMESTAMP) + {time})"


class OracleDumpExtractSecond(DumpExtractSecond):

    template = "(1D * EXTRACT(SECOND FROM {op}))"


