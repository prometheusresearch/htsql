
from htsql import HTSQL
import sys, decimal, datetime

db = __pbbt__['demo'].db

htsql = HTSQL(db)

uri = "/school{code, count(program), count(department)}"
print("URI:", uri)
for row in htsql.produce(uri):
    print(row)
print()

uri = "/school{name, count(program)}?code=$school_code"
school_code = "bus"
print("URI:", uri)
print("$school_code: %r" % school_code)
for row in htsql.produce(uri, school_code=school_code):
    print("%s: %s" % row)
print()

uri = "/school{name, num_prog:=count(program)}" \
      "?num_prog>=$min_prog&num_prog<=$max_prog"
min_prog = 6
max_prog = 8
print("URI:", uri)
print("$min_prog: %r" % min_prog)
print("$max_prog: %r" % max_prog)
for row in htsql.produce(uri, min_prog=min_prog, max_prog=max_prog):
    print("%s: %s" % (row.name, row.num_prog))
print()

uri = "/school?campus==$campus"
campus = None
print("URI:", uri)
print("$campus: %r" % campus)
for row in htsql.produce(uri, campus=campus):
    print(row)
print()

uri = "/school?campus=$campus"
campus = ['north', 'south']
print("URI:", uri)
print("$campus: %r" % campus)
for row in htsql.produce(uri, campus=campus):
    print(row)
print()

uri = "/{$untyped, $selector, $boolean, $integer, $float, $decimal," \
      " $date, $time, $datetime}"
untyped_value = "HTSQL"
selector_value = ["HTTP", "SQL"]
boolean_value = True
integer_value = 3571
float_value = -57721e-5
decimal_value = decimal.Decimal("0.875")
if 'sqlite' in __pbbt__:
    decimal_value = None
date_value = datetime.date(2010, 4, 15)
time_value = datetime.time(20, 3)
datetime_value = datetime.datetime(2010, 4, 15, 20, 3)
print("URI:", uri)
print("$untyped: %r" % untyped_value)
print("$selector: %r" % selector_value)
print("$boolean: %r" % boolean_value)
print("$integer: %r" % integer_value)
print("$float: %r" % float_value)
if sys.version_info[:2] == (2, 5):
    print(("$decimal: %r" % decimal_value).replace('"', '\''))
else:
    print("$decimal: %r" % decimal_value)
print("$date: %r" % date_value)
print("$time: %r" % time_value)
print("$datetime: %r" % datetime_value)
for row in htsql.produce(uri, untyped=untyped_value,
                              selector=selector_value,
                              boolean=boolean_value,
                              integer=integer_value,
                              float=float_value,
                              decimal=decimal_value,
                              date=date_value,
                              time=time_value,
                              datetime=datetime_value):
    if sys.version_info[:2] == (2, 5):
        print(str(row).replace('"', '\''))
    else:
        print(row)
print()

htsql = HTSQL(db, {'tweak.meta': None})

uri = "/meta(/link?table.name=$table_name)"
table_name = "school"
print("URI:", uri)
print("$table_name: %r" % table_name)
for row in htsql.produce(uri, table_name=table_name):
    print(row)
print()

htsql = HTSQL(None, {'htsql': {'db': db}, 'tweak.autolimit': {'limit': 3}})

uri = "/school"
print("URI:", uri)
for row in htsql.produce(uri):
    print(row)

