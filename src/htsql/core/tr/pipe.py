#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from ..util import Clonable, YAMLable
from ..context import context
from ..domain import Product
from ..connect import transaction, scramble, unscramble
from ..error import PermissionError
import operator
import tempfile
import pickle


class Pipe(Clonable, YAMLable):
    pass


class ComposePipe(Pipe):

    def __init__(self, left_pipe, right_pipe):
        self.left_pipe = left_pipe
        self.right_pipe = right_pipe

    def __call__(self):
        def compose(input, left=self.left_pipe(),
                           right=self.right_pipe()):
            return right(left(input))
        return compose

    def __yaml__(self):
        yield ('left', self.left_pipe)
        yield ('right', self.right_pipe)


class SQLPipe(Pipe):

    def __init__(self, sql, input_domains, output_domains):
        self.sql = sql
        self.input_domains = input_domains
        self.output_domains = output_domains

    def __call__(self):
        def run_sql(input, sql=self.sql.encode('utf-8'),
                           input_domains=self.input_domains,
                           output_domains=self.output_domains):
            if not context.env.can_read:
                raise PermissionError("No read permissions")
            scrambles = None
            if input_domains is not None:
                scrambles = [scramble(domain) for domain in input_domains]
            unscrambles = list(enumerate(
                    [unscramble(domain) for domain in output_domains]))
            with transaction() as connection:
                cursor = connection.cursor()
                if scrambles is None:
                    assert input is None
                    cursor.execute(sql)
                else:
                    assert isinstance(input, (tuple, list))
                    assert len(input) == len(scrambles)
                    parameters = dict((str(index+1), scramble(item))
                            for index, (item, scramble)
                                    in enumerate(zip(input, scrambles)))
                    cursor.execute(sql, parameters)
                output = []
                for row in cursor:
                    #assert len(row) == len(unscrambles)
                    output.append(tuple([
                        convert(row[idx])
                        for idx, convert in unscrambles]))
            return output
        return run_sql

    def __yaml__(self):
        yield ('sql', self.sql+'\n')
        if self.input_domains:
            yield ('input', [str(domain)
                             for domain in self.input_domains])
        if self.output_domains:
            yield ('output', [str(domain)
                              for domain in self.output_domains])


class BatchSQLPipe(Pipe):

    def __init__(self, sql, input_domains, output_domains, batch):
        self.sql = sql
        self.input_domains = input_domains
        self.output_domains = output_domains
        self.batch = batch

    def __call__(self):
        def run_sql(input, sql=self.sql.encode('utf-8'),
                           input_domains=self.input_domains,
                           output_domains=self.output_domains,
                           batch=self.batch):
            if not context.env.can_read:
                raise PermissionError("No read permissions")
            scrambles = None
            if input_domains is not None:
                scrambles = [scramble(domain) for domain in input_domains]
            unscrambles = [unscramble(domain) for domain in output_domains]
            with transaction() as connection:
                cursor = connection.cursor()
                if scrambles is None:
                    assert input is None
                    cursor.execute(sql)
                else:
                    assert isinstance(input, (tuple, list))
                    assert len(input) == len(scrambles)
                    parameters = dict((str(index+1), scramble(item))
                            for index, (item, scramble)
                                    in enumerate(zip(input, scrambles)))
                    cursor.execute(sql, parameters)
                chunk = cursor.fetchmany(batch)
                chunk = [tuple([convert(item)
                                for item, convert in zip(row, unscrambles)])
                         for row in chunk]
                if len(chunk) < batch:
                    return chunk
                stream = tempfile.TemporaryFile()
                size = 0
                while chunk:
                    size += 1
                    pickle.dump(chunk, stream, 2)
                    chunk = cursor.fetchmany(batch)
                    chunk = [tuple([convert(item)
                                    for item, convert in zip(row, unscrambles)])
                             for row in chunk]
                stream.seek(0)
                def iterate(stream=stream, size=size, load=pickle.load):
                    for k in range(size):
                        for row in load(stream):
                            yield row
                return iterate(stream, size)
        return run_sql

    def __yaml__(self):
        yield ('sql', self.sql+'\n')
        if self.input_domains:
            yield ('input', [str(domain)
                             for domain in self.input_domains])
        if self.output_domains:
            yield ('output', [str(domain)
                              for domain in self.output_domains])
        yield ('batch', self.batch)


class ProducePipe(Pipe):

    def __init__(self, meta, data_pipe, **properties):
        self.meta = meta
        self.data_pipe = data_pipe
        self.properties = properties

    def __call__(self):
        def produce(input, make_data=self.data_pipe(),
                           meta=self.meta,
                           pipe=self,
                           properties=self.properties):
            return Product(meta, make_data(input), pipe=pipe, **properties)
        return produce

    def __yaml__(self):
        yield ('meta', str(self.meta))
        yield ('data', self.data_pipe)


class ValuePipe(Pipe):

    def __init__(self, data):
        self.data = data

    def __call__(self):
        def make_value(input, data=self.data):
            return data
        return make_value

    def __yaml__(self):
        yield ('data', self.data)


class RecordPipe(Pipe):

    def __init__(self, field_pipes, record_class=tuple):
        self.field_pipes = field_pipes
        self.record_class = record_class

    def __call__(self):
        make_fields = [field_pipe() for field_pipe in self.field_pipes]
        def make_record(input, make_fields=make_fields,
                               record_class=self.record_class):
            return record_class([make_field(input)
                                 for make_field in make_fields])
        return make_record

    def __yaml__(self):
        yield ('fields', self.field_pipes)


class ExtractPipe(Pipe):

    def __init__(self, index):
        self.index = index

    def __call__(self):
        return operator.itemgetter(self.index)

    def __yaml__(self):
        yield ('index', self.index)


class SinglePipe(Pipe):

    def __init__(self):
        pass

    def __call__(self):
        def make_single(input):
            assert len(input) <= 1
            if input:
                return input[0]
        return make_single


class IteratePipe(Pipe):

    def __init__(self, value_pipe):
        self.value_pipe = value_pipe

    def __call__(self):
        def iterate(input, make_value=self.value_pipe()):
            if isinstance(input, list):
                return list(map(make_value, input))
            else:
                return (make_value(item) for item in input)
        return iterate

    def __yaml__(self):
        yield ('value', self.value_pipe)


class AnnihilatePipe(Pipe):

    def __init__(self, test_pipe, value_pipe):
        self.test_pipe = test_pipe
        self.value_pipe = value_pipe

    def __call__(self):
        if (isinstance(self.test_pipe, ValuePipe) and
                self.test_pipe.data is True):
            return self.value_pipe()
        def annihilate(input, test=self.test_pipe(),
                              make_value=self.value_pipe()):
            if test(input) is True:
                return make_value(input)
        return annihilate

    def __yaml__(self):
        yield ('test', self.test_pipe)
        yield ('value', self.value_pipe)


class MixPipe(Pipe):

    def __init__(self, key_pipes):
        self.key_pipes = key_pipes

    def __call__(self):
        make_keys = [key_pipe() for key_pipe in self.key_pipes]
        def mix(input, make_parent_key=make_keys[0],
                       make_kid_keys=make_keys[1:]):
            parent = input[0]
            kids = input[1:]
            kids_range = list(range(len(kids)))
            tops = [0]*len(kids)
            output = []
            for parent_row in parent:
                row = list(parent_row)
                parent_key = make_parent_key(parent_row)
                for idx in kids_range:
                    kid = kids[idx]
                    top = tops[idx]
                    make_kid_key = make_kid_keys[idx]
                    kid_rows = []
                    while (top < len(kid) and
                           make_kid_key(kid[top]) == parent_key):
                        kid_rows.append(kid[top])
                        top += 1
                    tops[idx] = top
                    row.append(kid_rows)
                output.append(tuple(row))
            for idx in kids_range:
                assert tops[idx] == len(kids[idx])
            return output
        return mix

    def __yaml__(self):
        yield ('keys', self.key_pipes)


