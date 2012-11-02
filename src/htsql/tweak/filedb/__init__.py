#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect
from ...core.addon import Addon, Parameter
from ...core.util import DB
from ...core.validator import StrVal, SeqVal, RecordVal


class TweakFileDBAddon(Addon):

    name = 'tweak.filedb'
    hint = """make a database from a set of CSV files"""

    prerequisites = []
    postrequisites = ['htsql']
    parameters = [
            Parameter('sources', SeqVal(RecordVal([
                ("file", StrVal())])),
                default=[]),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        return { 'htsql': { 'db': DB(engine='sqlite',
                                     database=':filedb:',
                                     username=None,
                                     password=None,
                                     host=None, port=None) },
                 'engine.sqlite': {} }


