#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from . import connect, introspect
from ...core.addon import Addon, Parameter
from ...core.util import DB
from ...core.validator import StrVal, SeqVal, RecordVal


class TweakFileDBAddon(Addon):

    name = 'tweak.filedb'
    hint = """make a database from a set of CSV files"""
    help = """
    This addon makes a database from a set of CSV files. Each source
    CSV file becomes a table in the database.  The name of the table
    is derived from the file name; the column names are taken from
    the first row of the CSV file.  The remaining rows become the
    records in the table.

    Parameter `sources` is a list of entries describing the source
    files; each entry has the following fields:

    `file`: the path to the CSV file.

    Optional parameter `cache-file` allows you to specify a persistent
    storage for the database.
    """

    prerequisites = []
    postrequisites = ['htsql']
    parameters = [
            Parameter('sources', SeqVal(RecordVal([
                ("file", StrVal())])),
                default=[],
                hint="""source CSV files"""),
            Parameter('cache_file', StrVal(),
                hint="""persistent storage"""),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        return { 'htsql': { 'db': DB(engine='sqlite',
                                     database=':filedb:',
                                     username=None,
                                     password=None,
                                     host=None, port=None) },
                 'engine.sqlite': {} }


