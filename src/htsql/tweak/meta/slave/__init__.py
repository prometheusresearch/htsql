#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import connect
from ....core.addon import Addon, Parameter
from ....core.util import DB
from ....core.validator import ClassVal


class TweakMetaSlaveAddon(Addon):

    name = 'tweak.meta.slave'
    hint = """implement meta database (auxiliary)"""

    prerequisites = []
    postrequisites = ['htsql']
    parameters = [
            Parameter('master', ClassVal(object)),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        return { 'htsql': { 'db': DB(engine='sqlite',
                                     database=':meta:',
                                     username=None,
                                     password=None,
                                     host=None, port=None) },
                 'engine.sqlite': {} }


