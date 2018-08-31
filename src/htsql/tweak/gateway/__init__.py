#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from . import command
from ...core.util import DB
from ...core.addon import Addon, Parameter
from ...core.validator import AnyVal, UnionVal, MapVal, NameVal, StrVal, DBVal
from .command import SummonGateway


class TweakGatewayAddon(Addon):

    name = 'tweak.gateway'
    hint = """define gateways to other databases"""
    help = """
    This addon allows you to create a gateway to another database
    and execute HTSQL queries against it.

    Parameter `gateways` is a mapping of names to connection URIs.
    Each mapping entry creates a function which takes a query
    as a parameter and execute it against the database specified
    by the connection URI.
    """

    parameters = [
            Parameter('gateways',
                      MapVal(NameVal(),
                          UnionVal([
                              DBVal(),
                              MapVal(StrVal(), AnyVal())])),
                      default={},
                      value_name="{NAME:DB}",
                      hint="""gateway definitions"""),
    ]

    def __init__(self, app, attributes):
        super(TweakGatewayAddon, self).__init__(app, attributes)
        self.functions = {}
        for name in sorted(self.gateways):
            db = self.gateways[name]
            if isinstance(db, DB):
                instance = app.__class__(db)
            else:
                instance = app.__class__(None, db)
            class_name = "Summon%s" % name.title().replace('_', '')
            namespace = {
                '__names__': [name],
                'instance': instance,
            }
            summon_class = type(class_name, (SummonGateway,), namespace)
            self.functions[name] = summon_class


