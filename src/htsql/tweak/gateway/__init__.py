#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from . import command
from ...core.addon import Addon, Parameter
from ...core.validator import MapVal, NameVal, DBVal
from .command import BindGateway


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
            Parameter('gateways', MapVal(NameVal(), DBVal()),
                      default={},
                      value_name="{NAME:DB}",
                      hint="""gateway definitions"""),
    ]

    def __init__(self, app, attributes):
        super(TweakGatewayAddon, self).__init__(app, attributes)
        self.functions = {}
        for name in sorted(self.gateways):
            db = self.gateways[name]
            instance = app.__class__(db)
            class_name = "Bind%s" % name.title().replace('_', '').encode('utf-8')
            namespace = {
                '__names__': [(name.encode('utf-8'), 1)],
                'instance': instance,
            }
            bind_class = type(class_name, (BindGateway,), namespace)
            self.functions[name] = bind_class


