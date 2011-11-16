#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from htsql.validator import PIntVal
from htsql.addon import Addon, Parameter, addon_registry


class TweakTimeoutAddon(Addon):

    name = 'tweak.timeout'
    hint = """limit query execution time"""
    help = """
    This addon limits all queries to a given amount of time.
    Use it to ensure against accidental denial of service caused
    by complex queries.

    Parameter `timeout` sets the timeout value (the default is 60
    seconds).

    Currently, only PostgreSQL backend is supported.
    """

    parameters = [
            Parameter('timeout', PIntVal(is_nullable=True), default=60,
                      value_name="SEC",
                      hint="""query timeout, in sec (default: 60)"""),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        if app.htsql.db is not None:
            name = '%s.%s' % (cls.name, app.htsql.db.engine)
            if name not in addon_registry:
                raise ImportError("%s is not implemented for %s"
                                  % (cls.name, app.htsql.db.engine))
            return name


