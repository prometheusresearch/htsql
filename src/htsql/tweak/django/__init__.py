#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from ...core.validator import StrVal
from ...core.addon import Addon, Parameter
from ...core.util import DB
import os


class TweakDjangoAddon(Addon):

    prerequisites = []
    postrequisites = ['htsql']
    name = 'tweak.django'
    hint = """provide Django integration"""
    help = """
    This addon replaces built-in database introspection and
    connection handling with Django facilities.

    Parameter `settings` is the path to the settings module.  If not
    set, environment variable `DJANGO_SETTINGS_MODULE` is used.
    """

    parameters = [
            Parameter('settings', StrVal(),
                      value_name="PROJECT.MODULE",
                      hint="""name of the `settings` module"""),
    ]

    @classmethod
    def get_extension(cls, app, attributes):
        settings = attributes['settings']
        if settings is not None:
            os.environ['DJANGO_SETTINGS_MODULE'] = settings
        from . import connect, introspect
        from django.conf import settings
        from django.db.utils import DEFAULT_DB_ALIAS
        engine = settings.DATABASES[DEFAULT_DB_ALIAS]['ENGINE']
        engine = {
                'django.db.backends.postgresql_psycopg2': 'pgsql',
                'django.db.backends.postgresql': 'pgsql',
                'django.db.backends.mysql': 'mysql',
                'django.db.backends.sqlite3': 'sqlite',
                'django.db.backends.oracle': 'oracle',
        }.get(engine, engine)
        username = settings.DATABASES[DEFAULT_DB_ALIAS]['USER']
        if not username:
            username = None
        password = settings.DATABASES[DEFAULT_DB_ALIAS]['PASSWORD']
        if not password:
            password = None
        host = settings.DATABASES[DEFAULT_DB_ALIAS]['HOST']
        if not host:
            host = None
        port = settings.DATABASES[DEFAULT_DB_ALIAS]['PORT']
        if not port:
            port = None
        else:
            port = int(port)
        database = settings.DATABASES[DEFAULT_DB_ALIAS]['NAME']
        return {
            'htsql': {
                'db': DB(engine=engine,
                         username=username,
                         password=password,
                         host=host,
                         port=port,
                         database=database),
                },
            'engine.%s' % engine : {},
        }


