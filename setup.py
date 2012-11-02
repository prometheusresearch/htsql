#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


#
# This is a setup script for a development version of HTSQL.
# Type `python setup.py install` to install HTSQL;
# type `python setup.py develop` to install it in a development mode.
#


from setuptools import setup, find_packages
import os.path, re


def get_version():
    # Fetch `htsql.__version__`.
    root = os.path.dirname(__file__)
    source = open(os.path.join(root, 'src/htsql/__init__.py')).read()
    version = re.search(r"__version__ = '(?P<version>[^']+)'",
                        source).group('version')
    return version


def get_routines():
    # Get all provided routines.
    # FIXME: move to an external file.
    return [
        'default = htsql.ctl.default:DefaultRoutine',
        'help = htsql.ctl.help:HelpRoutine',
        'version = htsql.ctl.version:VersionRoutine',
        'extension = htsql.ctl.extension:ExtensionRoutine',
        'server = htsql.ctl.server:ServerRoutine',
        'shell = htsql.ctl.shell:ShellRoutine',
        'regress = htsql.ctl.regress:RegressRoutine',
        'ui = htsql_ui.ctl.ui:UIRoutine',
    ]


def get_addons():
    # Get all exported addons.
    # FIXME: move to an external file or introspect from the source.
    return [
        'htsql = htsql.core:HTSQLAddon',
        'engine = htsql.core:EngineAddon',
        'engine.sqlite = htsql_sqlite.core:EngineSQLiteAddon',
        'engine.pgsql = htsql_pgsql.core:EnginePGSQLAddon',
        'engine.mysql = htsql_mysql.core:EngineMySQLAddon',
        'engine.oracle = htsql_oracle.core:EngineOracleAddon',
        'engine.mssql = htsql_mssql.core:EngineMSSQLAddon',
        'tweak = htsql.tweak:TweakAddon',
        'tweak.autolimit = htsql.tweak.autolimit:TweakAutolimitAddon',
        'tweak.cors = htsql.tweak.cors:TweakCORSAddon',
        'tweak.csrf = htsql.tweak.csrf:TweakCSRFAddon',
        'tweak.django = htsql.tweak.django:TweakDjangoAddon',
        'tweak.etl = htsql.tweak.etl:TweakETLAddon',
        'tweak.filedb = htsql.tweak.filedb:TweakFileDBAddon',
        'tweak.gateway = htsql.tweak.gateway:TweakGatewayAddon',
        'tweak.hello = htsql.tweak.hello:TweakHelloAddon',
        'tweak.inet = htsql.tweak.inet:TweakINetAddon',
        'tweak.inet.pgsql = htsql_pgsql.tweak.inet:TweakINetPGSQLAddon',
        'tweak.meta = htsql.tweak.meta:TweakMetaAddon',
        'tweak.meta.slave = htsql.tweak.meta.slave:TweakMetaSlaveAddon',
        'tweak.override = htsql.tweak.override:TweakOverrideAddon',
        'tweak.pool = htsql.tweak.pool:TweakPoolAddon',
        'tweak.resource = htsql.tweak.resource:TweakResourceAddon',
        'tweak.shell = htsql.tweak.shell:TweakShellAddon',
        'tweak.shell.default = htsql.tweak.shell.default:TweakShellDefaultAddon',
        'tweak.sqlalchemy = htsql.tweak.sqlalchemy:TweakSQLAlchemyAddon',
        'tweak.system = htsql.tweak.system:TweakSystemAddon',
        'tweak.system.pgsql = htsql_pgsql.tweak.system:TweakSystemPGSQLAddon',
        'tweak.timeout = htsql.tweak.timeout:TweakTimeoutAddon',
        'tweak.timeout.pgsql'
            ' = htsql_pgsql.tweak.timeout:TweakTimeoutPGSQLAddon',
        'tweak.view = htsql.tweak.view:TweakViewAddon',
        'tweak.view.pgsql = htsql_pgsql.tweak.view:TweakViewPGSQLAddon',
    ]


if __name__ == '__main__':
    setup(name="HTSQL",
          version=get_version(),
          packages=find_packages('src'),
          package_dir={'': 'src'},
          include_package_data=True,
          zip_safe=False,
          entry_points={
              'console_scripts': ['htsql-ctl = htsql.ctl:main'],
              'htsql.routines': get_routines(),
              'htsql.addons': get_addons()})


