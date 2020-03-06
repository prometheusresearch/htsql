#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


#
# This is a setup script for a development version of HTSQL.
# Type `python setup.py install` to install HTSQL;
# type `python setup.py develop` to install it in a development mode.
#


from setuptools import setup, find_packages
from setuptools.command.egg_info import egg_info as setuptools_egg_info
from distutils.cmd import Command
from distutils.dir_util import remove_tree
from distutils import log
from Cython.Build import cythonize
import os, os.path, re, hashlib, urllib.request, urllib.error, urllib.parse, io, zipfile


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
    ]


def get_vendors():
    # Vendor packages to download `(url, md5, target)`.
    return [
        ('http://code.jquery.com/jquery-1.6.4.min.js',
         '9118381924c51c89d9414a311ec9c97f',
         'src/htsql/tweak/shell/vendor/jquery-1.6.4'),
        ('http://codemirror.net/codemirror-2.13.zip',
         '211de80f62d67c2475cd189d295191ff',
         'src/htsql/tweak/shell/vendor/codemirror-2.13'),
    ]


class htsql_download_vendor(Command):
    # Download vendor packages.

    user_options = []

    def initialize_options(self):
        pass

    def finalize_options(self):
        pass

    def run(self):
        info_cmd = self.get_finalized_command('egg_info')
        setup_dir = os.path.dirname(os.path.abspath(__file__))
        for url, md5_hash, target in get_vendors():
            target = os.path.join(setup_dir, target)
            if os.path.exists(target):
                continue
            log.info("downloading vendor package '%s'" % url)
            stream = urllib.request.urlopen(url)
            data = stream.read()
            stream.close()
            assert hashlib.md5(data).hexdigest() == md5_hash
            build_dir = os.path.join(info_cmd.egg_info,
                                     os.path.basename(target))
            if os.path.exists(build_dir):
                remove_tree(build_dir)
            os.makedirs(build_dir)
            if url.endswith('.zip'):
                archive = zipfile.ZipFile(io.BytesIO(data))
                entries = archive.infolist()
                assert entries
                common = entries[0].filename
                if not (common.endswith('/') and
                        all(entry.filename.startswith(common)
                            for entry in entries)):
                    common = ''
                for entry in entries:
                    filename = entry.filename[len(common):]
                    if filename.startswith('/'):
                        filename = filename[1:]
                    if not filename:
                        continue
                    filename = os.path.join(build_dir, filename)
                    if filename.endswith('/'):
                        os.mkdir(filename)
                    else:
                        stream = open(filename, 'wb')
                        stream.write(archive.read(entry))
                        stream.close()
            else:
                filename = os.path.join(build_dir,
                                        os.path.basename(url))
                stream = open(filename, 'wb')
                stream.write(data)
                stream.close()
            target_base = os.path.dirname(target)
            if not os.path.exists(target_base):
                os.makedirs(target_base)
            os.rename(build_dir, target)


class htsql_egg_info(setuptools_egg_info):
    # Make sure `download_vendor` is executed as early as possible.

    def find_sources(self):
        self.run_command('download_vendor')
        setuptools_egg_info.find_sources(self)


if __name__ == '__main__':
    setup(name="HTSQL",
          version=get_version(),
          packages=find_packages('src'),
          package_dir={'': 'src'},
          ext_modules=cythonize('src/htsql/_htsql_speedups.pyx'),
          include_package_data=True,
          zip_safe=False,
          entry_points={
              'console_scripts': ['htsql-ctl = htsql.ctl:main'],
              'htsql.routines': get_routines(),
              'htsql.addons': get_addons()},
          cmdclass={
              'download_vendor': htsql_download_vendor,
              'egg_info': htsql_egg_info})


