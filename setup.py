#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

# The setup script for HTSQL; requires setuptools to run.
#
# Type `python setup.py install` to install HTSQL, or see `INSTALL`
# for the list of prerequisites and detailed installation instructions.

from setuptools import setup, find_packages
from setuptools.command.sdist import sdist as _sdist, log
import sys, os, os.path, shutil, re

# We use the merged content of `README`, `INSTALL`, and `NEWS` as the
# long description of the package.

root = os.path.dirname(__file__)
README = open(os.path.join(root, 'README')).read()
INSTALL = open(os.path.join(root, 'INSTALL')).read()
NEWS = open(os.path.join(root, 'NEWS')).read()

# Extract the package version from `src/htsql/__init__.py`.

INIT_PY = open(os.path.join(root, 'src/htsql/__init__.py')).read()
INIT_PY_VERSION = re.search(r"__version__ = '(?P<version>[^']+)'",
                            INIT_PY).group('version')

# The distutils parameters are defined here.  Do not forget to update
# the `__version__` attribute in `src/htsql/__init__.py` and `version`
# and `release` attributes in `doc/conf.py` any time the `VERSION`
# parameter is updated here.

NAME = "HTSQL"
VERSION = INIT_PY_VERSION
DESCRIPTION = "Query language for the accidental programmer"
LONG_DESCRIPTION = "\n".join([README, INSTALL, NEWS])
AUTHOR = "Clark C. Evans and Kirill Simonov; Prometheus Research, LLC"
AUTHOR_EMAIL = "cce@clarkevans.com"
LICENSE = "Free To Use But Restricted"
PLATFORMS = "Any"
URL = "http://htsql.org/"
CLASSIFIERS = [
    "Development Status :: 4 - Beta",
    "Environment :: Console",
    "Environment :: Web Environment",
    "Intended Audience :: Developers",
    "Intended Audience :: Information Technology",
    "Intended Audience :: Science/Research",        
    "License :: Free To Use But Restricted",
    "License :: Other/Proprietary License",
    "Programming Language :: Python",
    "Programming Language :: Python :: 2.5",
    "Programming Language :: Python :: 2.6",
    "Programming Language :: Python :: 2.7",
    "Programming Language :: SQL",
    "Topic :: Database :: Front-Ends",
    "Topic :: Internet :: WWW/HTTP :: WSGI",
    "Topic :: Software Development :: Libraries",
]
KEYWORDS = "sql http uri relational database"
PACKAGES = find_packages('src')
PACKAGE_DIR = {'': 'src'}
INCLUDE_PACKAGE_DATA = True
ZIP_SAFE = False
ENTRY_POINTS = {
    'console_scripts': ['htsql-ctl = htsql_ctl:main'],
    'htsql.addons': [
        'htsql = htsql:HTSQLAddon',
        'engine = htsql_engine:EngineAddon',
        'engine.sqlite = htsql_engine.sqlite:EngineSQLiteAddon',
        'engine.pgsql = htsql_engine.pgsql:EnginePGSQLAddon',
        'engine.mysql = htsql_engine.mysql:EngineMySQLAddon',
        'engine.oracle = htsql_engine.oracle:EngineOracleAddon',
        'engine.mssql = htsql_engine.mssql:EngineMSSQLAddon',
        'tweak = htsql_tweak:TweakAddon',
        'tweak.autolimit = htsql_tweak.autolimit:TweakAutolimitAddon',
        'tweak.cors = htsql_tweak.cors:TweakCORSAddon',
        'tweak.hello = htsql_tweak.hello:TweakHelloAddon',
        'tweak.django = htsql_tweak.django:TweakDjangoAddon',
        'tweak.inet = htsql_tweak.inet:TweakINetAddon',
        'tweak.inet.pgsql = htsql_tweak.inet.pgsql:TweakINetPGSQLAddon',
        'tweak.meta = htsql_tweak.meta:TweakMetaAddon',
        'tweak.meta.slave = htsql_tweak.meta.slave:TweakMetaSlaveAddon',
        'tweak.override = htsql_tweak.override:TweakOverrideAddon',
        'tweak.resource = htsql_tweak.resource:TweakResourceAddon',
        'tweak.shell = htsql_tweak.shell:TweakShellAddon',
        'tweak.shell.default = htsql_tweak.shell.default:TweakShellDefaultAddon',
        'tweak.sqlalchemy = htsql_tweak.sqlalchemy:TweakSQLAlchemyAddon',
        'tweak.system = htsql_tweak.system:TweakSystemAddon',
        'tweak.system.pgsql = htsql_tweak.system.pgsql:TweakSystemPGSQLAddon',
        'tweak.timeout = htsql_tweak.timeout:TweakTimeoutAddon',
        'tweak.timeout.pgsql'
            ' = htsql_tweak.timeout.pgsql:TweakTimeoutPGSQLAddon',
        'tweak.view = htsql_tweak.view:TweakViewAddon',
        'tweak.view.pgsql = htsql_tweak.view.pgsql:TweakViewPGSQLAddon',
    ],
}
INSTALL_REQUIRES = [
    'setuptools>=0.6c9',
    'pyyaml>=3.07',
#    'psycopg2>=2.0.10',
#    'MySQL-python>=1.2.2',
#    'cx_Oracle>=5.0',
#    'pymssql>=1.0.2',
]

# Override the default sdist command to include compiled documentation.
class sdist(_sdist):

    def make_release_tree(self, base_dir, files):
        # On `--dry-run`, create the `base_dir`; otherwise setuptools fails.
        if self.dry_run:
            if not os.path.exists(base_dir):
                os.mkdir(base_dir)
        # Populate the release tree.
        _sdist.make_release_tree(self, base_dir, files)
        # Remove `base_dir` if we previously created it.
        if self.dry_run:
            shutil.rmtree(base_dir)
        # `setup.cfg` is overriden by setuptools, so copy it back.
        release_setup_cfg = os.path.join(base_dir, 'setup.cfg')
        if os.path.exists(release_setup_cfg):
            os.unlink(release_setup_cfg)
        self.copy_file('setup.cfg', release_setup_cfg)
        # Check if we have source documentation.
        if not os.path.isfile('doc/conf.py'):
            return
        # Check if we have Sphinx installed.
        try:
            from sphinx.application import Sphinx
        except ImportError:
            log.warn("Sphinx is not installed"
                      " -- unable to compile documentation")
        # Make sure `sphinxext_*` packages could be found.
        sys.path.append(os.path.join(root, 'src'))
        # Instantiate and run Sphinx builder.
        log.info("compiling documentation")
        # Do not do anything on `--dry-run`.
        if self.dry_run:
            return
        srcdir = os.path.abspath('doc')
        confdir = srcdir
        outdir = os.path.abspath(os.path.join(base_dir, 'doc'))
        if not os.path.isdir(outdir):
            os.mkdir(outdir)
        doctreedir = os.path.join(self.dist_dir, '.doctrees')
        buildername = 'html'
        sphinx = Sphinx(srcdir, confdir, outdir, doctreedir, buildername,
                        confoverrides=None, status=None,
                        warningiserror=True, freshenv=True)
        sphinx.build()
        if os.path.exists(doctreedir):
            shutil.rmtree(doctreedir)
        if sphinx.statuscode:
            log.error("failed to compile documentation!")

CMDCLASS = {
    'sdist': sdist,
}


setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      platforms=PLATFORMS,
      url=URL,
      classifiers=CLASSIFIERS,
      keywords=KEYWORDS,
      packages=PACKAGES,
      package_dir=PACKAGE_DIR,
      include_package_data=INCLUDE_PACKAGE_DATA,
      zip_safe=ZIP_SAFE,
      entry_points=ENTRY_POINTS,
      install_requires=INSTALL_REQUIRES,
      cmdclass=CMDCLASS,
)


