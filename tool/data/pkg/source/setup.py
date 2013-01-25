#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


#
# To install ${NAME}, run `python setup.py install`.
#


from setuptools import setup
import os.path


NAME = "${NAME}"
VERSION = "${VERSION}"
DESCRIPTION = "${DESCRIPTION}"
LONG_DESCRIPTION = open(os.path.join(os.path.dirname(__file__),
                                     "README")).read()
AUTHOR = "${AUTHOR}"
AUTHOR_EMAIL = "${AUTHOR_EMAIL}"
LICENSE = "${LICENSE}"
KEYWORDS = "${KEYWORDS}"
PLATFORMS = "${PLATFORMS}"
URL = "${URL}"
CLASSIFIERS = """
${CLASSIFIERS}""".strip().splitlines() or None
PACKAGE_DIR = {'': '${PACKAGE_DIR}'}
INCLUDE_PACKAGE_DATA = True
ZIP_SAFE = False
PACKAGES = """
${PACKAGES}""".strip().splitlines()
INSTALL_REQUIRES = """
${INSTALL_REQUIRES}""".strip().splitlines()
CONSOLE_SCRIPTS = """
${CONSOLE_SCRIPTS}""".strip().splitlines() or None
HTSQL_ROUTINES = """
${HTSQL_ROUTINES}""".strip().splitlines() or None
HTSQL_ADDONS = """
${HTSQL_ADDONS}""".strip().splitlines() or None
ENTRY_POINTS = {}
if CONSOLE_SCRIPTS:
    ENTRY_POINTS['console_scripts'] = CONSOLE_SCRIPTS
if HTSQL_ROUTINES:
    ENTRY_POINTS['htsql.routines'] = HTSQL_ROUTINES
if HTSQL_ADDONS:
    ENTRY_POINTS['htsql.addons'] = HTSQL_ADDONS


setup(name=NAME,
      version=VERSION,
      description=DESCRIPTION,
      long_description=LONG_DESCRIPTION,
      author=AUTHOR,
      author_email=AUTHOR_EMAIL,
      license=LICENSE,
      keywords=KEYWORDS,
      platforms=PLATFORMS,
      url=URL,
      classifiers=CLASSIFIERS,
      package_dir=PACKAGE_DIR,
      include_package_data=INCLUDE_PACKAGE_DATA,
      zip_safe=ZIP_SAFE,
      packages=PACKAGES,
      install_requires=INSTALL_REQUIRES,
      entry_points=ENTRY_POINTS)


