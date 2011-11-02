#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import filecolor, python, setup_py, sphinx
import os, os.path, shutil, glob

# Fabric commands.
__all__ = ['build', 'install', 'develop', 'doc', 'dist', 'pypi', 'clean']


def build():
    """build HTSQL"""
    setup_py("build --build-base=build/lib")
    print
    print "HTSQL is built successfully."


def install():
    """install HTSQL"""
    setup_py("build --build-base=build/lib install")
    print
    print "HTSQL is installed.  For information on usage, run"
    print "    %s" % filecolor("htsql-ctl help")


def develop():
    """install HTSQL in a development mode"""
    setup_py("develop")
    print
    print "HTSQL is installed in a development mode.  Any changes"
    print "to the source tree affect subsequent calls of %s." \
          % filecolor("htsql-ctl")


def doc():
    """build HTSQL documentation"""
    sphinx("-b html doc build/doc")
    print
    print "To see the compiled documentation, open"
    print "    %s" % filecolor("./build/doc/index.html")


def dist():
    """build the source distribution"""
    setup_py("sdist --formats=zip,gztar --dist-dir=build/dist")
    print
    print "The generated source distribution archives are put in"
    print "    %s" % filecolor("./build/dist/")


def pypi():
    """build the source distribution and upload it to PyPI"""
    setup_py("sdist --formats=zip,gztar --dist-dir=build/dist"
             " register upload")
    print
    print "Source distribution archives are uploaded to PyPI."


def clean():
    """delete generated files"""
    if os.path.exists("build"):
        print "removing ./build"
        shutil.rmtree("build")
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if filename.endswith(".pyc") or filename.endswith(".pyo"):
                filename = os.path.join(dirpath, filename)
                print "removing %s" % filename
                os.unlink(filename)
    for filename in glob.glob("HTSQL-*"):
        if os.path.isdir(filename):
            print "removing ./%s" % filename
            shutil.rmtree(filename)


