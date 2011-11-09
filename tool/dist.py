#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import job, rm, rmtree, run, log
import os, os.path, glob


def python(command):
    # Run `python <command>`.
    PYTHON = os.environ.get("PYTHON", "python")
    run(PYTHON+" "+command, verbose=True)


def setup_py(command):
    # Run `python setup.py <command>`.
    python("setup.py "+command)


def sphinx(command):
    # Run `sphinx-build <command>`.
    SPHINX = os.environ.get("SPHINX_BUILD", "sphinx-build")
    run(SPHINX+" "+command, verbose=True)


@job
def build():
    """build HTSQL library

    This job copies HTSQL source files to the build directory:
      `./build/lib`
    """
    setup_py("build --build-base=build/lib")
    log()
    log("HTSQL is successfully built.")
    log()


@job
def install():
    """install HTSQL

    This job installs HTSQL to the local filesystem.

    This job may require root privileges.
    """
    setup_py("build --build-base=build/lib install")
    log()
    log("HTSQL is installed.  For information on usage, run:")
    log("  `htsql-ctl help`")
    log()


@job
def develop():
    """install HTSQL in a development mode

    This job installs HTSQL in a development mode so that any changes
    to the source tree affect subsequent calls of `htsql-ctl`.
    """
    setup_py("develop")
    log()
    log("HTSQL is installed in a development mode.  Any changes")
    log("to the source tree will affect subsequent calls of `htsql-ctl`.")
    log()


@job
def doc():
    """build HTSQL documentation

    This job compiles HTSQL documentation and places it to:
      `./build/doc/`
    """
    sphinx("-b html doc build/doc")
    log()
    log("To see the generated documentation, open:")
    log("  `./build/doc/index.html`")
    log()


@job
def dist():
    """build the source distribution

    This job builds `zip` and `tar.gz` source distributions and places
    them to the directory:
      `./build/dist/`
    """
    setup_py("sdist --formats=zip,gztar --dist-dir=build/dist")
    log()
    log("The generated source distribution archives are put in:")
    log("  `./build/dist/`")
    log()


@job
def pypi():
    """build the source distribution and upload it to PyPI

    This job builds `zip` and `tar.gz` source distributions and
    uploads them to PyPI.
    """
    setup_py("sdist --formats=zip,gztar --dist-dir=build/dist"
             " register upload --sign --identity=8E70D862")
    log()
    log("Source distribution archives are uploaded to:")
    log("  `http://pypi.python.org/pypi/HTSQL/`")
    log()


@job
def clean():
    """delete generated files

    This job deletes generated files.
    """
    if os.path.exists("./build"):
        rmtree("./build")
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if filename.endswith(".pyc") or filename.endswith(".pyo"):
                filename = os.path.join(dirpath, filename)
                rm(filename)
    for filename in glob.glob("./HTSQL-*"):
        if os.path.isdir(filename):
            rmtree(filename)


