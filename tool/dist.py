#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import job, ls, rm, rmtree, run, pipe, log
import os, os.path


def pipe_python(command, cd=None):
    # Run `python <command>` and return the output.
    PYTHON = os.environ.get("PYTHON", "python")
    return pipe(PYTHON+" "+command, cd=cd)


def python(command, cd=None):
    # Run `python <command>`.
    PYTHON = os.environ.get("PYTHON", "python")
    run(PYTHON+" "+command, verbose=True, cd=cd)


def setup_py(command, cd=None):
    # Run `python setup.py <command>`.
    python("setup.py "+command, cd=cd)


def sphinx(command, cd=None):
    # Run `sphinx-build <command>`.
    SPHINX = os.environ.get("SPHINX_BUILD", "sphinx-build")
    run(SPHINX+" "+command, verbose=True, cd=cd)


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
    for filename in ls("./HTSQL-*"):
        if os.path.isdir(filename):
            rmtree(filename)


