#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from cogs import task, setting, env
from cogs.fs import sh, pipe, rm, rmtree
from cogs.log import log
import os, os.path
import glob


def pipe_python(command, cd=None):
    # Run `python <command>` and return the output.
    return pipe(env.python_path+" "+command, cd=cd)


def python(command, cd=None):
    # Run `python <command>`.
    with env(debug=True):
        sh(env.python_path+" "+command, cd=cd)


def setup_py(command, cd=None):
    # Run `python setup.py <command>`.
    python("setup.py "+command, cd=cd)


def sphinx(command, cd=None):
    # Run `sphinx-build <command>`.
    with env(debug=True):
        sh(env.sphinx_path+" "+command, cd=cd)


@setting
def PYTHON(path=None):
    """path to Python executable"""
    if not path:
        path = 'python'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(python_path=path)


@setting
def SPHINX_BUILD(path=None):
    """path to sphinx-build executable"""
    if not path:
        path = 'sphinx-build'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(sphinx_path=path)


@task
def BUILD():
    """build HTSQL library

    This task copies HTSQL source files to the build directory:
      `./build/lib`
    """
    setup_py("build --build-base=build/lib")
    log()
    log("HTSQL is successfully built.")
    log()


@task
def INSTALL():
    """install HTSQL

    This task installs HTSQL to the local filesystem.

    This task may require root privileges.
    """
    setup_py("build --build-base=build/lib install")
    log()
    log("HTSQL is installed.  For information on usage, run:")
    log("  `htsql-ctl help`")
    log()


@task
def DEVELOP():
    """install HTSQL in a development mode

    This task installs HTSQL in a development mode so that any changes
    to the source tree affect subsequent calls of `htsql-ctl`.
    """
    setup_py("develop")
    log()
    log("HTSQL is installed in a development mode.  Any changes")
    log("to the source tree will affect subsequent calls of `htsql-ctl`.")
    log()


@task
def DOC():
    """build HTSQL documentation

    This task compiles HTSQL documentation and places it to:
      `./build/doc/`
    """
    sphinx("-b html doc build/doc")
    log()
    log("To see the generated documentation, open:")
    log("  `./build/doc/index.html`")
    log()


@task
def CLEAN():
    """delete generated files

    This task deletes generated files.
    """
    if os.path.exists("./build"):
        rmtree("./build")
    if os.path.exists("./dist"):
        rmtree("./dist")
    for dirpath, dirnames, filenames in os.walk("."):
        for filename in filenames:
            if filename.endswith(".pyc") or filename.endswith(".pyo"):
                filename = os.path.join(dirpath, filename)
                rm(filename)
        for dirname in dirnames:
            if dirname == "vendor":
                dirname = os.path.join(dirpath, dirname)
                rmtree(dirname)
    for filename in glob.glob("./HTSQL-*"):
        if os.path.isdir(filename):
            rmtree(filename)


