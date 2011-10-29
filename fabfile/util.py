#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from __future__ import with_statement
from fabric.api import local, hide, output
from fabric.colors import white, red
import os, os.path


# No fabric commands are defined here.
__all__ = []


def filecolor(message):
    # Paint filenames to bold white color.
    return white(message, bold=True)


def warncolor(message):
    # Paint warnings to bold red color.
    return red(message, bold=True)


is_fabfile_env_loaded = False
def load_fabfile_env():
    # Load environment variables from `fabfile.env`; but do not replace
    # already assignment variables -- assume those are overriden manually.

    # Load `fabfile.env` only once.
    global is_fabfile_env_loaded
    if is_fabfile_env_loaded:
        return
    is_fabfile_env_loaded = True

    # Complain if the file is not found.
    if not os.path.exists("./fabfile.env"):
        print warncolor("WARNING:"),
        print "%s does not exists!" % filecolor("fabfile.env")
        print "Please copy %s to %s" \
              % (filecolor("fabfile.env.sample"), filecolor("fabfile.env"))
        print "and customize it for your environment."
        print "Proceeding using default settings..."
        return

    # Use `env` to retrieve environment variables.
    with hide('running'):
        output = local(". ./fabfile.env && env --null", capture=True)

    # Update the environment.
    for line in output.split('\0'):
        if '=' not in line:
            continue
        key, value = line.split('=', 1)
        if key is os.environ:
            continue
        os.environ[key] = value


def execute(command):
    # Execute the command replacing the current process.
    if output.debug or output.running:
        print "[localhost] exec:", command
    command = command.split()
    os.execvp(command[0], command)


def python(command):
    # Run `python <command>`.
    load_fabfile_env()
    PYTHON = os.environ.get("PYTHON", "python")
    return local(PYTHON+" "+command)


def setup_py(command):
    # Run `python setup.py <command>`.
    return python("setup.py "+command)


def sphinx(command):
    # Run `sphinx-build <command>`.
    load_fabfile_env()
    SPHINX_BUILD = os.environ.get("SPHINX_BUILD", "sphinx-build")
    return local(SPHINX_BUILD+" "+command)


def pip(command):
    # Run `pip <command>`.
    load_fabfile_env()
    PIP = os.environ.get("PIP", "pip")
    return local(PIP+" "+command)


def htsql_ctl(command):
    # Run `htsql-ctl <command>`
    load_fabfile_env()
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    return local(HTSQL_CTL+" "+command)


def exec_htsql_ctl(command):
    # Execute `htsql-ctl <command>` replacing the current process.
    load_fabfile_env()
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    execute(HTSQL_CTL+" "+command)


def pyflakes(command):
    # Run `pyflakes <command>`.
    load_fabfile_env()
    PYFLAKES = os.environ.get("PYFLAKES", "pyflakes")
    return local(PYFLAKES+" "+command)


def coverage_py(command, coverage_file=None):
    # Run `COVERAGE_FILE=<coverage_file> coverage <command>`.
    load_fabfile_env()
    COVERAGE = os.environ.get("COVERAGE", "coverage")
    if coverage_file is not None:
        COVERAGE = "COVERAGE_FILE=\"%s\" %s" % (coverage_file, COVERAGE)
    return local(COVERAGE+" "+command)


