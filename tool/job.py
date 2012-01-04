#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


import sys, os, re, shutil, subprocess


class settings(object):
    # Global variables.

    jobs = []
    verbose = False
    vms = []


class FatalError(Exception):
    pass


def job(function):
    # Decorate the function as a job.
    settings.jobs.append(function)
    return function


def colorize(msg):
    # Convert backtick-quoted substring to bold white color.
    def expand(msg, pattern, escape_in, escape_out):
        regexp = re.compile(pattern)
        def replace(match):
            data = match.group('data')
            return escape_in+data+escape_out
        return regexp.sub(replace, msg)
    msg = expand(msg, r'`(?P<data>[^`]+)`', "\x1b[1;37m", "\x1b[0m")
    msg = expand(msg, r'\[!!\] (?P<data>[^:!]+[:!])', "\x1b[1;31m", "\x1b[0m")
    return msg

def out(*msgs, **opts):
    # Display given messages.
    sep = opts.pop('sep', " ")
    end = opts.pop('end', "\n")
    stream = opts.pop('file', sys.stdout)
    assert not opts
    with_color = (stream.isatty() and os.environ.get('COLORTERM'))
    data = sep.join((colorize(msg)
                     if with_color and isinstance(msg, str) else str(msg))
                    for msg in msgs)+end
    stream.write(data)
    stream.flush()


def log(*msgs, **opts):
    # Dump the given messages to the standard output.
    return out(file=sys.stdout, *msgs, **opts)


def debug(*msgs, **opts):
    # Display debug information.
    if settings.verbose:
        return log(*msgs, **opts)


def warn(*msgs, **opts):
    # Dump the given messages to the standard error stream.
    return out("[!!] WARNING:", file=sys.stderr, *msgs, **opts)


def fatal(*msgs, **opts):
    # Dump the error messages and exit the program.
    out("[!!] FATAL ERROR:", file=sys.stderr, *msgs, **opts)
    return FatalError()


def prompt(msg):
    value = ""
    while not value:
        value = raw_input(msg+" ").strip()
    return value


def cp(src_filename, dst_filename):
    # Copy a file.
    debug("copying: %s => %s" % (src_filename, dst_filename))
    shutil.copy(src_filename, dst_filename)


def rm(filename):
    # Remove a file.
    debug("removing: %s" % filename)
    os.unlink(filename)


def rmtree(filename):
    # Remove a directory tree.
    debug("removing directory: %s" % filename)
    shutil.rmtree(filename)


def mktree(filename):
    # Create a directory tree.
    debug("making directory: %s" % filename)
    os.makedirs(filename)


def exe(command):
    # Execute the command replacing the current process.
    log("`%s`" % command)
    line = command.split()
    try:
        os.execvp(line[0], line)
    except OSError, exc:
        raise fatal("cannot execute command: %s" % exc)


def run(command, data=None, verbose=None):
    # Run the command.
    if verbose is None:
        verbose = settings.verbose
    stream = subprocess.PIPE
    if verbose:
        stream = None
        log("running: %s" % command)
    proc = subprocess.Popen(command, shell=True, stdin=stream,
                            stdout=stream, stderr=stream)
    proc.communicate(data)
    if proc.returncode != 0:
        raise fatal("non-zero exit code: %s" % command)


def pipe(command, data=None, verbose=None):
    # Run the command, return the output.
    if verbose is None:
        verbose = settings.verbose
    stream = subprocess.PIPE
    if verbose:
        log("piping: %s" % command)
    proc = subprocess.Popen(command, shell=True, stdout=stream, stderr=stream)
    out, err = proc.communicate(data)
    if proc.returncode != 0:
        if verbose:
            if out:
                sys.stdout.write(out)
            if err:
                sys.stderr.write(err)
        raise fatal("non-zero exit code: %s" % command)
    return out


