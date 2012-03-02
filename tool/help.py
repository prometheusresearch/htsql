#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


from job import settings, job, log, fatal


@job
def default():
    # The default job.
    log("JOB - HTSQL development and build tool")
    log("Copyright (c) 2006-2012, Prometheus Research, LLC")
    log()
    log("Run `job help` for general usage and a list of jobs.")
    log("Run `job help <job>` for help on a specific job.")


def job_usage(job):
    # Generate a usage string for a job function.
    name = job.func_name.replace('_', '-')
    argcount = job.func_code.co_argcount
    optional = 0
    if job.func_defaults:
        optional = len(job.func_defaults)
    arguments = ["<%s>" % argument
                 for argument in job.func_code.co_varnames[:argcount]]
    if job.func_code.co_flags & 0x04:   # CO_VARARGS:
        arguments.append("<%s>..." % job.func_code.co_varnames[argcount])
        argcount += 1
        optional += 1
    for idx in range(len(arguments)-optional, len(arguments)):
        arguments[idx] = "["+arguments[idx]
    if optional:
        arguments[-1] += "]"*optional
    if not arguments:
        return name
    return (name+" "+" ".join(arguments))


def job_hint(job):
    # Generate a hint string for a job function.
    if not job.func_doc:
        return ""
    return job.func_doc.lstrip().splitlines()[0].rstrip()


def job_help(job):
    # Generate a help string for a job function.
    if not job.func_doc:
        return ""
    lines = job.func_doc.strip().splitlines()[1:]
    while lines and not lines[0].strip():
        lines.pop(0)
    while lines and not lines[-1].strip():
        lines.pop(-1)
    indent = None
    for line in lines:
        short_line = line.lstrip()
        if short_line:
            line_indent = len(line)-len(short_line)
            if indent is None or line_indent < indent:
                indent = line_indent
    if indent:
        lines = [line[indent:] for line in lines]
    return "\n".join(lines)


@job
def help(name=None):
    """describe the tool and its jobs

    Run `job help` to describe the usage of the tool and to get
    a list of supported jobs.

    Run `job help <job>` to describe the usage of the specified job.
    """
    if name is None:
        log("JOB - HTSQL development and build tool")
        log("Copyright (c) 2006-2012, Prometheus Research, LLC")
        log("Usage: `job [-v] <job> [<arguments>...]`")
        log()
        log("Run `job help` for general usage and a list of jobs.")
        log("Run `job help <job>` for help on a specific job.")
        log()
        log("File `job.env` sets environment variables that may affect")
        log("the tool.  Copy a sample file `job.env.sample` to `job.env`")
        log("and customize it for your configuration.")
        log()
        log("Available jobs:")
        for job in settings.jobs:
            if job.func_doc is None:
                continue
            log("  %-24s : %s" % (job_usage(job), job_hint(job)))
        log()
        log("Global options:")
        log("  %-24s : %s" % ("-v", "display debug information"))
        log()
    else:
        for job in settings.jobs:
            if job.func_doc is None:
                continue
            if job.func_name.replace('_', '-') == name:
                break
        else:
            raise fatal("unknown job: %s" % name)
        name = job.func_name.replace('_', '-').upper()
        hint = job_hint(job)
        if hint:
            log("%s - %s" % (name, hint))
        else:
            log(name)
        log("Usage: `job %s`" % job_usage(job))
        log()
        help = job_help(job)
        if help:
            log(help)
            log()


