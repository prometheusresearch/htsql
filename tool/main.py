#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import settings, FatalError, fatal
import help, dist, test, vm, check, pkg
import sys


def main(argv):
    # The entry point.
    parameters = []
    no_more_options = False
    for arg in argv[1:]:
        if arg.startswith('-') and not no_more_options:
            if arg == '-v':
                settings.verbose = True
            elif arg == '--':
                no_more_options = True
            else:
                raise fatal("unknown option: %s" % arg)
        else:
            parameters.append(arg)
    if not parameters:
        parameters = ['default']
    job_name = parameters.pop(0)
    for job in settings.jobs:
        if job.func_name.replace('_', '-') == job_name:
            break
    else:
        raise fatal("unknown job: %s" % job_name)
    min_args = max_args = job.func_code.co_argcount
    if job.func_defaults:
        min_args -= len(job.func_defaults)
    if job.func_code.co_flags & 0x04:   # CO_VARARGS
        max_args = None
    if len(parameters) < min_args:
        raise fatal("not enough parameters: %s expected; got %s"
                    % ((min_args if min_args == max_args
                                 else "at least %s" % min_args),
                       len(parameters)))
    if max_args is not None and len(parameters) > max_args:
        raise fatal("too many parameters: %s expected; got %s"
                    % ((max_args if max_args == min_args
                                 else "at most %s" % max_args),
                       len(parameters)))
    return job(*parameters)


if __name__ == '__main__':
    try:
        main(sys.argv)
    except (FatalError, IOError, KeyboardInterrupt), exc:
        if settings.verbose:
            raise
        sys.exit(exc)


