#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#

# This is a fabfile for fabric; run `fab -l` to get a list of commands.

from .dist import *
from .test import *
from .buildbot import *
import types


# Replace '_' with '-' in command names.
for key, value in sorted(vars().items()):
    if isinstance(value, types.FunctionType) and '_' in key:
        del vars()[key]
        vars()[key.replace('_', '-')] = value


