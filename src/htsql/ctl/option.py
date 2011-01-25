#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# Authors: Clark C. Evans <cce@clarkevans.com>,
#          Kirill Simonov <xi@resolvent.net>
#


"""
:mod:`htsql.ctl.option`
=======================

This module defines script options.
"""


from ..util import maybe
from ..validator import Validator, StrVal
import re


class Option(object):
    """
    Describes a script option.

    `attribute` (a string)
        The name of the routine attribute.  When the routine is
        initialized, the value of the option is assigned to the
        attribute.

    `short_name` (a string or ``None``)
        The abbrebiated form of the option (a dash + a character).

    `long_name` (a string or ``None``)
        The full form of the option (two dashes + the option name).

    `with_value` (Boolean)
        If set, the option requires a parameter.

    `value_name` (a string or ``None``)
        The name of the option parameter.

    `validator` (:class:`htsql.validator.Validator` or ``None``)
        The validator for the option parameter.

    `default`
        The default value of the parameter.

    `hint` (a string or ``None``)
        A one-line description of the option.

    When an option does not require a parameter, the value of the
    option is either ``False`` or ``True``.  If the option is
    provided in the command line, the value is ``True``; otherwise,
    the value is ``False``.

    When an option requires a parameter, the value of the option
    is determined by the parameter and the attributes `validator`
    and `default`.  If the option is not provided, its value is
    equal to `default`.  If the option is provided, its value is
    the value of the option parameter normalized by application
    of `validator`.
    """

    def __init__(self, attribute,
                 short_name=None, long_name=None,
                 with_value=False, value_name=None,
                 validator=None, default=None,
                 hint=None):
        # Sanity check on the arguments.
        assert isinstance(attribute, str)
        assert re.match(r'^[a-zA-Z_][0-9a-zA-Z_]*$', attribute)
        assert isinstance(short_name, maybe(str))
        if short_name is not None:
            assert re.match(r'^-[0-9a-zA-Z]$', short_name)
        assert isinstance(long_name, maybe(str))
        if long_name is not None:
            assert re.match(r'^--[0-9a-zA-Z][0-9a-zA-Z-]+$', long_name)
        assert short_name is not None or long_name is not None
        assert isinstance(with_value, bool)
        assert isinstance(value_name, maybe(str))
        assert isinstance(validator, maybe(Validator))
        if with_value:
            assert validator is not None
        else:
            assert value_name is None
            assert validator is None
            assert default is None
        assert isinstance(hint, maybe(str))

        self.attribute = attribute
        self.short_name = short_name
        self.long_name = long_name
        self.with_value = with_value
        self.value_name = value_name
        self.validator = validator
        self.default = default
        self.hint = hint

    def get_hint(self):
        """
        Returns a short one-line description of the option.
        """
        return self.hint

    def get_signature(self):
        """
        Returns the signature of the option parameters.
        """
        # The option signature has one of the forms:
        #   {short_name} {PARAMETER}
        #   {long_name} {PARAMETER}
        # or 
        #   {short_name} [{long_name}] {PARAMETER}
        if self.short_name is not None:
            signature = self.short_name
            if self.long_name is not None:
                signature = "%s [%s]" % (signature, self.long_name)
        else:
            signature = self.long_name
        if self.with_value:
            if self.value_name is not None:
                parameter = self.value_name
            else:
                parameter = self.attribute
            parameter = parameter.replace('_', '-').upper()
            signature = "%s %s" % (signature, parameter)
        return signature


#
# Options used by ``htsql-ctl``.
#


QuietOption = Option(
        attribute='quiet',
        short_name='-q',
        long_name='--quiet',
        hint="""display as little as possible""")

VerboseOption = Option(
        attribute='verbose',
        short_name='-v',
        long_name='--verbose',
        hint="""display more information""")

DebugOption = Option(
        attribute='debug',
        long_name='--debug',
        hint="""enable debug logging""")

ForceOption = Option(
        attribute='force',
        long_name='--force',
        hint="""force execution of the routine""")

DryRunOption = Option(
        attribute='dry_run',
        long_name='--dry-run',
        hint="""simulate execution of the routine""")

InputOption = Option(
        attribute='input',
        short_name='-i',
        long_name='--input',
        with_value=True,
        value_name="file",
        validator=StrVal(),
        hint="""set input file to FILE""")

OutputOption = Option(
        attribute='output',
        short_name='-o',
        long_name='--output',
        with_value=True,
        value_name="file",
        validator=StrVal(),
        hint="""set output file to FILE""")

TrainOption = Option(
        attribute='train',
        long_name='--train',
        hint="""train the test engine""")

PurgeOption = Option(
        attribute='purge',
        long_name='--purge',
        hint="""purge unused data""")

SlaveOption = Option(
        attribute='slave',
        long_name='--slave',
        hint="""run in the slave mode""")

RemoteUserOption = Option(
        attribute='remote_user',
        long_name='--remote-user',
        with_value=True,
        value_name="user",
        validator=StrVal(),
        hint="""set the remote user to USER""")

WithHeadersOption = Option(
        attribute='with_headers',
        long_name='--with-headers',
        hint="""display HTTP status line and headers""")

ContentTypeOption = Option(
        attribute='content_type',
        long_name='--content-type',
        with_value=True,
        value_name='type',
        validator=StrVal(r"^[0-9a-z-]+/[0-9a-z-]+$"),
        hint="""set the content type of the HTTP POST data""")


