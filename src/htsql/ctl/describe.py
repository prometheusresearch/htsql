#
# Copyright (c) 2006-2012, Prometheus Research, LLC
#


"""
:mod:`htsql.ctl.describe`
=====================

This module implements the `describe` routine.
"""

from __future__ import with_statement
from .error import ScriptError
from .routine import Argument, Routine
from .option import PasswordOption, ExtensionsOption, ConfigOption
from .request import Request, ConfigYAMLLoader
from ..core.validator import DBVal
from ..core.util import DB, listof, trim_doc
from ..core.model import HomeNode, ChainArc, ColumnArc
from ..core.classify import classify, normalize
from .error import ScriptError
from .routine import Routine, Argument
from ..core.validator import WordVal
import os
import sys
import yaml

class DescribeRoutine(Routine):
    """
    Implements the `describe` routine.

    When called without any parameters, it enumerates tables & links.   
    When called with a table name, it describes everything about the 
    given table including its columns and data types.
    """

    name = 'describe'
    aliases = []
    arguments = [
            Argument('db', DBVal(), default=None,
                     hint="""the connection URI"""),
            Argument('table', WordVal(), default=None,
                     hint="""the name of a table to describe""")
    ]
    options = [
            PasswordOption,
            ExtensionsOption,
            ConfigOption,
    ]
    hint = """describe the metadata catalog for the database"""
    help = """
    Run '%(executable)s describe' to enumerate tables and foreign key links
    available for a given database based on introspection and configuration.

    Run '%(executable)s describe <table>' to describe a given table,
    including both links and columns.
    """

    def run(self):
        # The database URI.
        db = self.db

        # Ask for the database password if necessary.
        if self.password and db is not None:
            db = DB(engine=db.engine,
                    username=db.username,
                    password=getpass.getpass(),
                    host=db.host,
                    port=db.port,
                    database=db.database,
                    options=db.options)

        # Load addon configuration.
        extensions = self.extensions
        if self.config is not None:
            stream = open(self.config, 'rb')
            loader = ConfigYAMLLoader(stream)
            try:
                config_extension = loader.load()
            except yaml.YAMLError, exc:
                raise ScriptError("failed to load application configuration:"
                                  " %s" % exc)
            extensions = extensions + [config_extension]

        # Create the HTSQL application.
        from htsql import HTSQL
        try:
            app = HTSQL(db, *extensions)
        except ImportError, exc:
            raise ScriptError("failed to construct application: %s" % exc)
      
        with app:
            def describe_table(label, show_columns=False):
                self.ctl.out("table: %s" % label.name)
                children = classify(label.arc.target)
                if show_columns:
                    for label in children:
                        if not isinstance(label.arc, ColumnArc):
                            continue
                        self.ctl.out(" %s is %s" % (label.name, label.arc.target))
                for label in children:
                    if isinstance(label.arc, ChainArc):
                        if label.name == label.arc.target.table.name:
                            description = label.name
                        else:
                            description = "%s [%s]" % (label.name, label.arc.target)
                        if label.arc.is_expanding:
                            self.ctl.out(" -> %s" % description)
                for label in children:
                    if isinstance(label.arc, ChainArc):
                        if label.name == label.arc.target.table.name:
                            description = label.name
                        else:
                            description = "%s [%s]" % (label.name, label.arc.target)
                        if not label.arc.is_expanding:
                            self.ctl.out(" => %s" % description)
                   
            labels = classify(HomeNode())
            if self.table is None:
                for label in labels:
                    describe_table(label, show_columns=False)
            else:
                found = False
                for label in labels:
                    if self.table == label.name:
                        describe_table(label, show_columns=True)
                        found = True
                if not found:
                    self.ctl.out("Couldn't find table %s" % self.table)


