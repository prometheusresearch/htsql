#
# Copyright (c) 2006-2008, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
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
from ..core.model import HomeNode, ChainArc, ColumnArc, TableArc, AmbiguousArc
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
            root_labels = classify(HomeNode())
             
            #
            # Enumerate introspected tables if one isn't provided
            #
            if self.table is None:
                if not(root_labels):
                     self.ctl.out("No tables introspected or configured.")
                     return
                self.ctl.out("Tables introspected for this database are:")
                for label in root_labels:
                    if not isinstance(label.arc, TableArc):
                         continue
                    if label.name == label.arc.target.table.name:
                        self.ctl.out("\t%s" % label.name)
                    else:
                        self.ctl.out("\t%s (%s)" % (label.name,
                                                    label.arc.target.table.name))
                self.ctl.out()
                return


            #
            # Dump table attributes if a specific table is requested.
            #

            slots = None
            for label in root_labels:
                if label.name == self.table:
                    slots = classify(label.arc.target)
            if slots is None:
                self.ctl.out("Unable to find table %s." % self.table)
                self.ctl.out()
                return

            max_width = 0
            for slot in slots:
                if isinstance(slot.arc, AmbiguousArc):
                    continue
                if len(slot.name) > max_width:
                    max_width = len(slot.name)
            if not max_width:
                self.ctl.out("Table %s has no slots." % self.table)
                self.ctl.out()
                return

            self.ctl.out("Slots for %s are:" % self.table)
            for slot in slots:
                name = slot.name.ljust(max_width)
                post = str(slot.arc.target)
                if isinstance(slot.arc, ChainArc):
                    if slot.arc.is_expanding:
                        post = "SINGULAR(%s)" % post
                    else:
                        post = "PLURAL(%s)" % post
                if isinstance(slot.arc, AmbiguousArc):
                    continue
                self.ctl.out("\t %s %s" % (name, post))
            self.ctl.out()
