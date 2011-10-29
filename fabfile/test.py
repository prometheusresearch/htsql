#
# Copyright (c) 2006-2011, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from .util import (execute, htsql_ctl, exec_htsql_ctl, pyflakes, coverage_py,
                   filecolor, load_fabfile_env)
from fabric.api import abort
import os, shutil


# Fabric commands defined here.
__all__ = ['test', 'train', 'purge_test', 'lint', 'coverage',
           'createdb', 'dropdb', 'shell', 'serve',
           'client_demo', 'client_admin']


valid_engines = ['sqlite', 'pgsql', 'mysql', 'oracle', 'mssql']
def validate_engine(engine):
    # Complain if the engine is not from the above list.
    if engine not in valid_engines:
        abort("unknown engine %r; must be one of %s"
              % (engine, ", ".join(repr(valid_engine)
                                   for valid_engine in valid_engines)))


def make_demo_db(engine):
    # Generate the DB URI for the demo database.
    load_fabfile.env()
    if engine == 'sqlite':
        return "sqlite:build/regress/sqlite/htsql_demo.sqlite"
    elif engine in ['pgsql', 'mysql', 'mssql']:
        host = os.environ.get("%s_HOST" % engine.upper(), "")
        if host:
            port = os.environ.get("%s_PORT" % engine.upper(), "")
            if port:
                host = "%s:%s" % (host, port)
        return "%s://htsql_demo:secret@%s/htsql_demo" % (engine, host)
    elif engine == 'oracle':
        host = os.environ.get("ORACLE_HOST", "")
        if host:
            port = os.environ.get("ORACLE_PORT", "")
            if port:
                host = "%s:%s" % (host, port)
        sid = os.environ.get("ORACLE_SID", "XE")
        return "oracle://htsql_demo:secret@%s/%s" % (host, sid)


def make_client_command(engine, username, password, host, port, database):
    # Generate a command calling a native database client.
    if engine == 'sqlite':
        command = "sqlite3"
        if database:
            command += " %s" % database
    elif engine == 'pgsql':
        command = "psql"
        if host:
            command += " -h %s" % host
        if port:
            command += " -p %s" % port
        if username:
            command += " -U %s" % username
        if database:
            command += " %s" % database
    elif engine == 'mysql':
        command = "mysql"
        if host:
            command += " -h %s" % host
        if port:
            command += " -P %s" % port
        if username:
            command += " -u %s" % username
        if password:
            command += " -p%s" % password
        if database:
            command += " %s" % database
    elif engine == 'oracle':
        command = "sqlplus -L "
        if username:
            command += "%s" % username
            if password:
                command += "/%s" % password
        command += "@"
        command += "%s" % (host or "localhost")
        if port:
            command += ":%s" % port
    elif engine == 'mssql':
        command = "tsql"
        if host:
            command += " -H %s" % host
        if port:
            command += " -p %s" % port
        if username:
            command += " -U %s" % username
        if password:
            command += " -P %s" % password
        if database:
            command += " -D %s" % database
    return command


def regress(command):
    # Run regression tests.
    return htsql_ctl("regress -i test/regress.yaml"+" "+command)


def exec_regress(command):
    # Execute regression tests replacing the current process.
    return exec_htsql_ctl("regress -i test/regress.yaml"+" "+command)


def test(*suites):
    """run regression tests"""
    command = "-q"
    if suites:
        command += " "+" ".join(suites)
    exec_regress(command)


def train(*suites):
    """run regression tests in the train mode"""
    command = "--train"
    if suites:
        command += " "+" ".join(suites)
    exec_regress(command)


def purge_test():
    """purge stale output records from regression tests"""
    exec_regress("-q --train --purge")


def lint():
    """detect errors in the source code (requires PyFlakes)"""
    pyflakes("src/htsql src/htsql_ctl src/htsql_engine src/htsql_tweak")


def coverage():
    """measure code coverage by regression tests (requires coverage.py)"""
    load_fabfile.env()
    shutil.rmtree("build/coverage", True)
    os.makedirs("build/coverage")
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    coverage_py("run --branch"
                " --source=htsql,htsql_ctl,htsql_engine,htsql_tweak"
                " `which \"%s\"` regress -i test/regress.yaml -q"
                % HTSQL_CTL,
                "build/coverage/coverage.dat")
    coverage_py("html --directory=build/coverage",
                "build/coverage/coverage.dat")
    print
    print "To see the coverage report, open"
    print "    %s" % filecolor("./build/coverage/index.html")


def createdb(engine):
    """deploy the demo database"""
    validate_engine(engine)
    regress("-q drop-%s create-%s" % (engine, engine))
    db = make_demo_db(engine)
    print
    print "The demo database has been deployed at"
    print "    %s" % filecolor(db)


def dropdb(engine):
    """delete the demo database"""
    validate_engine(engine)
    regress("-q drop-%s" % engine)
    print
    print "The demo database has been deleted."


def shell(engine):
    """start an HTSQL shell on the demo database"""
    validate_engine(engine)
    db = make_demo_db(engine)
    exec_htsql_ctl("shell %s" % db)


def serve(engine):
    """start an HTSQL server on the demo database"""
    validate_engine(engine)
    db = make_demo_db(engine)
    host = os.environ.get("HTSQL_HOST", "localhost")
    port = os.environ.get("HTSQL_PORT", "8080")
    exec_htsql_ctl("serve %s %s %s" % (db, host, port))


def client_demo(engine):
    """run a native database client against the demo database"""
    load_fabfile_env()
    validate_engine(engine)
    if engine == 'sqlite':
        command = make_client_command(engine, None, None, None, None,
                                      "build/regress/sqlite/htsql_demo.sqlite")
    else:
        host = os.environ.get("%s_HOST" % engine.upper())
        port = os.environ.get("%s_PORT" % engine.upper())
        command = make_client_command(engine, "htsql_demo", "secret",
                                      host, port, "htsql_demo")
    execute(command)


def client_admin(engine):
    """run a native database client with administrator rights"""
    load_fabfile_env()
    validate_engine(engine)
    if engine == 'sqlite':
        command = make_client_command(engine, None, None, None, None, None)
    else:
        username = os.environ.get("%s_USERNAME" % engine.upper())
        password = os.environ.get("%s_PASSWORD" % engine.upper())
        host = os.environ.get("%s_HOST" % engine.upper())
        port = os.environ.get("%s_PORT" % engine.upper())
        command = make_client_command(engine, username, password,
                                      host, port, None)
    execute(command)


