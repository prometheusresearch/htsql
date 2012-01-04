#
# Copyright (c) 2006-2012, Prometheus Research, LLC
# See `LICENSE` for license information, `AUTHORS` for the list of authors.
#


from job import job, run, exe, log, fatal, mktree, rmtree
import os


def htsql_ctl(command):
    # Run `htsql-ctl <command>`.
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    run(HTSQL_CTL+" "+command, verbose=True)


def exe_htsql_ctl(command):
    # Execute `htsql-ctl <command>`.
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    exe(HTSQL_CTL+" "+command)


def regress(command):
    # Run `htsql-ctl regress -i test/regress.yaml <command>`.
    htsql_ctl("regress -i test/regress.yaml "+command)


def exe_regress(command):
    # Execute `htsql-ctl regress -i test/regress.yaml <command>`.
    exe_htsql_ctl("regress -i test/regress.yaml "+command)


def pyflakes(command):
    # Run `pyflakes <command>`.
    PYFLAKES = os.environ.get("PYFLAKES", "pyflakes")
    return run(PYFLAKES+" "+command, verbose=True)


def coverage_py(command, coverage_file=None):
    # Run `COVERAGE_FILE=<coverage_file> coverage <command>`.
    COVERAGE = os.environ.get("COVERAGE", "coverage")
    if coverage_file is not None:
        COVERAGE = "COVERAGE_FILE=\"%s\" %s" % (coverage_file, COVERAGE)
    return run(COVERAGE+" "+command, verbose=True)


def validate_engine(engine):
    # Check if `engine` parameter is valid.
    valid_engines = ['sqlite', 'pgsql', 'mysql', 'oracle', 'mssql']
    if engine not in valid_engines:
        raise fatal("invalid engine: expected one of %s; got %s"
                    % (", ".join(valid_engines), engine))


def validate_database(database):
    # Check if `database` parameter is valid.
    valid_databases = ['demo', 'edge', 'sandbox']
    if database not in valid_databases:
        raise fatal("invalid database name: expected one of %s; got %s"
                    % (", ".join(valid_databases), database))


def make_db(engine, name):
    # Generate DB URI for the given engine and database.
    validate_engine(engine)
    validate_database(name)
    if engine == 'sqlite':
        return "sqlite:build/regress/sqlite/htsql_%s.sqlite" % name
    elif engine in ['pgsql', 'mysql', 'mssql']:
        host = os.environ.get("%s_HOST" % engine.upper(), "")
        if host:
            port = os.environ.get("%s_PORT" % engine.upper(), "")
            if port:
                host = "%s:%s" % (host, port)
        return ("%s://htsql_%s:secret@%s/htsql_%s"
                % (engine, name, host, name))
    elif engine == 'oracle':
        host = os.environ.get("ORACLE_HOST", "")
        if host:
            port = os.environ.get("ORACLE_PORT", "")
            if port:
                host = "%s:%s" % (host, port)
        sid = os.environ.get("ORACLE_SID", "XE")
        return "oracle://htsql_%s:secret@%s/%s" % (name, host, sid)


def make_client(engine, name=None):
    # Generate a command calling a native database client.
    validate_engine(engine)
    if name is not None:
        validate_database(name)
    host = None
    port = None
    username = None
    password = None
    database = None
    if engine == 'sqlite':
        if name is not None:
            database = "build/regress/sqlite/htsql_%s.sqlite" % name
    else:
        host = os.environ.get("%s_HOST" % engine.upper())
        port = os.environ.get("%s_PORT" % engine.upper())
        if name is not None:
            username = "htsql_%s" % name
            password = "secret"
            database = "htsql_%s" % name
        else:
            username = os.environ.get("%s_USERNAME" % engine.upper())
            password = os.environ.get("%s_PASSWORD" % engine.upper())
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


@job
def test(*suites):
    """run regression tests

    Run `job test` to run all regression tests.
    Run `job test <suite>` to run a specific test suite.

    Test suites (non-exhaustive list):
      routine                  : test htsql-ctl routines
      sqlite                   : test SQLite backend
      pgsql                    : test PostgreSQL backend
      mysql                    : test MySQL backend
      oracle                   : test Oracle backend
      mssql                    : test MS SQL Server backend
    """
    command = "-q"
    if suites:
        command += " "+" ".join(suites)
    exe_regress(command)


@job
def train(*suites):
    """run regression tests in the train mode

    Run tests in the train mode to confirm and save
    new or updated test output.

    Run `job train` to run all regression tests.
    Run `job train <suite>` to run a specific test suite.

    To see a non-exhaustive list of test suites, run:
      `job help test`
    """
    command = "--train"
    if suites:
        command += " "+" ".join(suites)
    exe_regress(command)


@job
def purge_test():
    """purge stale output records from regression tests

    Run this job to remove stale output data for deleted
    or modified tests.
    """
    exe_regress("-q --train --purge")


@job
def lint():
    """detect errors in the source code (PyFlakes)"""
    pyflakes("src/htsql src/htsql_ctl src/htsql_engine src/htsql_tweak")


@job
def coverage():
    """measure code coverage by regression tests (coverage.py)"""
    if os.path.exists("./build/coverage"):
        rmtree("./build/coverage")
    mktree("./build/coverage")
    HTSQL_CTL = os.environ.get("HTSQL_CTL", "htsql-ctl")
    coverage_py("run --branch"
                " --source=htsql,htsql_ctl,htsql_engine,htsql_tweak"
                " `which \"%s\"` regress -i test/regress.yaml -q"
                % HTSQL_CTL,
                "./build/coverage/coverage.dat")
    coverage_py("html --directory=build/coverage",
                "./build/coverage/coverage.dat")
    log()
    log("To see the coverage report, open:")
    log("  `./build/coverage/index.html`")
    log()


@job
def createdb(engine):
    """deploy regression databases

    Run `job createdb <engine>` to create regression databases
    for the specified backend.

    To set database connection parameters, copy file `job.env.sample`
    to `job.env` and update respective environment variables.

    Supported backends:
      sqlite                   : SQLite backend
      pgsql                    : PostgreSQL backend
      mysql                    : MySQL backend
      oracle                   : Oracle backend
      mssql                    : MS SQL Server backend

    Regression databases:
      demo                     : student enrollment database
      edge                     : edge cases collection
      sandbox                  : empty database
    """
    db = make_db(engine, 'demo')
    regress("-q drop-%s create-%s" % (engine, engine))
    log()
    log("The demo regression database has been deployed at:")
    log("  `%s`" % db)
    log()


@job
def dropdb(engine):
    """delete regression databases

    Run `job dropdb <engine>` to delete users and databases
    deployed by regression tests.

    To get a list of supported engines, run:
      `job help createdb`
    """
    db = make_db(engine, 'demo')
    regress("-q drop-%s" % engine)
    log()
    log("Regression databases has beeen deleted.")
    log()


@job
def shell(engine, name='demo'):
    """start an HTSQL shell on a regression database

    Run `job shell <engine> [<name>]` to start an HTSQL shell
    on a regression database.

    If the database name is not specified, the shell is started
    on the `demo` database.

    To get a list of supported engines and databases, run:
      `job help createdb`
    """
    db = make_db(engine, name)
    exe_htsql_ctl("shell %s" % db)


@job
def serve(engine, name='demo'):
    """start an HTSQL server on a regression database

    Run `job serve <engine> [<name>]` to start an HTTP server
    running HTSQL on a regression database.

    If the database name is not specified, the server is started
    on the `demo` database.

    To get a list of supported engines and databases, run:
      `job help createdb`
    """
    db = make_db(engine, name)
    host = os.environ.get("HTSQL_HOST", "localhost")
    port = os.environ.get("HTSQL_PORT", "8080")
    exe_htsql_ctl("serve %s %s %s" % (db, host, port))


@job
def client(engine, name=None):
    """start a native database client

    Run `job client <engine> [<name>]` to start a native database
    client on a regression database.

    If the database name is not specified, the client is started
    with administrative rights against the default database.

    To get a list of supported engines and databases, run:
      `job help createdb`
    """
    command = make_client(engine, name)
    exe(command)


