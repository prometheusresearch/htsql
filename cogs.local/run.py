#
# Copyright (c) 2006-2013, Prometheus Research, LLC
#


from cogs import task, setting, env
from cogs.fs import exe, sh, mktree, rmtree
from cogs.log import log, fail
import os


def htsql_ctl(command, environ=None):
    # Run `htsql-ctl <command>`.
    with env(debug=True):
        sh(env.ctl_path+" "+command)


def exe_htsql_ctl(command, environ=None):
    # Execute `htsql-ctl <command>`.
    with env(debug=True):
        exe(env.ctl_path+" "+command, environ=environ)


def regress(command):
    # Run `pbbt test/regress.yaml <command>`.
    variables = make_variables()
    with env(debug=True):
        sh(env.pbbt_path+" test/regress.yaml -E test/regress.py "
           +variables+command)


def exe_regress(command):
    # Run `pbbt test/regress.yaml <command>`.
    variables = make_variables()
    with env(debug=True):
        exe(env.pbbt_path+" test/regress.yaml -E test/regress.py "
            +variables+command)


def pyflakes(command):
    # Run `pyflakes <command>`.
    with env(debug=True):
        sh(env.pyflakes_path+" "+command)


def coverage_py(command, environ=None):
    # Run `coverage <command>`.
    path = env.coverage_path
    with env(debug=True):
        sh(path+" "+command, environ=environ)


def validate_engine(engine):
    # Check if `engine` parameter is valid.
    valid_engines = ['sqlite', 'pgsql', 'mysql', 'oracle', 'mssql']
    if engine not in valid_engines:
        raise fail("invalid engine: expected one of {}; got {}",
                   ", ".join(valid_engines), engine)


def validate_database(database):
    # Check if `database` parameter is valid.
    valid_databases = ['demo', 'edge', 'etl', 'sandbox']
    if database not in valid_databases:
        raise fail("invalid database name: expected one of {}; got {}",
                   ", ".join(valid_databases), database)


def make_db(engine, name):
    # Generate DB URI for the given engine and database.
    validate_engine(engine)
    validate_database(name)
    if engine == 'sqlite':
        return "sqlite:build/regress/sqlite/htsql_%s.sqlite" % name
    elif engine in ['pgsql', 'mysql', 'mssql']:
        host = getattr(env, '%s_host' % engine) or ''
        if host:
            port = getattr(env, '%s_port' % engine)
            if port:
                host = "%s:%s" % (host, port)
        return ("%s://htsql_%s:secret@%s/htsql_%s"
                % (engine, name, host, name))
    elif engine == 'oracle':
        host = env.oracle_host or ''
        if host and env.oracle_port:
            host = "%s:%s" % (host, env.oracle_port)
        sid = env.oracle_sid
        return "oracle://htsql_%s:secret@%s/%s" % (name, host, sid)


def make_variables():
    # Generate conditional variables for pbbt.
    variables = []
    for engine in ['pgsql', 'mysql', 'oracle', 'mssql']:
        for name in ['username', 'password', 'host', 'port']:
            value = getattr(env, '%s_%s' % (engine, name))
            if value:
                variables.append('-D %s_%s=%s '
                                 % (engine.upper(), name.upper(), str(value)))
        if engine == 'oracle' and env.oracle_sid:
            variables.append('-D ORACLE_SID=%s ' % env.oracle_sid)
    if env.ctl_path != 'htsql-ctl':
        variables.append('-D HTSQL_CTL=%s ' % env.ctl_path)
    return "".join(variables)


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
        host = getattr(env, '%s_host' % engine)
        port = getattr(env, '%s_port' % engine)
        if name is not None:
            username = "htsql_%s" % name
            password = "secret"
            database = "htsql_%s" % name
        else:
            username = getattr(env, '%s_username' % engine)
            password = getattr(env, '%s_password' % engine)
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


@setting
def PYFLAKES(path=None):
    """path to pyflakes executable"""
    if not path:
        path = 'pyflakes'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(pyflakes_path=path)


@setting
def COVERAGE(path=None):
    """path to coverage executable"""
    if not path:
        path = 'coverage'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(coverage_path=path)


@setting
def HTSQL_CTL(path=None):
    """path to htsql-ctl executable"""
    if not path:
        path = 'htsql-ctl'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(ctl_path=path)


@setting
def PBBT(path=None):
    """path to pbbt executable"""
    if not path:
        path = 'pbbt'
    if not isinstance(path, str):
        raise ValueError("expected a string value")
    env.add(pbbt_path=path)


@setting
def HTSQL_HOST(host=None):
    """host of the demo HTSQL server"""
    env.add(htsql_host=host or 'localhost')
    if not isinstance(env.htsql_host, str):
        raise ValueError("expected a string value")


@setting
def HTSQL_PORT(port=None):
    """port of the demo HTSQL server"""
    env.add(htsql_port=port or 8080)
    if not isinstance(env.htsql_port, int):
        raise ValueError("expected a string value")


@setting
def PGSQL_USERNAME(name=None):
    """user name for PostgreSQL regression database"""
    if not (name is None or isinstance(name, str)):
        raise ValueError("expected a string value")
    env.add(pgsql_username=name)


@setting
def PGSQL_PASSWORD(passwd=None):
    """password for PostgreSQL regression database"""
    if not (passwd is None or isinstance(passwd, str)):
        raise ValueError("expected a string value")
    env.add(pgsql_password=passwd)


@setting
def PGSQL_HOST(host=None):
    """host for PostgreSQL regression database"""
    if not (host is None or isinstance(host, str)):
        raise ValueError("expected a string value")
    env.add(pgsql_host=host)


@setting
def PGSQL_PORT(port=None):
    """port for PostgreSQL regression database"""
    if not (port is None or isinstance(port, int)):
        raise ValueError("expected an integer value")
    env.add(pgsql_port=port)


@setting
def MYSQL_USERNAME(name=None):
    """user name for MySQL regression database"""
    if not (name is None or isinstance(name, str)):
        raise ValueError("expected a string value")
    env.add(mysql_username=name)


@setting
def MYSQL_PASSWORD(passwd=None):
    """password for MySQL regression database"""
    if not (passwd is None or isinstance(passwd, str)):
        raise ValueError("expected a string value")
    env.add(mysql_password=passwd)


@setting
def MYSQL_HOST(host=None):
    """host for MySQL regression database"""
    if not (host is None or isinstance(host, str)):
        raise ValueError("expected a string value")
    env.add(mysql_host=host)


@setting
def MYSQL_PORT(port=None):
    """port for MySQL regression database"""
    if not (port is None or isinstance(port, int)):
        raise ValueError("expected an integer value")
    env.add(mysql_port=port)


@setting
def ORACLE_USERNAME(name=None):
    """user name for Oracle regression database"""
    if not (name is None or isinstance(name, str)):
        raise ValueError("expected a string value")
    env.add(oracle_username=name)


@setting
def ORACLE_PASSWORD(passwd=None):
    """password for Oracle regression database"""
    if not (passwd is None or isinstance(passwd, str)):
        raise ValueError("expected a string value")
    env.add(oracle_password=passwd)


@setting
def ORACLE_HOST(host=None):
    """host for Oracle regression database"""
    if not (host is None or isinstance(host, str)):
        raise ValueError("expected a string value")
    env.add(oracle_host=host)


@setting
def ORACLE_PORT(port=None):
    """port for Oracle regression database"""
    if not (port is None or isinstance(port, int)):
        raise ValueError("expected an integer value")
    env.add(oracle_port=port)


@setting
def ORACLE_SID(sid=None):
    """SID for Oracle regression database"""
    if not (sid is None or isinstance(sid, str)):
        raise ValueError("expected a string value")
    env.add(oracle_sid=sid or 'XE')


@setting
def MSSQL_USERNAME(name=None):
    """user name for MS SQL Server regression database"""
    if not (name is None or isinstance(name, str)):
        raise ValueError("expected a string value")
    env.add(mssql_username=name)


@setting
def MSSQL_PASSWORD(passwd=None):
    """password for MS SQL Server regression database"""
    if not (passwd is None or isinstance(passwd, str)):
        raise ValueError("expected a string value")
    env.add(mssql_password=passwd)


@setting
def MSSQL_HOST(host=None):
    """host for MS SQL Server regression database"""
    if not (host is None or isinstance(host, str)):
        raise ValueError("expected a string value")
    env.add(mssql_host=host)


@setting
def MSSQL_PORT(port=None):
    """port for MS SQL Server regression database"""
    if not (port is None or isinstance(port, int)):
        raise ValueError("expected an integer value")
    env.add(mssql_port=port)


@task
def TEST(*suites):
    """run regression tests

    Run `cogs test` to run all regression tests.
    Run `cogs test <suite>` to run a specific test suite.

    Test suites (non-exhaustive list):
      routine                  : test htsql-ctl routines
      sqlite                   : test SQLite backend
      pgsql                    : test PostgreSQL backend
      mysql                    : test MySQL backend
      oracle                   : test Oracle backend
      mssql                    : test MS SQL Server backend
    """
    command = "-q"
    for suite in suites:
        if not suite.startswith('/'):
            suite = '/all/'+suite
        command += " -S "+suite
    exe_regress(command)


@task
def TRAIN(*suites):
    """run regression tests in the train mode

    Run tests in the train mode to confirm and save
    new or updated test output.

    Run `cogs train` to run all regression tests.
    Run `cogs train <suite>` to run a specific test suite.

    To see a non-exhaustive list of test suites, run:
      `cogs help test`
    """
    command = "--train"
    for suite in suites:
        if not suite.startswith('/'):
            suite = '/all/'+suite
        command += " -S "+suite
    exe_regress(command)


@task
def PURGE_TEST():
    """purge stale output records from regression tests

    Run this task to remove stale output data for deleted
    or modified tests.
    """
    exe_regress("-q --train --purge")


@task
def LINT():
    """detect errors in the source code (PyFlakes)"""
    pyflakes("src/htsql src/htsql_sqlite src/htsql_pgsql src/htsql_oracle"
             " src/htsql_mssql src/htsql_django")


@task
def COVERAGE():
    """measure code coverage by regression tests (coverage.py)"""
    if os.path.exists("./build/coverage"):
        rmtree("./build/coverage")
    mktree("./build/coverage")
    environ=make_environ()
    environ['COVERAGE_FILE'] = "./build/coverage/coverage.dat"
    coverage_py("run --branch"
                " --source=htsql,htsql_sqlite,htsql_pgsql,htsql_oracle,"
                "htsql_mssql,htsql_django"
                " `which \"%s\"` regress -i test/regress.yaml -q"
                % env.pbbt_path,
                environ=environ)
    coverage_py("html --directory=build/coverage",
                "./build/coverage/coverage.dat")
    log()
    log("To see the coverage report, open:")
    log("  `./build/coverage/index.html`")
    log()


@task
def CREATEDB(engine):
    """deploy regression databases

    Run `cogs createdb <engine>` to create regression databases
    for the specified backend.

    To set database connection parameters, copy file `cogs.conf.sample`
    to `cogs.conf` and update respective settings.

    Supported backends:
      sqlite                   : SQLite backend
      pgsql                    : PostgreSQL backend
      mysql                    : MySQL backend
      oracle                   : Oracle backend
      mssql                    : MS SQL Server backend

    Regression databases:
      demo                     : student enrollment database
      edge                     : edge cases collection
      etl                      : CRUD/ETL database
      sandbox                  : empty database
    """
    db = make_db(engine, 'demo')
    regress("-q -S /all/%s/dropdb -S /all/%s/createdb" % (engine, engine))
    log()
    log("The demo regression database has been deployed at:")
    log("  `{}`", db)
    log()


@task
def DROPDB(engine):
    """delete regression databases

    Run `cogs dropdb <engine>` to delete users and databases
    deployed by regression tests.

    To get a list of supported engines, run:
      `cogs help createdb`
    """
    db = make_db(engine, 'demo')
    regress("-q -S /all/%s/dropdb" % engine)
    log()
    log("Regression databases has beeen deleted.")
    log()


@task
def SHELL(engine, name='demo'):
    """start an HTSQL shell on a regression database

    Run `cogs shell <engine> [<name>]` to start an HTSQL shell
    on a regression database.

    If the database name is not specified, the shell is started
    on the `demo` database.

    To get a list of supported engines and databases, run:
      `cogs help createdb`
    """
    db = make_db(engine, name)
    exe_htsql_ctl("shell %s -E htsql:debug=true" % db)


@task
def SERVE(engine, name='demo'):
    """start an HTSQL server on a regression database

    Run `cogs serve <engine> [<name>]` to start an HTTP server
    running HTSQL on a regression database.

    If the database name is not specified, the server is started
    on the `demo` database.

    To get a list of supported engines and databases, run:
      `cogs help createdb`
    """
    db = make_db(engine, name)
    exe_htsql_ctl("serve %s --host %s --port %s"
                  % (db, env.htsql_host, env.htsql_port))


@task
def CLIENT(engine, name=None):
    """start a native database client

    Run `cogs client <engine> [<name>]` to start a native database
    client on a regression database.

    If the database name is not specified, the client is started
    with administrative rights against the default database.

    To get a list of supported engines and databases, run:
      `cogs help createdb`
    """
    command = make_client(engine, name)
    with env(debug=True):
        exe(command)


