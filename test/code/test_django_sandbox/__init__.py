
from .settings import DATABASES

engine = {
        'pgsql': 'postgresql_psycopg2',
        'sqlite': 'sqlite3',
}.get(sandbox.engine, sandbox.engine)
DATABASES['default']['ENGINE'] += engine
DATABASES['default']['NAME'] = sandbox.database
if sandbox.username is not None:
    DATABASES['default']['USER'] = sandbox.username
if sandbox.password is not None:
    DATABASES['default']['PASSWORD'] = sandbox.password
if sandbox.host is not None:
    DATABASES['default']['HOST'] = sandbox.host
if sandbox.port is not None:
    DATABASES['default']['PORT'] = str(sandbox.port)

def createdb():
    if sandbox.engine == 'mysql':
        DATABASES['default']['OPTIONS'] = {
                'init_command': 'SET storage_engine=INNODB',
        }
    from datetime import datetime
    from django.core.management import call_command
    from .polls.models import Poll, Choice
    call_command('syncdb', verbosity=0)
    p = Poll(question="What's up?", pub_date=datetime(2011, 0o1, 0o1))
    p.save()
    p.choice_set.create(choice='Not much', votes=10)
    p.choice_set.create(choice='The sky', votes=20)
    p.choice_set.create(choice='Just hacking again', votes=30)

def dropdb():
    from django.db import models, connections, transaction, DEFAULT_DB_ALIAS
    from django.core.management import sql, color
    app = models.get_app('polls')
    connection = connections[DEFAULT_DB_ALIAS]
    sql_list = sql.sql_delete(app, color.no_style(), connection)
    cursor = connection.cursor()
    for sql in sql_list:
        cursor.execute(sql)
    transaction.commit_unless_managed(using=DEFAULT_DB_ALIAS)

