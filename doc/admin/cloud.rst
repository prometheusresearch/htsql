******************
HTSQL in the Cloud
******************

.. contents:: Table of Contents
   :depth: 1
   :local:

These instructions outline how to use HTSQL using a cloud
provider where you have a limited application environment.


.. index:: Heroku

Installing at Heroku
====================

Here we describe how to install HTSQL as a Heroku_ process 
using their cedar_ stack. 

Prerequisites
-------------

There are some basic steps that you must do if you're not
already using Heroku service.  Heroku's server management tools
are written in Ruby and use Git.
 
1. Install Ruby_, `Ruby Gems`_ and Git_ for your platform.  

2. Signup_ for a Heroku Account.

3. Install `Heroku CLI`_ command and register your 
   ssh key using ``heroku keys:add``.

4. When you first run a ``heroku`` command, it will prompt for your
   account information and cache it at ``~/.heroku/credentials``.

If you follow Heroku's quickstart_ don't follow the remaining
instructions past the Prerequisites since this is intended for
ruby application developers.

Application Template
--------------------

Next is an application template that matches with Heroku needs.
You'll need to create an empty "project" directory with the
following files & content.

``requirements.txt``::

  HTSQL
  PyYAML
  psycopg2

``Procfile``::

  web: python run_htsql.py

``config.yaml``::

  # shell plugin as a default command
  tweak.shell.default: 

``run_htsql.py``::

  # create HTSQL application
  import os, yaml
  from htsql import HTSQL
  config = yaml.load(open("config.yaml").read())
  dburi = os.environ['DATABASE_URL'].replace('postgres','pgsql')
  app = HTSQL(dburi, config)

  # start webserver
  from wsgiref.simple_server import make_server
  port = int(os.environ['PORT'])
  srv = make_server('0.0.0.0', port, app)
  srv.serve_forever()

These files can be found in the ``demo/heroku`` portion 
of the HTSQL distribution.

Deployment
----------  

Once those files are created, deployment to Heroku's system is
done by first creating a local git repository for your project::
  
  $ git init
  $ git add .
  $ git commit -m init

Then, initializing the heroku stack::

  $ heroku create --stack cedar

At this point, a PostgreSQL database is needed.  For testing,
you could use a shared database::

  $ heroku addons:add shared-database

Then, you're ready to push HTSQL to Heroku::

  $ git push heroku master

You could now go to the URL of the hosted service and enter 
``/'Hello World'`` for a very basic test.  If HTSQL server 
is not up and running, look at the logs::

  $ heroku logs

Likely you will want to customize your deployment, say running
it along side your web application and using ``nginx`` to
dispatch requests to some prefix to this process.

Limitations
-----------

One major limitation of Heroku deployment is that `Ruby on Rails`_
applications typically do not define foreign keys.  Furthermore, 
the tool Heroku uses to do import and export, taps_, discards any
foreign key constraints when migrating schemas.  Since HTSQL
relies upon foreign keys to determine linking, it's usage is 
limited to single table queries or manually specified links.

.. _Python: http://python.org
.. _Heroku: http://heroku.com
.. _cedar: http://devcenter.heroku.com/articles/cedar
.. _quickstart: http://devcenter.heroku.com/articles/quickstart
.. _Signup: http://heroku.com/signup
.. _Git: http://help.github.com/linux-set-up-git/ 
.. _Ruby: http://www.ruby-lang.org/en/downloads/
.. _Ruby Gems: http://www.ruby-lang.org/en/libraries/
.. _Heroku CLI: http://devcenter.heroku.com/articles/heroku-command 
.. _Ruby on Rails: http://rubyonrails.org/
.. _taps: http://devcenter.heroku.com/articles/taps
