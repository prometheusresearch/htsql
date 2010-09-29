=====================
Introduction to HTSQL
=====================

*A query language for the accidental programmer*

HTSQL makes accessing data as easy as browsing the web.  An HTSQL
processor translates web requests into relational database queries and
returns the results in a form ready for display or processing.
Information in a database can then be directly accessed from a browser,
a mobile application, statistics tool, or a rich Internet application.
Like any web resource, an HTSQL service can be secured via encryption
and standard authentication mechanisms -- either on your own private
intranet or publicly on the Internet.

HTSQL users are data experts.  They may be business users, but they can
also be technical users who value data transparency and direct access.
Business users can use HTSQL to quickly search the database and create
reports without the help of IT.  Programmers can use it as data access
layer for web applications.  HTSQL can be installed by DBAs to provide
easy, safe database access for power users.

HTSQL is a schema-driven URI-to-SQL translator that takes a request over
HTTP, converts it to a set of SQL queries, executes these queries in a
single transaction, and returns the results in a format (CSV, HTML,
JSON, etc.) requested by the user agent:: 

  /----------------\                   /------------------------\
  | USER AGENT     |                   |   HTSQL WEB SERVICE    |
  *----------------*  HTTP Request     *------------------------* 
  |                | >---------------> -.                       |
  | * Web Browsers |  URI, headers,    | \      .---> Generated |
  |   HTML, TEXT   |  post/put body    |  v    /      SQL Query |
  |                |                   |  HTSQL          |      |
  | * Applications |                   |  PROCESSOR      v      |
  |   JSON, XML    |  HTTP Response    | /   ^.       SECURED   |
  |                | <---------------< -.      \      DATABASE  |
  | * Spreadsheets |  status, header,  |     Query       .      |
  |   CSV, XML     |  csv/html/json    |     Results <---/      |
  |                |  result body      |                        |
  \----------------/                   \------------------------/  

The HTSQL query processor does heavy lifting for you.  Using
relationships between tables as permitted links, the HTSQL processor
translates graph-oriented web requests into corresponding relational
queries.  This translation can be especially involved for sophisticated
requests having projections and aggregates.  For complex cases, an
equivalent hand-written SQL query is tedious to write and non-obvious
without extensive training.  By doing graph to relational mapping on
your behalf, HTSQL permits your time to be spent exploring information
instead of debugging.

The HTSQL language is easy to use.  We've designed HTSQL to be broadly
usable by semi-technical domain experts, or what we call *accidental
programmers*.  We've field tested the toolset with business analysts,
medical researchers, statisticians, and web application developers. By
using a formalized directed graph as the underpinning of the query
algebra and by using a URI-inspired syntax over HTTP, we've obtained a
careful balance between clarity and functionality.

We hope you like it.


Getting Started
===============

The following examples show output from the HTSQL command-line system,
which is plain text.  HTSQL can output HTML, CSV, XML and many other
formats.  This makes it suitable not only for direct queries, but as a
data access layer for application development.

We'll use a fictional university that maintains a database for its
student enrollment system.  There are four tables that describe the
business units of the university and their relationship to the 
courses offered::

  +--------------------+              +---------------------+     
  | DEPARTMENT         |              | SCHOOL              |     
  +--------------------+              +---------------------+     
  | code            PK |--\       /---| code             PK |----\
  | school          FK |>-|------/    | name          NN,UK |    |
  | name         NN,UK |  |    .      +---------------------+    |
  +--------------------+  |     .                              . |
                        . |  departments                      .  |
       a department    .  |  may belong                      .   |
       offers zero or .   |  to at most        a school          |
       more courses       |  one school        administers zero  |
                          |                    or more programs  |
  +--------------------+  |                                      |
  | COURSE             |  |           +---------------------+    |
  +--------------------+  |           | PROGRAM             |    |
  | department  FK,PK1 |>-/           +---------------------+    |
  | number         PK2 |              | school       PK1,FK |>---/
  | title           NN |              | code            PK2 |        
  | credits         NN |              | title            NN |        
  | description        |              | degree           CK |        
  +--------------------+              +---------------------+        

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint   

The university consists of schools, which administer one or more
degree-granting programs.  Departments are associated with a school
and offer courses.  Further on in the tutorial we will introduce
other tables such as student, instructor and enrollment.

Selecting Data
--------------

HTSQL requests typically begin with a table name.  You can browse the
contents of a table, search for specific data, and select the columns
you want to see in the results.  

The most basic HTSQL request (A1_) returns everything from a table::

   /school 

.. _A1:  http://demo.htsql.org/school

The result set is a list of schools in the university, including all
columns, sorted by the primary key for the table::

    school                                            
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    code | name                     
    -----+--------------------------
    art  | School of Art and Design                   
    bus  | School of Business                
    edu  | College of Education                       
    egn  | School of Engineering
    ...                      

Not all columns are useful for every context.  Use a *selector* to
choose columns for display (A2_)::

    /program{school, code, title}

    program
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~ 
    school | code     | title
    -------+----------+-----------------------------------
    art    | gart     | Post Baccalaureate in Art History
    art    | uhist    | Bachelor of Arts in Art History  
    art    | ustudio  | Bachelor of Arts in Studio Art   
    bus    | pacc     | Graduate Certificate in Accounting
    ...

.. _A2: http://demo.htsql.org/program{school,code,title}

Add a plus (``+``) sign to the column name to sort the column in
ascending order.  Use a minus sign (``-``) for descending order.  For
example, this request (A3_) returns departments in descending order::

    /department{name-, school}

    department                                        
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    name                   | school
    -----------------------+-------
    Wind                   | mus   
    Vocals                 | mus   
    Teacher Education      | edu   
    Studio Art             | art
    ...   

.. _A3: 
    http://demo.htsql.org/department{name-,school}

Using two ordering indicators will sort on labeled columns as they
appear in the selector.  In the example below, we sort in ascending
order on ``department`` and then descending on ``credits`` (A4_)::

    /course{department+, number, credits-, title}

    course                                            
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    department | number | credits | title                     
    -----------+--------+---------+---------------------------
    acc        | 315    | 5       | Financial Accounting      
    acc        | 200    | 3       | Principles of Accounting I
    acc        | 426    | 3       | Corporate Taxation        
    ...

.. _A4: 
    http://demo.htsql.org
    /course{department+, number, credits-, title}
 
To display friendlier names for the columns, use ``as`` to rename a
column's title (A5_)::

    /course{department as 'Dept Code'+, number as 'No.',
            credits-, title}

    course                                            
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    Dept Code | No. | credits | title                      
    ----------+-----+---------+----------------------------
    acc       | 315 | 5       | Financial Accounting       
    acc       | 200 | 3       | Principles of Accounting I 
    acc       | 426 | 3       | Corporate Taxation         
    ...

.. _A5: 
    http://demo.htsql.org
    /course{department%20as%20'Dept%20Code'+,number%20as%20'No.',
            credits-, title}

Selectors let you choose, rearrange, and sort columns of interest.  They
are an easy way to exclude data that isn't meaningful to your report.   

Linking Data
------------

In our example schema, each ``program`` is administered by a ``school``.
Since the HTSQL processor knows about this relationship, it is possible
to link data accordingly (B1_)::

    /program{school.name, title}

    program                                           
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    school.name               | title                             
    --------------------------+-----------------------------------
    School of Art and Design  | Post Baccalaureate in Art History 
    School of Art and Design  | Bachelor of Arts in Art History   
    School of Art and Design  | Bachelor of Arts in Studio Art    
    School of Business        | Graduate Certificate in Accounting
    ...

.. _B1: 
    http://demo.htsql.org
    /program{school.name, title}

This request joins the ``program`` and ``school`` tables by the foreign
key from ``program{school}`` to ``school{code}``.  This is called a
*singular* relationship, since for every ``program``, there is exactly
one ``school``.  

It is possible to join through multiple foreign keys; since ``course``
is offered by a ``department`` which belongs to a ``school``, we can
list courses including school and department name (B2_)::

    /course{department.school.name, department.name, title}

    course                                           
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    department.school.name | department.name | title                    
    -----------------------+-----------------+---------------------------
    School of Business     | Accounting      | Practical Bookkeeping      
    School of Business     | Accounting      | Principles of Accounting I 
    School of Business     | Accounting      | Financial Accounting       
    School of Business     | Accounting      | Corporate Taxation         
    ...

.. _B2: 
    http://demo.htsql.org
    /course{department.school.name, department.name, title}

This request can be shortened a bit by collapsing the duplicate mention
of ``department``; the resulting request is equivalent (B3_)::

    /course{department{school.name, name}, title}

.. _B3: 
    http://demo.htsql.org
    /course{department{school.name, name}, title}

For cases where you don't wish to specify each column explicitly, use
the wildcard ``*`` selector.  The request below returns all columns from
program, and all columns from school (B4_)::

    /department{*,school.*}

    department                                       
    ~~~~~~~~~~~~~~~~~~~~ ... ~~~~~~~~~~~~~~~~~~~~~~~~~~~~ ...
    code   | name        ... | school.name                ...  
    -------+------------ ... +--------------------------- ...  
    acc    | Accounting  ... | School of Business         ...  
    arthis | Art History ... | School of Art and Design   ...  
    artstd | Studio Art  ... | School of Art and Design   ...  
    astro  | Astronomy   ... | School of Natural Sciences ...  
    ...

.. _B4: 
    http://demo.htsql.org
    /department{*,school.*}
    
Since the HTSQL processor knows about relationships between tables in
your relational database, joining tables in your reports is trivial.

Filtering Data
--------------

Predicate expressions in HTSQL follow the question mark ``?``.  
For example, to return departments in the 'School of Engineering'
we write (C1_)::
  
    /department?school='egn'

    department                            
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    code | name                   | school
    -----+------------------------+-------
    be   | Bioengineering         | egn   
    comp | Computer Science       | egn   
    ee   | Electrical Engineering | egn   
    me   | Mechanical Engineering | egn
    ...

.. _C1: 
    http://demo.htsql.org
    /department?school='egn'

The request above returns all rows in the ``department`` table where the
column ``school`` is equal to ``'eng'``.   In HTSQL, *literal* values are
single quoted, in this way we know ``'eng'`` isn't the name of a column.

Complex filters can be created using boolean connectors, such as the
conjunction (``&``) and alternation (``|``) operators .  The following
request returns programs in the 'School of Business' that do not
grant a 'Bachelor of Science' degree (C2_)::

    /program?school='bus'&degree!='bs'

    program                                                    
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    school | code | title                              | degree
    -------+------+------------------------------------+-------
    bus    | mba  | Masters of Business Administration | mb    
    bus    | pacc | Graduate Certificate in Accounting | ct    
    bus    | pcap | Certificate in Capital Markets     | ct
    ...

.. _C2: 
    http://demo.htsql.org
    /program?school='bus'&degree!='bs'

Filters can be combined with selectors and links.  The following request
returns courses, listing only department number and title, having less
than 3 credits in the school of natural science (C3_)::

    /course{department, number, title}?
       credits<3&department.school='ns'

    course                                              
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    department | number | title                         
    -----------+--------+-------------------------------
    phys       | 388    | Experimental Physics I        
    chem       | 115    | Organic Chemistry Laboratory I
    astro      | 142    | Solar System Lab              
    astro      | 155    | Telescope Workshop            
    ...

.. _C3: 
    http://demo.htsql.org
    /course{department, number, title}?
       credits<3&department.school='ns'

It is sometimes desirable to specify the filter before the selector.
Using a *table expression*, denoted by parenthesis, the previous request
is equivalent to (C4_)::

    /(course?credits<3&department.school='ns')
      {department, number, title}

.. _C4: 
    http://demo.htsql.org
    /(course?credits<3&department.school='ns')
      {department, number, title}

HTSQL supports a whole suite of functions and predicator operators.
Further, through the plug-in mechanism, custom data types, operators,
and functions may be integrated to support domain specific needs.

Formatters
----------

Once data is selected, linked and filtered, it is formatted for the
response.  By default, HTSQL uses the ``Accept`` header to negotiate the
output format with the user agent.  This can be overridden with a format
command, such as ``/:json``.  For example, results in JSON format (RFC
4627) can be requested as follows (D1_)::

    /school/:json

    [
      ["art", "School of Art and Design"],
      ["bus", "School of Business"],
      ["edu", "College of Education"],
      ["egn", "School of Engineering"],
      ["la", "School of Arts, Letters, and the Humanities"],
      ["mart", "School of Modern Art"],
      ["mus", "Musical School"],
      ["ns", "School of Natural Sciences"],
      ["sc", "School of Continuing Studies"]
    ]

.. _D1: 
    http://demo.htsql.org
    /school/:json

Other formats include ``/:txt`` for plain-text formatting, ``/:html`` for
display in web browsers, and ``/:csv`` for data exchange.

Putting it All Together
-----------------------

The following request selects records from the ``course`` table,
filtered by all departments in the 'School of Business', sorted by
``course`` ``title``, including ``department``'s ``code`` and ``name``,
and returned as a "Comma-Separated Values" (RFC 4180) (E1_)::

    /course{department{code,name},number,title+}?
      department.school='bus'/:csv

    department.code,department.name,number,title
    corpfi,Corporate Finance,234,Accounting Information Systems
    acc,Accounting,527,Advanced Accounting
    capmrk,Capital Markets,756,Capital Risk Management
    corpfi,Corporate Finance,601,Case Studies in Corporate Finance
    ... 

.. _E1: 
    http://demo.htsql.org
    /course{department{code,name},number,title+}?
          department.school='bus'/:csv
    
HTSQL requests are powerful without being complex.  They are easy to
read and modify.  They adapt to changes in the database.  These
qualities increase the usability of databases by all types of users and
reduce the likelihood of costly errors.
