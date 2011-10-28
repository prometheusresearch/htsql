--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- --------------------------------------------------------------------
-- The standard HTSQL regression schema for PostgreSQL
--


-- --------------------------------------------------------------------
-- AD -- Administrative Directory
--

CREATE SCHEMA ad;
COMMENT ON SCHEMA ad IS $_$
Administrative Directory
~~~~~~~~~~~~~~~~~~~~~~~~

This is the basic 4-table schema used by the tutorial and regression
tests.  It is designed to use limited data types (varchar, memo, and
integer) as well as simple primary and foreign key relationships.

There are two top-level tables, ``department`` and ``school`` having a
single-column primary key ``code`` and a unique ``name``.  In this
schema, we associate departments with exactly one school, although the
``code`` for the department must be unique across schools.  Non-academic
departments are modeled with a ``NULL`` for their ``school_code``.

Two second-tier tables, ``course`` and ``program`` have compound primary
keys, consisting of a parent table and a second column.  For ``course``
table, we have an integer key, ``no`` which is a 3-digit course
code, scoped by department.  Hence, ``mth.101`` is distinct from
``eng.101`` even though they have the same course number.  The
``degree`` column of ``program`` is an authority controlled field, with
values ``ms``, ``bs`` and ``cert``; it is optional permitting programs
which do not offer a degree.

::

  +-------------------------+              +---------------------+
  | DEPARTMENT              |              | SCHOOL              |
  +-------------------------+              +---------------------+
  | code                 PK |--\       /--o| code             PK |----\
  | school_code          FK |>-|------/    | name          NN,UK |    |
  | name              NN,UK |  |   .       | campus              |    |
  +-------------------------+  |    .      +---------------------+    |
                             . |     .                              . |
            a department    .  |   departments      a school       .  |
            offers zero or .   |   belong to        administers zero  |
            more courses       |   at most one      or more programs  |
                               |   school                             | 
  +-------------------------+  |           +---------------------+    | 
  | COURSE                  |  |           | PROGRAM             |    |
  +-------------------------+  |           +---------------------+    |
  | department_code  FK,PK1 |>-/           | school_code  PK1,FK |>---/
  | no                  PK2 |              | code            PK2 |----\    
  | title                NN |              | title            NN |    |
  | credits              NN |              | degree           CK |    |
  | description             |              | part_of          FK |>---/
  +-------------------------+              +---------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TABLE ad.school (
    code                VARCHAR(16) NOT NULL,
    name                VARCHAR(64) NOT NULL,
    campus              VARCHAR(5),
    CONSTRAINT school_pk
      PRIMARY KEY (code),
    CONSTRAINT name_uk
      UNIQUE (name),
    CONSTRAINT school_campus_ck
      CHECK (campus IN ('old', 'north', 'south'))
);

CREATE TABLE ad.department (
    code                VARCHAR(16) NOT NULL,
    name                VARCHAR(64) NOT NULL,
    school_code         VARCHAR(16),
    CONSTRAINT department_pk
      PRIMARY KEY (code),
    CONSTRAINT department_name_uk
      UNIQUE (name),
    CONSTRAINT department_school_fk
      FOREIGN KEY (school_code)
      REFERENCES ad.school(code)
);

CREATE TABLE ad.program (
    school_code         VARCHAR(16) NOT NULL,
    code                VARCHAR(16) NOT NULL,
    title               VARCHAR(64) NOT NULL,
    degree              CHAR(2),
    part_of             VARCHAR(16),
    CONSTRAINT program_pk
      PRIMARY KEY (school_code, code),
    CONSTRAINT program_title_uk
      UNIQUE (title),
    CONSTRAINT program_degree_ck
      CHECK (degree IN ('bs', 'pb', 'ma', 'ba', 'ct', 'ms', 'ph')),
    CONSTRAINT program_school_fk
      FOREIGN KEY (school_code)
      REFERENCES ad.school(code),
   CONSTRAINT program_part_of_fk
      FOREIGN KEY (school_code, part_of)
      REFERENCES ad.program(school_code, code)
);

CREATE TABLE ad.course (
    department_code     VARCHAR(16) NOT NULL,
    no                  INTEGER NOT NULL,
    title               VARCHAR(64) NOT NULL,
    credits             INTEGER,
    description         TEXT,
    CONSTRAINT course_pk
      PRIMARY KEY (department_code, no),
    CONSTRAINT course_title_uk
      UNIQUE (title),
    CONSTRAINT course_dept_fk
      FOREIGN KEY (department_code)
      REFERENCES ad.department(code)
);


-- --------------------------------------------------------------------
-- ID -- Instructor Directory
--

CREATE SCHEMA id;
COMMENT ON SCHEMA id IS $_$
Instructor Directory
~~~~~~~~~~~~~~~~~~~~

This schema introduces 3 tables, a top-level ``instructor`` table
enumerating members of the university teaching staff, and two other
tables: ``appointment`` and ``confidential``.   These tables provide an
example of two concepts, a "cross product" (via ``appointment``) and
"facet" (via ``confidential``).  A cross product is a relationship
between two tables, in this case, ``department`` and ``instructor``.  A
facet is a set of columns attached to another table, either to address
permission differentiation or optionality.  Facets are useful for
representing sets of mutually-mandatory fields.  In this example, the
``confidential`` represents a table that is optionally present, and
likely having different permission restrictions.

This schema also introduces two data types.  The social security number,
or ``SSN`` of each instructor is a character value of fixed width.  The
``fraction`` column of ``appointment`` table is a ``DECIMAL(3,2)``
representing a number such as ``0.50`` for a half-time appointment of an
instructor to a given department.

::

  +-------------------------+              +---------------------+
  | APPOINTMENT             |      /-------| DEPARTMENT          |
  |-------------------------|      |  .    +---------------------+
  | department_code  FK,PK1 |>-----/   .
  | instructor_code  FK,PK2 |>-----\     a department may have many
  | fraction                |      |     instructors with part-time
  +-------------------------+    . |     teaching appointments
                                .  |
         an instructor may have    |       +---------------------+
         teaching appointments     |       | INSTRUCTOR          |
         in many departments       |       +---------------------+
                                   \-- /---| code             PK |
  +-------------------------+          |   | title            NN |
  | CONFIDENTIAL            |          |   | full_name        NN |
  +-------------------------+          |   | phone               |
  | instructor_code   FK,PK |o-------- /   | email               |
  | SSN                  NN |   .          +---------------------+
  | pay_grade            NN |    .
  | home_phone              |      an instructor may have a record
  +-------------------------+      holding confidential information


  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TABLE id.instructor (
    code                VARCHAR(16) NOT NULL,
    title               VARCHAR(4) NOT NULL,
    full_name           VARCHAR(64) NOT NULL,
    phone               VARCHAR(16),
    email               VARCHAR(64),
    CONSTRAINT instructor_pk
      PRIMARY KEY (code),
    CONSTRAINT instructor_title_ck
      CHECK (title IN ('mr', 'dr', 'prof', 'ms'))
);

CREATE TABLE id.confidential (
    instructor_code     VARCHAR(16) NOT NULL,
    SSN                 CHAR(11) NOT NULL,
    pay_grade           DECIMAL(1,0) NOT NULL,
    home_phone          VARCHAR(16),
    CONSTRAINT confidential_pk
      PRIMARY KEY (instructor_code),
    CONSTRAINT confidential_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES id.instructor(code)
);

CREATE TABLE id.appointment (
    department_code     VARCHAR(16) NOT NULL,
    instructor_code     VARCHAR(16) NOT NULL,
    fraction            DECIMAL(3,2),
    CONSTRAINT appointment_pk
      PRIMARY KEY (department_code, instructor_code),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department_code)
      REFERENCES ad.department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES id.instructor(code)
);


-- --------------------------------------------------------------------
-- CD -- Class Directory
--

CREATE SCHEMA cd;
COMMENT ON SCHEMA cd IS $_$
Class Directory
~~~~~~~~~~~~~~~

The principal purpose of this schema is to provide a highly linked table
with many foreign key references.  The table here, ``class``, represents
a ``course`` offering during a particular ``semester`` delivered by a
given ``instructor``.  The ``semester`` table has a compound primary key
of ``year`` and ``period``.  The ``class`` table has a 5 column primary
key, 2 columns referencing a given course (by number), 2 columns
referencing a semester, and finally, a 3-digit ``section`` number.

Since the natural primary key is so large, a secondary candidate key
``class_seq`` is provided.  This mandatory and unique column is then
referenced by down-stream tables, such as student enrollment.

::

  +--------------------------+              +---------------------+
  | COURSE                   |---\          | SEMESTER            |
  +--------------------------+   |          +---------------------+
                               . |   /------| year            PK1 |
          each course may be  .  |   |      | season          PK2 |
          offered many times     |   |      | begin_date       NN |
          in a given quarter     |   | .    | end_date         NN |
                                 |   |  .   +---------------------+
  +--------------------------+   |   |
  | CLASS                    |   |   |   each section of a class
  +--------------------------+   |   |   is tracked by semester
  | department_code  PK1,FK1 |\__/   |
  | course_no        PK2,FK2 |/      |    classes are taught by
  | year             PK3,FK1 |\______/    an instructor
  | season           PK4,FK2 |/          .
  | section              PK5 |          .   +---------------------+
  | instructor_code      FK  |>-------------| INSTRUCTOR          |
  | class_seq   NN,UK        |              +---------------------+
  +--------------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TYPE cd.season_t AS ENUM ('fall', 'spring', 'summer');

CREATE DOMAIN cd.year_t AS DECIMAL(4,0);

CREATE TABLE cd.semester (
    year                cd.year_t NOT NULL,
    season              cd.season_t NOT NULL,
    begin_date          DATE NOT NULL,
    end_date            DATE NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season)
);

CREATE SEQUENCE class_seq START 20001;

CREATE TABLE cd.class (
    department_code     VARCHAR(16) NOT NULL,
    course_no           INTEGER NOT NULL,
    year                cd.year_t NOT NULL,
    season              cd.season_t NOT NULL,
    section             CHAR(3) NOT NULL,
    instructor_code     VARCHAR(16),
    class_seq           INTEGER NOT NULL DEFAULT nextval('class_seq'),
    CONSTRAINT class_pk
      PRIMARY KEY (department_code, course_no, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_course_fk
      FOREIGN KEY (department_code, course_no)
      REFERENCES ad.course(department_code, no),
    CONSTRAINT class_semester_fk
      FOREIGN KEY (year, season)
      REFERENCES cd.semester(year, season),
    CONSTRAINT class_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES id.instructor(code)
);


-- --------------------------------------------------------------------
-- ED -- Enrollment Directory
--

CREATE SCHEMA ed;
COMMENT ON SCHEMA ed IS $_$
Enrollment Directory
~~~~~~~~~~~~~~~~~~~~

This schema provides a fact table for use in demonstrating aggregates in
multiple dimensions.  The ``enrollment``, while only directly
referencing ``class`` and ``student``, indirectly provides summary
statics options by semester, course, program, department and other
dimensions.  The ``enrollment`` table is also a cross-product table,
reflecting a "membership" of a student within a given class.  Cross
products are used in relational databases to model many-to-many
relationships, in this case, the relationship has a few attribute
columns: ``status`` and ``grade``.

::

  +--------------------+              +---------------------+
  | CLASS              |---\          | PROGRAM             |----\
  +--------------------+   |          +---------------------+    |
                           |                                   . |
    each class may have    |            students are admitted .  |
    several student     .  |            into a school program    |
    enrollments          . |                                     |
                           |          +---------------------+    |
  +--------------------+   |          | STUDENT             |    |
  | ENROLLMENT         |   |          +---------------------+    |
  +--------------------+   |     /----| id               PK |    |
  | class_seq   PK, FK |>--/     |    | name             NN |    |
  | student_id  PK, FK |---------/    | gender           NN |    |
  | status             |     .        | dob              NN |    |
  | grade              |    .         | school_code     FK1 |\___/
  +--------------------+   .          | program_code    FK2 |/
                                      | start_date       NN |
    students may enroll in            | is_active        NN |
    one or more classes               +---------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TYPE ed.enrollment_status_t AS ENUM ('enr', 'inc', 'ngr');
CREATE TYPE ed.student_gender_t AS ENUM ('f', 'i', 'm');

CREATE TABLE ed.student (
    id                  INTEGER NOT NULL,
    name                VARCHAR(64) NOT NULL,
    gender              ed.student_gender_t NOT NULL,
    dob                 DATE NOT NULL,
    school_code         VARCHAR(16),
    program_code        VARCHAR(16),
    start_date          DATE NOT NULL,
    is_active           BOOLEAN NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (id),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES ad.program(school_code, code)
);

CREATE TABLE ed.enrollment (
    student_id          INTEGER NOT NULL,
    class_seq           INTEGER NOT NULL,
    status              ed.enrollment_status_t NOT NULL,
    grade               DECIMAL(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student_id, class_seq),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student_id)
      REFERENCES ed.student(id),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class_seq)
      REFERENCES cd.class(class_seq)
);


-- --------------------------------------------------------------------
-- RD -- Requirement Directory
--

CREATE SCHEMA rd;
COMMENT ON SCHEMA rd IS $_$
Requirement Directory
~~~~~~~~~~~~~~~~~~~~~

This schema focuses on recursive and linked structures.  The
``prerequisite`` cross-product table represents a many-to-many
relationship from ``course`` onto itself.  The ``classification`` table
represents a one-to-many self relationship, forming a a hierarchy.  The
``course_classification`` table is simply another cross-product tables
representing membership, associating a ``course`` with a
``classification``.

The classification table models distributional requirements, where a
course may be tagged with a specific program requirement, such as
``"art-history"`` which also acts as a broader distributional
requirement, ``"humanities"``.

A ``program_requirement`` table models an optional The hierarchy is
interpreted to imply that a class tagged with the former also counts to
fulfill requirements of the latter.

::

  +-----------------------------+
  | PREREQUISITE                |            two foreign keys denote a
  +-----------------------------+          . dependency *by* a course,
  | by_department_code  FK1,PK1 |\_______ .  such as chem.100, *on*
  | by_course_no     F   K2,PK2 |/       \   another, such as mth.101
  | on_department_code  FK1,PK3 |\______ |
  | on_course_no        FK2,PK4 |/     | |  +---------------------+
  +-----------------------------+   /- \-\--| COURSE              |
                                    |       +---------------------+
         a course can be a member   |
         of several classifications |       +---------------------+
                                 .  |       | CLASSIFICATION      |
  +-----------------------------+ . |       +---------------------+
  | COURSE_CLASSIFICATION       |   | /- /--| code             PK |----\
  +-----------------------------+   | |  |  | type             NN |    |
  | department_code     FK1,PK1 |\__/ |  |  | title            NN |    |
  | course_no           FK2,PK2 |/    |  |  | description         |    |
  | classification_code  FK,PK3 |>----/  |  | part_of          FK |>---/
  +-----------------------------+  .     |  +---------------------+  .
                                  .      |                          .
          a classification is used       |    a classification may be
          to tag multiple courses   /----/    part of a broader category
                                    |
  +-----------------------------+   | . courses, by classification, are
  | PROGRAM_REQUIREMENT         |   |.  required by a given program
  +-----------------------------+   |
  | school_code         FK1,PK1 |\__|____     +---------------------+
  | program_code        FK2,PK2 |/  |    `----| PROGRAM             |
  | classification_code  FK,PK3 |>--/   .     +---------------------+
  | credit_hours             NN |      .
  | rationale                   |    programs require class credits
  +-----------------------------+    specified via classifications

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TABLE rd.prerequisite (
    of_department_code  VARCHAR(16) NOT NULL,
    of_course_no        INTEGER NOT NULL,
    on_department_code  VARCHAR(16) NOT NULL,
    on_course_no        INTEGER NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department_code, of_course_no,
                   on_department_code, on_course_no),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department_code, on_course_no)
      REFERENCES ad.course(department_code, no),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department_code, of_course_no)
      REFERENCES ad.course(department_code, no)
);

CREATE TYPE rd.classification_type_t
  AS ENUM ('department', 'school', 'university');

CREATE TABLE rd.classification (
    code                VARCHAR(16) NOT NULL,
    type                rd.classification_type_t,
    title               VARCHAR(64) NOT NULL,
    description         TEXT,
    part_of             VARCHAR(16),
    CONSTRAINT classification_pk
      PRIMARY KEY (code),
    CONSTRAINT classification_title_uk
      UNIQUE (title),
    CONSTRAINT classification_part_of_fk
      FOREIGN KEY (part_of)
      REFERENCES rd.classification(code)
);

CREATE TABLE rd.course_classification (
    department_code     VARCHAR(16) NOT NULL,
    course_no           INTEGER NOT NULL,
    classification_code VARCHAR(16) NOT NULL,
    CONSTRAINT course_classification_pk
      PRIMARY KEY (department_code, course_no, classification_code),
    CONSTRAINT course_classification_course_fk
      FOREIGN KEY (department_code, course_no)
      REFERENCES ad.course(department_code, no),
    CONSTRAINT course_classification_classification_fk
      FOREIGN KEY (classification_code)
      REFERENCES rd.classification(code)
);

CREATE TABLE rd.program_requirement (
    school_code         VARCHAR(16) NOT NULL,
    program_code        VARCHAR(16) NOT NULL,
    classification_code VARCHAR(16) NOT NULL,
    credit_hours        INTEGER NOT NULL,
    rationale           TEXT,
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school_code, program_code, classification_code),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES ad.program(school_code, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification_code)
      REFERENCES rd.classification(code)
);


