--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- Authors: Clark C. Evans <cce@clarkevans.com>,
--          Kirill Simonov <xi@resolvent.net>
--


-- --------------------------------------------------------------------
-- The standard HTSQL regression schema for MySQL
--


-- --------------------------------------------------------------------
-- Administrative Directory
--

CREATE SCHEMA ad;

CREATE TABLE ad.school (
    code        VARCHAR(16) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    CONSTRAINT school_pk
      PRIMARY KEY (code),
    CONSTRAINT name_uk
      UNIQUE (name)
);

CREATE TABLE ad.department (
    code        VARCHAR(16) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    school      VARCHAR(16),
    CONSTRAINT department_pk
      PRIMARY KEY (code),
    CONSTRAINT department_name_uk
      UNIQUE (name),
    CONSTRAINT department_school_fk
      FOREIGN KEY (school)
      REFERENCES ad.school(code)
);

CREATE TABLE ad.program (
    school      VARCHAR(16) NOT NULL,
    code        VARCHAR(16) NOT NULL,
    title       VARCHAR(64) NOT NULL,
    degree      CHAR(2),
    part_of     VARCHAR(16),
    CONSTRAINT program_pk
      PRIMARY KEY (school, code),
    CONSTRAINT program_title_uk
      UNIQUE (title),
    CONSTRAINT program_degree_ck
      CHECK (degree IN ('bs', 'pb', 'ma', 'ba', 'ct', 'ms','ph')),
    CONSTRAINT program_school_fk
      FOREIGN KEY (school)
      REFERENCES ad.school(code),
   CONSTRAINT program_part_of_fk
      FOREIGN KEY (school, part_of)
      REFERENCES ad.program(school, code)
);

CREATE TABLE ad.course (
    department  VARCHAR(16) NOT NULL,
    no          INTEGER NOT NULL,
    title       VARCHAR(64) NOT NULL,
    credits     INTEGER,
    description VARCHAR(MAX),
    CONSTRAINT course_pk
      PRIMARY KEY (department, no),
    CONSTRAINT course_title_uk
      UNIQUE (title),
    CONSTRAINT course_dept_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code)
);


-- --------------------------------------------------------------------
-- Instructor Directory
--

CREATE SCHEMA id;

CREATE TABLE id.instructor (
    code        VARCHAR(16) NOT NULL,
    title       VARCHAR(4) NOT NULL,
    full_name   VARCHAR(64) NOT NULL,
    phone       VARCHAR(16),
    email       VARCHAR(64),
    CONSTRAINT instructor_pk
      PRIMARY KEY (code),
    CONSTRAINT instructor_title_ck
      CHECK (title IN ('mr', 'dr', 'prof', 'ms'))
);

CREATE TABLE id.confidential (
    instructor  VARCHAR(16) NOT NULL,
    SSN         CHAR(11) NOT NULL,
    pay_grade   DECIMAL(1,0) NOT NULL,
    home_phone  VARCHAR(16),
    CONSTRAINT confidential_pk
      PRIMARY KEY (instructor),
    CONSTRAINT confidential_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES id.instructor(code)
);

CREATE TABLE id.appointment (
    department  VARCHAR(16) NOT NULL,
    instructor  VARCHAR(16) NOT NULL,
    fraction    DECIMAL(3,2),
    CONSTRAINT appointment_pk
      PRIMARY KEY (department, instructor),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES id.instructor(code)
);


-- --------------------------------------------------------------------
-- Class Directory
--

CREATE SCHEMA cd;

CREATE TABLE cd.semester (
    year        DECIMAL(4,0) NOT NULL,
    season      VARCHAR(6) NOT NULL,
    begin_date  DATETIME NOT NULL,
    end_date    DATETIME NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season),
    CONSTRAINT semester_season_ck
      CHECK (season IN ('fall', 'spring', 'summer'))
);

CREATE TABLE cd.class (
    department  VARCHAR(16) NOT NULL,
    course      INTEGER NOT NULL,
    year        DECIMAL(4,0) NOT NULL,
    season      VARCHAR(6) NOT NULL,
    section     CHAR(3) NOT NULL,
    instructor  VARCHAR(16),
    class_seq   INTEGER IDENTITY(20001,1) NOT NULL,
    CONSTRAINT class_pk
      PRIMARY KEY (department, course, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_season_ck
      CHECK (season IN ('fall', 'spring', 'summer')),
    CONSTRAINT class_department_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code),
    CONSTRAINT class_course_fk
      FOREIGN KEY (department, course)
      REFERENCES ad.course(department, no),
    CONSTRAINT class_semester_fk
      FOREIGN KEY (year, season)
      REFERENCES cd.semester(year, season),
    CONSTRAINT class_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES id.instructor(code)
);


-- --------------------------------------------------------------------
-- ED -- Enrollment Directory
--

CREATE SCHEMA ed;

CREATE TABLE ed.student (
    id          INTEGER NOT NULL,
    name        VARCHAR(64) NOT NULL,
    gender      CHAR(1) NOT NULL,
    dob         DATETIME NOT NULL,
    school      VARCHAR(16),
    program     VARCHAR(16),
    start_date  DATETIME NOT NULL,
    is_active   BIT NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (id),
    CONSTRAINT student_gender_ck
       CHECK (gender IN ('f', 'i', 'm')),
    CONSTRAINT student_school_fk
      FOREIGN KEY (school)
      REFERENCES ad.school (code),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school, program)
      REFERENCES ad.program (school, code)
);

CREATE TABLE ed.enrollment (
    student     INTEGER NOT NULL,
    class       INTEGER NOT NULL,
    status      CHAR(3) NOT NULL,
    grade       DECIMAL(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student, class),
    CONSTRAINT enrollment_status_ck
       CHECK (status IN ('enr', 'inc', 'ngr')),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student)
      REFERENCES ed.student(id),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class)
      REFERENCES cd.class(class_seq)
);


-- --------------------------------------------------------------------
-- RD -- Requirement Directory
--

CREATE SCHEMA rd;

CREATE TABLE rd.prerequisite (
    of_department   VARCHAR(16) NOT NULL,
    of_course       INTEGER NOT NULL,
    on_department   VARCHAR(16) NOT NULL,
    on_course       INTEGER NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department, of_course, on_department, on_course),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department, on_course)
      REFERENCES ad.course(department, no),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department, of_course)
      REFERENCES ad.course(department, no)
);

CREATE TABLE rd.classification (
    code        VARCHAR(16) NOT NULL,
    type        VARCHAR(10),
    title       VARCHAR(64) NOT NULL,
    description VARCHAR(MAX),
    part_of     VARCHAR(16),
    CONSTRAINT classification_pk
      PRIMARY KEY (code),
    CONSTRAINT classification_title_uk
      UNIQUE (title),
    CONSTRAINT classification_type_ck
       CHECK (type IN ('department', 'school', 'university')),
    CONSTRAINT classification_part_of_fk
      FOREIGN KEY (part_of)
      REFERENCES rd.classification(code)
);

CREATE TABLE rd.course_classification (
    department      VARCHAR(16) NOT NULL,
    course          INTEGER NOT NULL,
    classification  VARCHAR(16) NOT NULL,
    CONSTRAINT course_classification_pk
      PRIMARY KEY (department, course, classification),
    CONSTRAINT course_classification_course_fk
      FOREIGN KEY (department, course)
      REFERENCES ad.course(department, no),
    CONSTRAINT course_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES rd.classification(code)
);

CREATE TABLE rd.program_requirement (
    school          VARCHAR(16) NOT NULL,
    program         VARCHAR(16) NOT NULL,
    classification  VARCHAR(16) NOT NULL,
    credit_hours    INTEGER NOT NULL,
    rationale       VARCHAR(MAX),
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school, program, classification),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school, program)
      REFERENCES ad.program(school, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES rd.classification(code)
);


