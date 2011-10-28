--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- See `LICENSE` for license information, `AUTHORS` for the list of authors.
--


-- --------------------------------------------------------------------
-- The standard HTSQL regression schema for MySQL
--


-- --------------------------------------------------------------------
-- Administrative Directory
--

CREATE SCHEMA ad;

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
    description         VARCHAR(MAX),
    CONSTRAINT course_pk
      PRIMARY KEY (department_code, no),
    CONSTRAINT course_title_uk
      UNIQUE (title),
    CONSTRAINT course_dept_fk
      FOREIGN KEY (department_code)
      REFERENCES ad.department(code)
);


-- --------------------------------------------------------------------
-- Instructor Directory
--

CREATE SCHEMA id;

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
-- Class Directory
--

CREATE SCHEMA cd;

CREATE TABLE cd.semester (
    year                DECIMAL(4,0) NOT NULL,
    season              VARCHAR(6) NOT NULL,
    begin_date          DATETIME NOT NULL,
    end_date            DATETIME NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season),
    CONSTRAINT semester_season_ck
      CHECK (season IN ('fall', 'spring', 'summer'))
);

CREATE TABLE cd.class (
    department_code     VARCHAR(16) NOT NULL,
    course_no           INTEGER NOT NULL,
    year                DECIMAL(4,0) NOT NULL,
    season              VARCHAR(6) NOT NULL,
    section             CHAR(3) NOT NULL,
    instructor_code     VARCHAR(16),
    class_seq           INTEGER IDENTITY(20001,1) NOT NULL,
    CONSTRAINT class_pk
      PRIMARY KEY (department_code, course_no, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_season_ck
      CHECK (season IN ('fall', 'spring', 'summer')),
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

CREATE TABLE ed.student (
    id                  INTEGER NOT NULL,
    name                VARCHAR(64) NOT NULL,
    gender              CHAR(1) NOT NULL,
    dob                 DATETIME NOT NULL,
    school_code         VARCHAR(16),
    program_code        VARCHAR(16),
    start_date          DATETIME NOT NULL,
    is_active           BIT NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (id),
    CONSTRAINT student_gender_ck
       CHECK (gender IN ('f', 'i', 'm')),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES ad.program(school_code, code)
);

CREATE TABLE ed.enrollment (
    student_id          INTEGER NOT NULL,
    class_seq           INTEGER NOT NULL,
    status              CHAR(3) NOT NULL,
    grade               DECIMAL(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student_id, class_seq),
    CONSTRAINT enrollment_status_ck
       CHECK (status IN ('enr', 'inc', 'ngr')),
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

CREATE TABLE rd.classification (
    code                VARCHAR(16) NOT NULL,
    type                VARCHAR(10),
    title               VARCHAR(64) NOT NULL,
    description         VARCHAR(MAX),
    part_of             VARCHAR(16),
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
    rationale           VARCHAR(MAX),
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school_code, program_code, classification_code),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES ad.program(school_code, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification_code)
      REFERENCES rd.classification(code)
);


