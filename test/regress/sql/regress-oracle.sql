--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- Authors: Clark C. Evans <cce@clarkevans.com>,
--          Kirill Simonov <xi@resolvent.net>
--


-- --------------------------------------------------------------------
-- The standard HTSQL regression schema for Oracle
--


-- --------------------------------------------------------------------
-- AD -- Administrative Directory
--

CREATE TABLE school (
    code        VARCHAR2(16) NOT NULL,
    name        VARCHAR2(64) NOT NULL,
    CONSTRAINT school_pk
      PRIMARY KEY (code),
    CONSTRAINT name_uk
      UNIQUE (name)
);

CREATE TABLE department (
    code        VARCHAR2(16) NOT NULL,
    name        VARCHAR2(64) NOT NULL,
    school      VARCHAR2(16),
    CONSTRAINT department_pk
      PRIMARY KEY (code),
    CONSTRAINT department_name_uk
      UNIQUE (name),
    CONSTRAINT department_school_fk
      FOREIGN KEY (school)
      REFERENCES school(code)
);

CREATE TABLE program (
    school      VARCHAR2(16) NOT NULL,
    code        VARCHAR2(16) NOT NULL,
    title       VARCHAR2(64) NOT NULL,
    degree      CHAR(2),
    part_of     VARCHAR2(16),
    CONSTRAINT program_pk
      PRIMARY KEY (school, code),
    CONSTRAINT program_title_uk
      UNIQUE (title),
    CONSTRAINT program_degree_ck
      CHECK (degree IN ('bs', 'pb', 'ma', 'ba', 'ct', 'ms','ph')),
    CONSTRAINT program_school_fk
      FOREIGN KEY (school)
      REFERENCES school(code),
   CONSTRAINT program_part_of_fk
      FOREIGN KEY (school, part_of)
      REFERENCES program(school, code)
);

CREATE TABLE course (
    department  VARCHAR2(16) NOT NULL,
    no          NUMBER(3) NOT NULL,
    title       VARCHAR2(64) NOT NULL,
    credits     NUMBER(1),
    description CLOB,
    CONSTRAINT course_pk
      PRIMARY KEY (department, no),
    CONSTRAINT course_title_uk
      UNIQUE (title),
    CONSTRAINT course_dept_fk
      FOREIGN KEY (department)
      REFERENCES department(code)
);


-- --------------------------------------------------------------------
-- ID -- Instructor Directory
--

CREATE TABLE instructor (
    code        VARCHAR2(16) NOT NULL,
    title       VARCHAR2(4) NOT NULL,
    full_name   VARCHAR2(64) NOT NULL,
    phone       VARCHAR2(16),
    email       VARCHAR2(64),
    CONSTRAINT instructor_pk
      PRIMARY KEY (code),
    CONSTRAINT instructor_title_ck
      CHECK (title IN ('mr', 'dr', 'prof', 'ms'))
);

CREATE TABLE confidential (
    instructor  VARCHAR2(16) NOT NULL,
    SSN         CHAR(11) NOT NULL,
    pay_grade   NUMBER(1) NOT NULL,
    home_phone  VARCHAR2(16),
    CONSTRAINT confidential_pk
      PRIMARY KEY (instructor),
    CONSTRAINT confidential_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES instructor(code)
);

CREATE TABLE appointment (
    department  VARCHAR2(16) NOT NULL,
    instructor  VARCHAR2(16) NOT NULL,
    fraction    NUMBER(3,2),
    CONSTRAINT appointment_pk
      PRIMARY KEY (department, instructor),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department)
      REFERENCES department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES instructor(code)
);


-- --------------------------------------------------------------------
-- CD -- Class Directory
--

CREATE TABLE semester (
    year        NUMBER(4) NOT NULL,
    season      VARCHAR2(6) NOT NULL,
    begin_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season),
    CONSTRAINT semester_season_ck
      CHECK (season IN ('fall', 'spring', 'summer'))
);

CREATE SEQUENCE class_seq START WITH 20001;

CREATE TABLE class (
    department  VARCHAR2(16) NOT NULL,
    course      NUMBER(3) NOT NULL,
    year        NUMBER(4) NOT NULL,
    season      VARCHAR2(6) NOT NULL,
    section     CHAR(3) NOT NULL,
    instructor  VARCHAR2(16),
    class_seq   NUMBER(38) NOT NULL,
    CONSTRAINT class_pk
      PRIMARY KEY (department, course, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_season_ck
       CHECK (season IN ('fall', 'spring', 'summer')),
    CONSTRAINT class_department_fk
      FOREIGN KEY (department)
      REFERENCES department(code),
    CONSTRAINT class_course_fk
      FOREIGN KEY (department, course)
      REFERENCES course(department, no),
    CONSTRAINT class_semester_fk
      FOREIGN KEY (year, season)
      REFERENCES semester(year, season),
    CONSTRAINT class_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES instructor(code)
);

CREATE TRIGGER class_insert_trg
    BEFORE INSERT ON class
    FOR EACH ROW WHEN (NEW.class_seq IS NULL)
BEGIN
    SELECT class_seq.nextval INTO :NEW.class_seq FROM DUAL;
END;


-- --------------------------------------------------------------------
-- ED -- Enrollment Directory
--

CREATE TABLE student (
    id          NUMBER(38) NOT NULL,
    name        VARCHAR2(64) NOT NULL,
    gender      CHAR(1) NOT NULL,
    dob         DATE NOT NULL,
    school      VARCHAR2(16),
    program     VARCHAR2(16),
    start_date  DATE NOT NULL,
    is_active   NUMBER(1) NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (id),
    CONSTRAINT student_gender_ck
       CHECK (gender IN ('f', 'i', 'm')),
    CONSTRAINT is_active_ck
      CHECK (is_active IN (0, 1)),
    CONSTRAINT student_school_fk
      FOREIGN KEY (school)
      REFERENCES school (code),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school, program)
      REFERENCES program (school, code)
);

CREATE TABLE enrollment (
    student     NUMBER(38) NOT NULL,
    class       NUMBER(38) NOT NULL,
    status      CHAR(3) NOT NULL,
    grade       NUMBER(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student, class),
    CONSTRAINT enrollment_status_ck
       CHECK (status IN ('enr', 'inc', 'ngr')),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student)
      REFERENCES student(id),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class)
      REFERENCES class(class_seq)
);


-- --------------------------------------------------------------------
-- RD -- Requirement Directory
--

CREATE TABLE prerequisite (
    of_department   VARCHAR2(16) NOT NULL,
    of_course       NUMBER(3) NOT NULL,
    on_department   VARCHAR2(16) NOT NULL,
    on_course       NUMBER(3) NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department, of_course, on_department, on_course),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department, on_course)
      REFERENCES course(department, no),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department, of_course)
      REFERENCES course(department, no)
);

CREATE TABLE classification (
    code        VARCHAR2(16) NOT NULL,
    type        VARCHAR2(10),
    title       VARCHAR2(64) NOT NULL,
    description CLOB,
    part_of     VARCHAR2(16),
    CONSTRAINT classification_pk
      PRIMARY KEY (code),
    CONSTRAINT classification_title_uk
      UNIQUE (title),
    CONSTRAINT classification_type_ck
       CHECK (type IN ('department', 'school', 'university')),
    CONSTRAINT classification_part_of_fk
      FOREIGN KEY (part_of)
      REFERENCES classification(code)
);

CREATE TABLE course_classification (
    department      VARCHAR2(16) NOT NULL,
    course          NUMBER(3) NOT NULL,
    classification  VARCHAR2(16) NOT NULL,
    CONSTRAINT course_classification_pk
      PRIMARY KEY (department, course, classification),
    CONSTRAINT course_classification_cours_fk
      FOREIGN KEY (department, course)
      REFERENCES course(department, no),
    CONSTRAINT course_classification_class_fk
      FOREIGN KEY (classification)
      REFERENCES classification(code)
);

CREATE TABLE program_requirement (
    school          VARCHAR2(16) NOT NULL,
    program         VARCHAR2(16) NOT NULL,
    classification  VARCHAR2(16) NOT NULL,
    credit_hours    NUMBER(2) NOT NULL,
    rationale       CLOB,
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school, program, classification),
    CONSTRAINT program_classification_cour_fk
      FOREIGN KEY (school, program)
      REFERENCES program(school, code),
    CONSTRAINT program_classification_clas_fk
      FOREIGN KEY (classification)
      REFERENCES classification(code)
);


