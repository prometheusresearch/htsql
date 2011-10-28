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

CREATE TABLE school (
    code                VARCHAR(16) NOT NULL,
    name                VARCHAR(64) NOT NULL,
    campus              VARCHAR(5),
    CONSTRAINT school_pk
      PRIMARY KEY (code),
    CONSTRAINT name_uk
      UNIQUE (name),
    CONSTRAINT school_campus_ck
      CHECK (campus IN ('old', 'north', 'south'))
) ENGINE=INNODB;

CREATE TABLE department (
    code                VARCHAR(16) NOT NULL,
    name                VARCHAR(64) NOT NULL,
    school_code         VARCHAR(16),
    CONSTRAINT department_pk
      PRIMARY KEY (code),
    CONSTRAINT department_name_uk
      UNIQUE (name),
    CONSTRAINT department_school_fk
      FOREIGN KEY (school_code)
      REFERENCES school(code)
) ENGINE=INNODB;

CREATE TABLE program (
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
      REFERENCES school(code),
   CONSTRAINT program_part_of_fk
      FOREIGN KEY (school_code, part_of)
      REFERENCES program(school_code, code)
) ENGINE=INNODB;

CREATE TABLE course (
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
      REFERENCES department(code)
) ENGINE=INNODB;


-- --------------------------------------------------------------------
-- Instructor Directory
--

CREATE TABLE instructor (
    code                VARCHAR(16) NOT NULL,
    title               VARCHAR(4) NOT NULL,
    full_name           VARCHAR(64) NOT NULL,
    phone               VARCHAR(16),
    email               VARCHAR(64),
    CONSTRAINT instructor_pk
      PRIMARY KEY (code),
    CONSTRAINT instructor_title_ck
      CHECK (title IN ('mr', 'dr', 'prof', 'ms'))
) ENGINE=INNODB;

CREATE TABLE confidential (
    instructor_code     VARCHAR(16) NOT NULL,
    SSN                 CHAR(11) NOT NULL,
    pay_grade           DECIMAL(1,0) NOT NULL,
    home_phone          VARCHAR(16),
    CONSTRAINT confidential_pk
      PRIMARY KEY (instructor_code),
    CONSTRAINT confidential_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES instructor(code)
) ENGINE=INNODB;

CREATE TABLE appointment (
    department_code     VARCHAR(16) NOT NULL,
    instructor_code     VARCHAR(16) NOT NULL,
    fraction            DECIMAL(3,2),
    CONSTRAINT appointment_pk
      PRIMARY KEY (department_code, instructor_code),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department_code)
      REFERENCES department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES instructor(code)
) ENGINE=INNODB;


-- --------------------------------------------------------------------
-- CD -- Class Directory
--

CREATE TABLE semester (
    year                DECIMAL(4,0) NOT NULL,
    season              ENUM('fall', 'spring', 'summer') NOT NULL,
    begin_date          DATE NOT NULL,
    end_date            DATE NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season)
) ENGINE=INNODB;

CREATE TABLE class (
    department_code     VARCHAR(16) NOT NULL,
    course_no           INTEGER NOT NULL,
    year                DECIMAL(4,0) NOT NULL,
    season              ENUM('fall', 'spring', 'summer') NOT NULL,
    section             CHAR(3) NOT NULL,
    instructor_code     VARCHAR(16),
    class_seq           INTEGER NOT NULL AUTO_INCREMENT,
    CONSTRAINT class_pk
      PRIMARY KEY (department_code, course_no, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_course_fk
      FOREIGN KEY (department_code, course_no)
      REFERENCES course(department_code, no),
    CONSTRAINT class_semester_fk
      FOREIGN KEY (year, season)
      REFERENCES semester(year, season),
    CONSTRAINT class_instructor_fk
      FOREIGN KEY (instructor_code)
      REFERENCES instructor(code)
) ENGINE=INNODB;


-- --------------------------------------------------------------------
-- ED -- Enrollment Directory
--

CREATE TABLE student (
    id                  INTEGER NOT NULL,
    name                VARCHAR(64) NOT NULL,
    gender              ENUM('f', 'i', 'm') NOT NULL,
    dob                 DATE NOT NULL,
    school_code         VARCHAR(16),
    program_code        VARCHAR(16),
    start_date          DATE NOT NULL,
    is_active           BOOLEAN NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (id),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES program (school_code, code)
) ENGINE=INNODB;

CREATE TABLE enrollment (
    student_id          INTEGER NOT NULL,
    class_seq           INTEGER NOT NULL,
    status              ENUM('enr', 'inc', 'ngr') NOT NULL,
    grade               DECIMAL(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student_id, class_seq),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student_id)
      REFERENCES student(id),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class_seq)
      REFERENCES class(class_seq)
) ENGINE=INNODB;


-- --------------------------------------------------------------------
-- RD -- Requirement Directory
--

CREATE TABLE prerequisite (
    of_department_code  VARCHAR(16) NOT NULL,
    of_course_no        INTEGER NOT NULL,
    on_department_code  VARCHAR(16) NOT NULL,
    on_course_no        INTEGER NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department_code, of_course_no,
                   on_department_code, on_course_no),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department_code, on_course_no)
      REFERENCES course(department_code, no),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department_code, of_course_no)
      REFERENCES course(department_code, no)
) ENGINE=INNODB;

CREATE TABLE classification (
    code                VARCHAR(16) NOT NULL,
    type                ENUM('department', 'school', 'university'),
    title               VARCHAR(64) NOT NULL,
    description         TEXT,
    part_of             VARCHAR(16),
    CONSTRAINT classification_pk
      PRIMARY KEY (code),
    CONSTRAINT classification_title_uk
      UNIQUE (title),
    CONSTRAINT classification_part_of_fk
      FOREIGN KEY (part_of)
      REFERENCES classification(code)
) ENGINE=INNODB;

CREATE TABLE course_classification (
    department_code     VARCHAR(16) NOT NULL,
    course_no           INTEGER NOT NULL,
    classification_code VARCHAR(16) NOT NULL,
    CONSTRAINT course_classification_pk
      PRIMARY KEY (department_code, course_no, classification_code),
    CONSTRAINT course_classification_course_fk
      FOREIGN KEY (department_code, course_no)
      REFERENCES course(department_code, no),
    CONSTRAINT course_classification_classification_fk
      FOREIGN KEY (classification_code)
      REFERENCES classification(code)
) ENGINE=INNODB;

CREATE TABLE program_requirement (
    school_code         VARCHAR(16) NOT NULL,
    program_code        VARCHAR(16) NOT NULL,
    classification_code VARCHAR(16) NOT NULL,
    credit_hours        INTEGER NOT NULL,
    rationale           TEXT,
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school_code, program_code, classification_code),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school_code, program_code)
      REFERENCES program(school_code, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification_code)
      REFERENCES classification(code)
) ENGINE=INNODB;


