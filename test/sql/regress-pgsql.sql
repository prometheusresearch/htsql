--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- Authors: Clark C. Evans <cce@clarkevans.com>,
--          Kirill Simonov <xi@resolvent.net>
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
``code`` for the department must be unique across schools.  Non-acedemic
departments are modeled with a ``NULL`` for their ``school``.

Two second-tier tables, ``course`` and ``program`` have compound primary
keys, consisting of a parent table and a second column.  For ``course``
table, we have an integer key, ``number`` which is a 3-digit course
code, scoped by department.  Hence, ``mth.101`` is distinct from
``eng.101`` even though they have the same course number.  The
``degree`` column of ``program`` is an authority controlled field, with
values ``ms``, ``bs`` and ``cert``; it is optional permitting programs
which do not offer a degree.

::

  +--------------------+              +---------------------+
  | DEPARTMENT         |              | SCHOOL              |
  +--------------------+              +---------------------+
  | code            PK |--\       /---| code             PK |----\
  | school          FK |>-|------/    | name          NN,UK |    |
  | name         NN,UK |  |    .      +---------------------+    |
  +--------------------+  |     .                              . |
                        . |  departments                      .  |
       a department    .  |  belong to         a school          |
       offers zero or .   |  at most one       administers zero  |
       more courses       |  school            or more programs  |
                          |                                      | 
  +--------------------+  |           +---------------------+    | 
  | COURSE             |  |           | PROGRAM             |    |
  +--------------------+  |           +---------------------+    |
  | department  FK,PK1 |>-/           | school       PK1,FK |>---/
  | number         PK2 |              | code            PK2 |>---\    
  | title           NN |              | title            NN |    |
  | credits         NN |              | degree           CK |    |
  | description        |              | part_of          FK |----/
  +--------------------+              +---------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

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
    number      INTEGER NOT NULL,
    title       VARCHAR(64) NOT NULL,
    credits     INTEGER,
    description TEXT,
    CONSTRAINT course_pk
      PRIMARY KEY (department, number),
    CONSTRAINT course_title_uk
      UNIQUE (title),
    CONSTRAINT course_dept_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code)
);

INSERT INTO ad.school (code, name) VALUES
('art', 'School of Art and Design'),
('bus', 'School of Business'),
('edu', 'College of Education'),
('eng', 'School of Engineering'),
('la', 'School of Arts and Humanities'),
('mus', 'School of Music & Dance'),
('ns', 'School of Natural Sciences'),
('ph', 'Public Honorariums'),
('sc', 'School of Continuing Studies')
;

INSERT INTO ad.department (code, name, school) VALUES
('astro', 'Astronomy', 'ns'),
('chem', 'Chemistry', 'ns'),
('phys', 'Physics', 'ns'),
('mth', 'Mathematics', 'ns'),
('eng', 'English', 'la'),
('lang', 'Foreign Languages', 'la'),
('hist', 'History', 'la'),
('psych', 'Psychology', 'la'),
('poli', 'Political Science', 'la'),
('comp', 'Computer Science', 'eng'),
('ee', 'Electrical Engineering', 'eng'),
('me', 'Mechanical Engineering', 'eng'),
('be', 'Bioengineering', 'eng'),
('arthis', 'Art History', 'art'),
('stdart', 'Studio Art', 'art'),
('tched', 'Teacher Education', 'edu'),
('edpol', 'Educational Policy', 'edu'),
('corpfi', 'Corporate Finance', 'bus'),
('acc', 'Accounting', 'bus'),
('capmrk', 'Capital Markets', 'bus'),
('voc', 'Vocals', 'mus'),
('pia', 'Piano', 'mus'),
('win', 'Wind', 'mus'),
('str', 'Strings', 'mus'),
-- Non-Academic Departments
('bursar', 'Bursar''s Office', NULL),
('career', 'Career Development', NULL),
('parent', 'Parents & Alumni', NULL)
;

INSERT INTO ad.program (school, code, title, degree, part_of) VALUES
('ns', 'uastro', 'Bachelor of Science in Astronomy', 'bs', NULL),
('ns', 'uchem', 'Bachelor of Science in Chemistry', 'bs', NULL),
('ns', 'uphys', 'Bachelor of Science in Physics', 'bs', NULL),
('ns', 'pmth', 'Doctorate of Science in Mathematics', 'ph', NULL),
('ns', 'gmth', 'Masters of Science in Mathematics', 'bs', 'pmth'),
('ns', 'umth', 'Bachelor of Science in Mathematics', 'bs', 'gmth'),
('la', 'upsych', 'Bachelor of Arts in Psychology', 'ba', NULL),
('la', 'upolisci', 'Bachelor of Arts in Political Science', 'ba', NULL),
('la', 'gscitch', 'Master of Arts in Science Teaching', 'ma', NULL),
('la', 'psciwri', 'Science Writing', 'ct', NULL),
('la', 'gengl', 'Master of Arts in English', 'ma', NULL),
('la', 'uengl', 'Bachelor of Arts in English', 'ba', 'gengl'),
('la', 'uhist', 'Bachelor of Arts in History', 'ba', NULL),
('la', 'uspan', 'Bachelor of Arts in Spanish', 'ba', NULL),
('la', 'glang', 'Master of Arts in Modern Languages', 'ma', NULL),
('eng', 'gee', 'M.S. in Electrical Engineering', 'ms', NULL),
('eng', 'gme', 'M.S. in Mechanical Engineering', 'ms', NULL),
('eng', 'gbe', 'M.S. in Bioengineering', 'ms', NULL),
('eng', 'uelec', 'B.S. in Electrical Engineering', 'bs', 'gee'),
('eng', 'umech', 'B.S. in Mechanical Engineering', 'bs', 'gme'),
('eng', 'ubio', 'B.S. in Bioengineering', 'bs', 'gbe'),
('eng', 'ucompsci', 'B.S. in Computer Science', 'bs', NULL),
('eng', 'gbuseng', 'M.S. in Business and Engineering', 'ms', NULL),
('edu', 'umath', 'Bachelor of Arts in Math Education', 'ba', NULL),
('edu', 'usci', 'Bachelor of Arts in Science Education', 'ba', NULL),
('edu', 'psci', 'Certificate in Science Teaching', 'ct', NULL),
('edu', 'glited', 'Master of Arts in Literacy Education', 'ma', NULL),
('edu', 'gedlead', 'Master of Arts in Education Leadership', 'ma', NULL),
('edu', 'gedu', 'M.S. in Education', 'ms', NULL),
('edu', 'gtch', 'Master of Arts in Teaching', 'ma', NULL),
('bus', 'uacct', 'B.S. in Accounting', 'bs', NULL),
('bus', 'ucorpfi', 'B.S. in Corporate Finance', 'bs', NULL),
('bus', 'ubusad', 'B.S. in Business Administration', 'bs', NULL),
('bus', 'pacc', 'Graduate Certificate in Accounting', 'ct', NULL),
('bus', 'pcap', 'Certificate in Capital Markets', 'ct', NULL),
('art', 'gart', 'Post Baccalaureate in Art History', 'pb', NULL),
('art', 'uhist', 'Bachelor of Arts in Art History', 'ba', NULL),
('art', 'ustudio', 'Bachelor of Arts in Studio Art', 'ba', NULL),
('ph', 'phd', 'Honorary PhD', NULL, NULL)
;

INSERT INTO ad.course (department, number, title, credits, description) VALUES
('astro', 137, 'The Solar System', 3, 'Introductory survey of the solar system, including structure and motion of the planets, properties of the sun, and comparison to extrasolar systems.'),
('astro', 142, 'Solar System Lab', 2, 'Laboratory studies that complement the lecture course ASTRO 137.'),
('astro', 155, 'Telescope Workshop', 1, 'Introduction to correct use of the 8-inch Schmidt-Cassegrain type telescope. You will learn about magnification, how to locate an object, and how setting circles work.'),
('astro', 254, 'Life in the Universe', 3, 'Nature and origin of large numbers in the cosmos, the formation of planets, formation of life, and the anthropic principle.'),
('mth', 101, 'College Algebra', 5, 'This course, taken mostly by members from the School of Business is a primary graduate student funding vehicle for the the Department of Mathematics.  This is a 1000 person lecture reviews basic concepts taught in high school.'),
('chem', 100, 'Principles of Chemistry', 3, 'This course offers an introduction to the biological, inorganic, and chemical molecules, with an emphasis on basic principles of atomic and molecular electronic structure.'),
('chem', 110, 'Organic Chemistry I', 3, 'This course offers a practical introduction to organic chemistry, including a full set of problem sets with solutions. Focus is on the basic principles of understanding the structure and reactivity of organic molecules.'),
('chem', 115, 'Organic Chemistry Laboratory I', 2, 'Complements Organic Chemistry I. Practical laboratory experiments.'),
('chem', 314, 'Laboratory Chemistry', 3, 'Experimental chemistry for students who are planning to major in chemistry. Covers principles and applications of chemical laboratory techniques, including preparation and analysis of chemical materials.'),
('chem', 655, 'Protein Folding Problem', 3, 'Focuses on the mechanisms through which the amino acid sequence of polypeptide chains determine their three-dimensional conformation.'),
('phys', 211, 'General Physics I', 3, 'This course deals primarily with motions of objects and the forces that underlie these motions, including free fall, collisions between objects, rolling, and spinning.'),
('phys', 303, 'Relativity & Cosmology', 3, 'The course will describe developments that led to special relativity and its implications about the nature of space and time, as well as general relativity and quantum mechanics.'),
('phys', 388, 'Experimental Physics I', 2, 'In this course students will perform several experiments in different areas of physics. They will also learn fundamental experimental techniques and how to record and report results and perform analysis.'),
('phys', 512, 'Quantum Field Theory', 3, 'This course will cover the basic quantization of bosonic and fermionic fields, discrete spacetime symmetries, perturbative methods in QFT, definition of the S-matrix for scattering and decay processes.'),
('phys', 713, 'Quarks, Nuclei, and Cosmology', 3, 'General topics covered will include the structure of the nucleon, beta decay and weak interactions, and nucleon-nucleon interaction.'),
('phys', 819, 'Superconductivity', 3, 'This course provides a phenomenological approach to superconductivity, emphasizing super-conducting electronics.'),
('eng', 101, 'Introduction to Composition', 3, 'Provides students with the fundamental skills to read, interpret, and write critically at the college level.'),
('eng', 247, 'Boccaccio''s Decameron', 3, 'Follows of the arc of of the career Boccaccio with an emphasis on the Decameron, which is read in light of its cultural density and contextualized in terms of its antecedents, especially the Commedia of Dante.'),
('eng', 311, 'Writing Styles', 3, 'This practical course aids in understanding and writing non-fiction. We will look at some of the ways that prominent English writers have constructed their sentences and paragraphs. Students will write weekly exercises exploring these stylistic patterns.'),
('eng', 175, 'College Newspaper', 2, 'Students will perform journalistic writing exercises for publication in the college newspaper.'),
('eng', 276, 'Introduction to Science Writing', 3, 'This course provides an introduction to science writing. Students will learn about the business of science writing and will become familiar with the craft of making complex scientific research understandable for the general public.'),
('eng', 412, 'Ecology Writing Workshop', 2, 'This monthly workshop will focus on creation of a course assignment, a feature-length magazine article covering a complex topic in ecology.'),
('hist', 112, 'The United States in World History', 3, 'Examines the meaning of empire in relationship to the historical development of the United States of America.'),
('hist', 212, 'Historical Perspective on the Constitution', 3, 'This course covers the development of the constitutional doctrine from 1787 to the present. The Constitution as an experiment in Republicanism.'),
('hist', 415, 'History of the Family in Global Perspective', 3, 'Has the family really declined? What has changed in the last 1000 years? Drawing on cross-cultural examples, primarily from Latin America, the U.S. and Europe, this seminar explores the varieties of domestic forms.'),
('hist', 505, 'Science and History', 3, 'Introduces students to approaches and methods in the history of science, technology, and medicine'),
('hist', 333, 'History of American Education', 3, 'A study of informal and formal education in American history leading to an understanding of present educational theory and practice.'),
('lang', 201, 'Introduction to Spanish', 4, 'Introduction to the Spanish language, with an emphasis on every day conversation and basic grammar.'),
('lang', 203, 'Intermediate Spanish', 3, 'Continuation of fundamental Spanish language learning, including verb tenses, reading skills, and basic conversation.'),
('lang', 304, 'Spanish Conversation Group', 2, 'Informal weekly conversation group designed to improve understanding of spoken Spanish. Suitable for both majors and casual speakers who wish to improve their skills for personal enjoyment or travel.'),
('lang', 207, 'Child Second Language Development', 3, 'Examines issues in child second language acquisition, including the critical period hypothesis and universal grammar.'),
('lang', 305, 'Second Language Syntax', 3, 'This course examines the form and acquisition of nonnative syntax. Consideration of whether nonnative grammars are fundamentally different than native grammars.'),
('psych', 102, 'General Psychology', 3, 'This course introduces the student to the major topics in scientific psychology as applied to human behavior. Applications of these principles will be made to the human experience.'),
('psych', 304, 'Introduction to Cognitive Psychology', 3, 'An introduction to the basic concepts of cognitive psychology, including areas such as perception, attention, memory, language, and thought.'),
('psych', 450, 'Laboratory in Applied Behavioral Science', 4, 'This course will provide students with hands-on training in the application of behavioral research technology to a clinical population.'),
('psych', 560, 'Examination of Real-Time Language Processing', 4, 'This lab course examines methods for the real-time examination of language processing in normal and disordered  language populations.'),
('psych', 610, 'Applied Child Psychology', 3, 'Introduction to major concepts and models used in psychological assessment and psychotherapeutic intervention of children. Several modalities of psychotherapy (individual, group, and family) will be reviewed along with research on their efficacy.'),
('poli', 113, 'American Government and Politics', 3, 'This course examines the structure, services, functions, and problems of government and politics at the national level.'),
('poli', 347, 'American Foreign Policy', 3, 'Theories, processes, and problem of American foreign policy and the craft of diplomacy, with special attention to contemporary issues.'),
('poli', 402, 'Government Internship: Semester in Washington', 4, 'Junior or seniors with a 3.0 grade point average or higher may apply for a limited number of internship opportunities in Washington, DC. Interns will live at University House on Capitol Hill and continue their normal class schedule at the DC Campus.'),
('poli', 644, 'Research Seminar in Middle Eastern Affairs', 3, 'Government and Politics of the Middle East and North Africa Spring. Historical background, contemporary setting, political processes, and major problems of some of the countries of Middle East and North Africa.'),
('poli', 715, '#5 is the 50% Solution', 3, 'A history of the two-state solution and other approaches to Palestian Statehood'),
('poli', 431, 'American Government and Corporate Interests', 3, 'This course will examine the methods by which American business exert influence over legislators and the legislative process.'),
('comp', 102, 'Introduction to Computer Science', 3, 'This course in an introduction to the discipline of computer science. Topics include algorithmic foundations, hardware concepts, virtual machine concepts, software systems, applications, and social issues.'),
('comp', 230, 'History of Computing', 3, 'This course will survey the history of the computing field from antiquity to the present, focusing on the era of the electronic digital computer. Topics will include historical developments in hardware, software, and the theoretical foundations of computer science.'),
('comp', 350, 'Introduction to Signal Processing', 3, 'This course covers the nature of information, signals, transforms, and applications. Topics include analog to digital and digital to analog conversion, data storage (such as the audio format MP3), data transforms, and filters.'),
('comp', 615, 'Introduction to Automata', 3, 'Theory of computing devices and the languages they recognize.'),
('comp', 710, 'Laboratory in Computer Science', 4, 'Independent research opportunity using the university computer lab. Requires instructor permission and may be repeated.'),
('comp', 810, 'Thesis Research', 3, 'Guided research leading to production of the thesis. Requires instructor permission and may be repeated.'),
('comp', 819, 'Advanced Algorithms in Bioinformatics', 3, 'This course is focused on fundamental algorithmic techniques in Bioinformatics, including classed methods such as dynamic programming, support vector machines and other statistical and learning optimization methods.'),
('ee', 107, 'Exploration of Electrical Engineering', 3, 'Exploration of electrical engineering through several hands-on activities that cover a broad spectrum of applications and fundamental concepts. '),
('ee', 202, 'Engineering Electromagnetics', 3, 'Static electric and magnetic fields; solutions to static field problems, electromagnetic waves, boundary conditions, engineering applications.'),
('ee', 412, 'Laboratory in Electrical Engineering', 4, 'Hands-on experience covering areas of optical transforms, electro-optics devices, signal processing, fiber optics transmission, and holography.'),
('ee', 505, 'Information Theory', 3, 'Mathematical measurement of information; information transfer in discrete systems; redundancy, efficiency, and channel capacity; encoding systems.'),
('ee', 615, 'Learning and Adaptive Systems', 3, 'Adaptive and learning control systems; system identification; performance indices; gradient, stochastic approximation, controlled random search methods; introduction to pattern recognition.'),
('me', 111, 'Introduction to Mechanical Engineering', 3, 'Topics include an overview of career opportunities, problem solving processes, an introduction to the basic engineering design process, professionalism, professional registration, and ethics.'),
('me', 344, 'Undergraduate Research', 4, 'Undergraduates will conduct independent research activities under the direction of their major adviser.'),
('me', 501, 'Advanced Welding', 3, 'Advanced applications of welding and machine tool technology. Computer numerical control, multi-axis machining set-up, gas tungsten arc welding, and gas metal arch welding.'),
('me', 627, 'Advanced Heating and Air Conditioning', 4, ''),
('me', 712, 'Graphic Communication and Design', 3, 'Sketching and orthographic projection. Covers detail and assembly working drawings, dimensioning, tolerance specification, and design projects.'),
('be', 112, 'Introduction to Biomedical Engineering', 3, 'This course covers topics in multiple formats ranging from lectures by faculty or guest speakers to presentations by participating students.'),
('be', 308, 'Fundamentals of Biochemistry', 3, 'Fundamental aspects of human biochemistry are introduced in this course for students in the bioinstrumentation/biosensors, biomechanics, and medical-imaging tracks.'),
('be', 415, 'Internship in Biomedical Engineering', 8, 'The student will work twenty hours per week in an area firm to gain experience in the application of biomedical engineering principles in an industrial setting.'),
('be', 509, 'Systems of Drug Delivery', 3, 'The mathematics of diffusion through various types of biological media is discussed.'),
('arthis', 202, 'History of Art Criticism', 3, 'An introductory survey course on Prehistoric through late-Medieval art history.'),
('arthis', 712, 'Museum and Gallery Management', 4, 'Supervised independent field experience and practical work in all areas of Art Museum management in the university and greater metropolitan area communities.'),
('arthis', 340, 'Arts of Asia', 3, 'An introduction to the history and criticism of Far Eastern art, including the art of China and Japan, fine and decorative arts.'),
('arthis', 710, 'Methods in Art History', 3, 'This seminar focuses on basic types of art-historical method. Some meetings focus on a single author who exemplifies a particular approach.'),
('arthis', 809, 'Materials and Construction in European Art', 3, 'A scientific examination of the materials and manufacturing techniques employed in Europe over the last two centuries.'),
('arthis', 623, 'Contemporary Latin American Art', 3, 'A survey of the last twenty years of Latin American art with a focus on the Caribbean and Central America.'),
('stdart', 714, 'Peer Portfolio Review', 0, 'An opportunity to practice giving and receiving constructive criticism.'),
('stdart', 411, 'Underwater Basket Weaving', 4, 'This course provides a novel perspective on the traditional art of basketry as it is experienced in reduced gravity and in the context of fluid dynamics. Requires instructor permission and a valid c-card.'),
('stdart', 512, 'Art in Therapy', 3, 'Surveys methods and results of using art and craft therapy with developmentally disabled adults.'),
('stdart', 614, 'Drawing Master Class', 5, 'For fine arts majors only, an intensive studio study including field trips to local parks and museums and a final group art show.'),
('stdart', 509, 'Twentieth Century Printmaking', 4, 'Development of personalized concepts and individual aesthetic expression in printmaking with reference to various styles and trends in Twentieth Century printmaking.'),
('stdart', 333, 'Drawing', 3, 'Exploration of the structure and interrelationships of visual form in drawing, painting, and sculpture. Principal historical modes of drawing are examined.'),
('tched', 122, 'Theory and Practice of Early Childhood Education', 3, 'Emphasis on the skills and processes needed for the design and implementation of optimal learning environments. Exploration of issues related to societal and cultural influences on the education of young children.'),
('tched', 155, 'Methods of Early Science Education', 3, 'A study of the curriculum methods, concepts, techniques, and materials in the teaching of general science to children in the early grades.'),
('tched', 367, 'Problems in Education Management', 3, 'This course is designed to assist the student to prepare for management of educational organizations and programs. Emphasis will be placed upon identifying specific problems and developing specific techniques by which to solve them.'),
('tched', 501, 'Challenges of Teaching the Gifted and Talented', 3, 'The nature and needs of the talented and gifted in all areas of development are explored.'),
('tched', 609, 'Supervised Internship in Education', 4, 'Supervised Internship I provides on-site, supervised instructional experience within a public school setting under the leadership of an appropriate, competent professional.'),
('edpol', 202, 'Technology in the Classroom', 3, 'Theories and practice of using educational technologies to support problem-based learning.'),
('edpol', 551, 'Classroom Visit', NULL, 'Elective visit to a local classroom for observation.'),
('stdart', 119, 'Spring Basket Weaving Workshop', NULL, 'A just-for-fun chance to learn the basics of basket weaving.'),
('edpol', 313, 'Technology, Society and Schools', 3, 'Examination of theories and history of interaction of society and technology with implications for instructional technology and schooling. Resources for constructing personal definitions of technology.'),
('edpol', 505, 'Qualitative Research for Educators', 3, 'This course provides an introduction to qualitative research at the Master level.'),
('edpol', 617, 'Educational Policy Analysis', 3, 'Frameworks for analyzing, designing policy proposals, and implementing plans.'),
('corpfi', 234, 'Accounting Information Systems', 3, 'This course bridges the gap between two disciplines critical to business operations.  This course of study teaches students to design and deploy information technology to improve the accounting systems of an organization.'),
('corpfi', 404, 'Corporate Financial Management', 3, 'This course covers advanced topics in corporate financial management, including its role in corporate governance.'),
('corpfi', 601, 'Case Studies in Corporate Finance', 3, 'A course designed to use case studies and financial analysis to further knowledge and ability to make financial management decisions.'),
('acc', 100, 'Practical Bookkeeping', 2, NULL),
('acc', 200, 'Principles of Accounting I', 3, 'The initial course in the theory and practice of financial accounting. Topics emphasized include the preparation, reporting, and analysis of financial data.'),
('acc', 315, 'Financial Accounting', 5, 'Integration of the conceptual and computational aspects of asset, liability and stockholders equity accounting.'),
('acc', 426, 'Corporate Taxation', 3, 'Concepts and methods of determining federal tax liability of corporations.'),
('acc', 527, 'Advanced Accounting', 3, 'Theory and practical applications of accounting for consolidated entities and partnerships; includes foreign currency transactions, hedging and derivatives.'),
('acc', 606, 'Corporate Financial Law', 3, 'Law governing business corporations; fiduciary duties of managers and directors in situations such as mergers, acquisitions, securities offerings, market domination, litigation.'),
('capmrk', 712, 'International Financial Markets', 3, 'Offers an understanding of the international financial structure and studies its impact on business and individuals in various nations.'),
('capmrk', 808, 'Principles of Portfolio Management', 3, 'Comprehensive coverage of the theory and practice of money management as well as in-depth analysis of the theory and practice involved when securities are combined into portfolios.'),
('capmrk', 818, 'Financial Statement Analysis', 3, 'This course presents techniques for analyzing current and projected financial statements for the purposes of credit analysis, security analysis, and internal financial analysis, and cash flow forecasting.'),
('capmrk', 756, 'Capital Risk Management', 3, 'This course introduces fundamental principles and techniques of financial risk management.')
;


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
``percent`` column of ``appointment`` table is a ``DECIMAL(3,2)``
representing a number such as ``0.50`` for a half-time appointment of an
instructor to a given department.

::

  +--------------------+              +---------------------+
  | APPOINTMENT        |      /-------| DEPARTMENT          |
  |--------------------|      |  .    +---------------------+
  | department  FK,PK1 |>-----/   .
  | instructor  FK,PK2 |>-----\     a department may have many
  | percent            |      |     instructors with part-time
  +--------------------+    . |     teaching appointments
                           .  |
    an instructor may have    |       +---------------------+
    teaching appointments     |       | INSTRUCTOR          |
    in many departments       |       +---------------------+
                              \-- /---| code             PK |
  +--------------------+          |   | title            NN |
  | CONFIDENTIAL       |          |   | full_name        NN |
  +--------------------+          |   | phone               |
  | instructor   FK,PK |o-------- /   | email               |
  | SSN             NN |   .          +---------------------+
  | pay_grade       NN |    .
  | home_phone         |      an instructor may have a record
  +--------------------+      holding confidential information


  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

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
    percent     DECIMAL(3,2),
    CONSTRAINT appointment_pk
      PRIMARY KEY (department, instructor),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES id.instructor(code)
);

INSERT INTO id.instructor (code, title, full_name, phone, email) VALUES
('cfergus12', 'prof', 'Adam Ferguson', NULL, 'cfergus12@example.com'),
('evargas112', 'prof', 'Elena Vargas', '555-1572', 'evargas112@example.com'),
('afrenski', 'mr', 'Andre Frenski', '555-1723', 'afrenski@example.com'),
('wyu112', 'mr', 'Walter Yu', '555-2954', 'wyu112@example.com'),
('amiller213', 'ms', 'Antoinette Miller', '555-7728', 'amiller213@example.com'),
('dsims51', 'mr', 'Dante Sims', NULL, 'dsims51@example.com'),
('srandrews', 'mr', 'Stuart Andrews', '555-2113', 'srandrews@example.com'),
('sbyrne202', 'ms', 'Susan Byrne', '555-9002', NULL),
('sbadhreya', 'prof', 'Said Badhreya', '555-2873', 'sbadhreya@example.com'),
('lmcooper11', 'prof', 'Louisa M. Cooper', '555-2112', 'lmscooper11@example.com'),
('mbyer55', 'prof', 'Michael L. Byer', '555-1287', NULL),
('pblum21', 'ms', 'Petra Blum', '555-2873', 'pblum21@example.com'),
('icampbell12', 'prof', 'Ian W. Campbell', '555-2275', 'icampbell12@example.com'),
('tobrien21', 'prof', 'Timothy O''Brien', NULL, 'tobrien21@example.com'),
('acaspar', 'dr', 'Alesia Caspar', NULL, NULL),
('dbundt31', 'dr', 'David Bundt', '555-1553', 'dbundt31@example.com'),
('kmarkman', 'ms', 'Kristen Maison', NULL, 'kmarkman@example.com'),
('kmaas11', 'prof', 'Kari Maas', '555-1027', 'kmaas11@example.com'),
('hbenmahem', 'mr', 'Hani Ben-Mahem', '555-1827', 'hbenmahem@example.com'),
('bburling', 'prof', 'Benjamin Burling', '555-1823', 'bburling@example.com'),
('mcardana', 'prof', 'Maximo Cardana', '555-1738', 'mcardana@example.com'),
('jconnell51', 'dr', 'Jamie Connell', '555-2157', 'jconnell51@example.com'),
('vball77', 'ms', 'Vivienne Ball', '555-2830', NULL),
('kmurray44', 'mr', 'Kevin Murray', '555-1753', 'kmurray44@example.com'),
('lbrooks61', 'prof', 'Lynn L. Brooks', '555-8872', 'lbrooks61@example.com'),
('anabib', 'prof', 'Ashish Nabib', '555-1667', 'anabib@example.com'),
('alang42', 'prof', 'Adrian Laang', '555-0973', 'alang42@example.com'),
('kcavallaro', 'prof', 'Katherine Cavallaro', '555-4325', 'kvallaro@example.com'),
('hbarone', 'prof', 'Harold Barone', '555-0911', 'hbarone@example.com'),
('emurphy55', 'prof', 'Erin L. Murphy', NULL, 'emurphy55@example.com'),
('egasner', 'dr', 'Ernst Gasner', '555-6652', 'egasner@example.com'),
('elhill4', 'dr', 'Ellen Last Hill', '555-1995', 'ehill4@example.com'),
('rrosenfeld31', 'ms', 'Rebecca L. Rosenfeld', '555-8816', 'rrosenfeld31@example.com'),
('astone77', 'mr', 'Alan P. Stone', '555-1738', 'astone77@example.com'),
('dfallon23', 'prof', 'David N. Fallon', '555-1666', 'dfallon23@example.com'),
('jflug29', 'dr', 'Jason Flug', '555-6672', 'jflug23@example.com'),
('asacco', 'prof', 'Andrea Sacco', '555-1381', 'asacco@example.com'),
('bsacks66', 'prof', 'Benjamin Sacks', '555-2212', 'bsacks66@example.com'),
('mscott51', 'prof', 'Mindy Scott', '555-3521', 'mscott51@example.com')
;

INSERT INTO id.confidential (instructor, SSN, pay_grade, home_phone) VALUES
('cfergus12', '987-65-4320', 6, '702-555-1738'),
('afrenski', '987-65-4321', 4, NULL),
('wyu112', '987-65-4323', 5, '702-555-2954'),
('amiller213', '987-65-4324', 7, '452-555-7728'),
('dsims51', '987-65-4325', 5, '452-555-9273'),
('srandrews', '987-65-4326', 8, '702-555-3627'),
('sbyrne202', '987-65-4327', 5, '617-555-8382'),
('sbadhreya', '987-65-4328', 4, '702-555-66738'),
('lmcooper11', '987-65-4329', 8, '702-555-9992'),
('mbyer55', '000-22-4320', 6, '452-555-7311'),
('pblum21', '000-33-2783', 5, '702-555-6522'),
('icampbell12', '000-52-8758', 5, NULL),
('tobrien21', '000-38-2875', 4, NULL),
('bburling', '672-88-0000', 5, NULL),
('dbundt31', '000-53-2873', 7, '202-555-1738'),
('kmarkman', '000-72-1875', 4, '702-555-8211'),
('kmaas11', '371-55-0000', 7, '702-555-1875'),
('hbenmahem', '674-57-0000', 4, '702-555-0115'),
('jconnell51', '717-67-0000', 8, '702-555-1672'),
('vball77', '799-11-0000', 4, '702-555-1425'),
('kmurray44', '152-62-0000', 7, '702-555-6612'),
('lbrooks61', '673-11-0000', 8, '452-555-7276'),
('anabib', '787-22-0000', 7, '702-55-1627'),
('alang42', '788-33-0000', 6, '702-555-1721'),
('kcavallaro', '123-74-0000', 5, '702-555-1670'),
('hbarone', '511-66-0000', 4, '702-555-1089'),
('emurphy55', '787-22-0000', 8, '452-555-7849'),
('egasner', '784-44-0000', 8, '702-555-8995'),
('elhill4', '933-55-0000', 4, '702-555-8829'),
('rrosenfeld31', '857-22-0000', 7, '702-555-0989'),
('astone77', '688-33-0000', 8, '702-555-0173'),
('dfallon23', '274-66-0000', 4, '702-555-1778'),
('jflug29', '578-41-0000', 6, '702-555-7727'),
('asacco', '783-78-0000', 5, '702-555-1692'),
('bsacks66', '782-78-0000', 7, '202-555-7283'),
('mscott51', '126-33-0000', 7, '702-555-7819')
;

INSERT INTO id.appointment (department, instructor, percent) VALUES
('stdart', 'acaspar', 1.00),
('phys', 'afrenski', 1.00),
('ee', 'alang42', 1.00),
('lang', 'amiller213', 1.00),
('comp', 'anabib', 0.50),
('astro', 'asacco', NULL),
('corpfi', 'astone77', 1.00),
('phys', 'bburling', 1.00),
('chem', 'bsacks66', 1.00),
('astro', 'cfergus12', 0.50),
('tched', 'dbundt31', 0.50),
('acc', 'dfallon23', 1.00),
('hist', 'dsims51', 0.50),
('stdart', 'egasner', 1.00),
('tched', 'elhill4', 1.00),
('arthis', 'emurphy55', 0.50),
('chem', 'evargas112', 1.00),
('be', 'hbarone', 0.50),
('chem', 'hbenmahem', 0.50),
('arthis', 'icampbell12', 1.00),
('lang', 'jconnell51', 1.00),
('capmrk', 'jflug29', 1.00),
('me', 'kcavallaro', 1.00),
('astro', 'kmaas11', NULL),
('edpol', 'kmarkman', 1.00),
('psych', 'kmurray44', 1.00),
('poli', 'lbrooks61', 0.50),
('ee', 'lmcooper11', 1.00),
('me', 'mbyer55', 1.00),
('eng', 'mcardana', NULL),
('phys', 'mscott51', 1.00),
('be', 'pblum21', 0.50),
('edpol', 'rrosenfeld31', 1.00),
('psych', 'srandrews', 1.00),
('comp', 'sbadhreya', 1.00),
('poli', 'sbyrne202', 1.00),
('hist', 'vball77', 1.00),
('eng', 'wyu112', 0.50)
;


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

  +--------------------+              +---------------------+
  | COURSE             |---\          | SEMESTER            |
  +--------------------+   |          +---------------------+
                         . |   /------| year            PK1 |
    each course may be  .  |   |      | season          PK2 |
    offered many times     |   |      | begin_date       NN |
    in a given quarter     |   | .    | end_date         NN |
                           |   |  .   +---------------------+
  +--------------------+   |   |
  | CLASS              |   |   |   each section of a class
  +--------------------+   |   |   is tracked by semester
  | department PK1,FK1 |\__/   |
  | course     PK2,FK2 |/      |    classes are taught by
  | year       PK3,FK1 |\______/    an instructor
  | season     PK4,FK2 |/          .
  | section        PK5 |          .   +---------------------+
  | instructor     FK  |>-------------| INSTRUCTOR          |
  | class_seq   NN,UK  |              +---------------------+
  +--------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TYPE cd.season_t AS ENUM ('fall', 'spring', 'summer');

CREATE DOMAIN cd.year_t AS DECIMAL(4,0);

CREATE TABLE cd.semester (
    year        cd.year_t NOT NULL,
    season      cd.season_t NOT NULL,
    begin_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season)
);

CREATE SEQUENCE class_seq START 20001;

CREATE TABLE cd.class (
    department  VARCHAR(16) NOT NULL,
    course      INTEGER NOT NULL,
    year        cd.year_t NOT NULL,
    season      cd.season_t NOT NULL,
    section     CHAR(3) NOT NULL,
    instructor  VARCHAR(16),
    class_seq   INTEGER NOT NULL DEFAULT nextval('class_seq'),
    CONSTRAINT class_pk
      PRIMARY KEY (department, course, year, season, section),
    CONSTRAINT class_uk
      UNIQUE (class_seq),
    CONSTRAINT class_department_fk
      FOREIGN KEY (department)
      REFERENCES ad.department(code),
    CONSTRAINT class_course_fk
      FOREIGN KEY (department, course)
      REFERENCES ad.course(department, number),
    CONSTRAINT class_semester_fk
      FOREIGN KEY (year, season)
      REFERENCES cd.semester(year, season),
    CONSTRAINT class_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES id.instructor(code)
);

INSERT INTO cd.semester (year, season, begin_date, end_date) VALUES
(2009, 'spring', '2010-01-01', '2010-05-15'),
(2010, 'spring', '2011-01-01', '2011-05-15'),
(2011, 'spring', '2012-01-01', '2012-05-15'),
(2012, 'spring', '2013-01-01', '2013-05-15'),
(2009, 'fall', '2009-08-15', '2009-12-31'),
(2010, 'fall', '2010-08-15', '2010-12-31'),
(2011, 'fall', '2011-08-15', '2011-12-31'),
(2012, 'fall', '2012-08-15', '2012-12-31'),
(2009, 'summer', '2010-06-01', '2010-08-01'),
(2010, 'summer', '2011-06-01', '2011-08-01'),
(2011, 'summer', '2012-06-01', '2012-08-01'),
(2012, 'summer', '2013-06-01', '2013-08-01')
;

INSERT INTO cd.class (department, course, year, season, section, instructor, class_seq) VALUES
('astro', 137, 2009, 'fall', '001', 'egasner', 10001),
('astro', 142, 2009, 'spring', '001', 'asacco', 10002),
('astro', 155, 2010, 'fall', '001', 'cfergus12', 10003),
('astro', 254, 2009, 'summer', '002', 'cfergus12', 10004),
('chem', 100, 2010, 'fall', '002', 'bsacks66', 10005),
('chem', 110, 2010, 'spring', '001', 'evargas112', 10006),
('chem', 115, 2012, 'summer', '003', 'hbenmahem', 10007),
('chem', 314, 2011, 'fall', '001', 'evargas112', 10008),
('chem', 655, 2011, 'fall', '001', 'bsacks66', 10009),
('phys', 211, 2011, 'spring', '001', 'afrenski', 10010),
('phys', 303, 2012, 'fall', '001', 'bburling', 10011),
('phys', 388, 2011, 'summer', '002', 'mscott51', 10012),
('phys', 512, 2009, 'fall', '002', 'afrenski', 10013),
('phys', 713, 2009, 'spring', '001', 'bburling', 10014),
('phys', 819, 2010, 'fall', '003', 'bsacks66', 10015),
('eng', 101, 2009, 'summer', '001', 'mcardana', 10016),
('eng', 247, 2010, 'fall', '001', 'wyu112', 10017),
('eng', 311, 2010, 'spring', '001', 'wyu112', 10018),
('eng', 175, 2010, 'summer', '001', 'wyu112', 10019),
('eng', 276, 2012, 'fall', '002', 'mcardana', 10020),
('eng', 412, 2011, 'fall', '002', 'mcardana', 10021),
('hist', 112, 2011, 'spring', '001', 'dsims51', 10022),
('hist', 212, 2011, 'spring', '003', 'vball77', 10023),
('hist', 415, 2012, 'fall', '001', 'dsims51', 10024),
('hist', 505, 2009, 'fall', '001', 'vball77', 10025),
('hist', 333, 2009, 'spring', '001', 'dsims51', 10026),
('lang', 201, 2010, 'fall', '001', 'amiller213', 10027),
('lang', 203, 2012, 'spring', '002', 'amiller213', 10028),
('lang', 304, 2010, 'fall', '002', 'jconnell51', 10029),
('lang', 207, 2010, 'spring', '001', 'jconnell51', 10030),
('lang', 305, 2010, 'summer', '003', 'jconnell51', 10031),
('psych', 102, 2011, 'fall', '001', 'kmurray44', 10032),
('psych', 304, 2011, 'fall', '001', 'srandrews', 10033),
('psych', 450, 2011, 'spring', '001', 'srandrews', 10034),
('psych', 560, 2012, 'fall', '001', 'kmurray44', 10035),
('psych', 610, 2011, 'summer', '002', 'kmurray44', 10036),
('poli', 113, 2009, 'fall', '002', 'lbrooks61', 10037),
('poli', 347, 2012, 'spring', '001', 'lbrooks61', 10038),
('poli', 402, 2010, 'fall', '003', 'sbyrne202', 10039),
('poli', 644, 2009, 'summer', '001', 'sbyrne202', 10040),
('poli', 431, 2010, 'fall', '001', 'sbyrne202', 10041),
('comp', 102, 2010, 'spring', '001', 'anabib', 10042),
('comp', 230, 2010, 'summer', '001', 'anabib', 10043),
('comp', 350, 2011, 'fall', '002', 'sbadhreya', 10044),
('comp', 615, 2011, 'fall', '002', 'sbadhreya', 10045),
('comp', 710, 2011, 'spring', '001', 'sbadhreya', 10046),
('comp', 810, 2011, 'spring', '003', 'anabib', 10047),
('comp', 819, 2011, 'summer', '001', 'anabib', 10048),
('ee', 107, 2009, 'fall', '001', 'alang42', 10049),
('ee', 202, 2012, 'spring', '001', 'alang42', 10050),
('ee', 412, 2010, 'fall', '001', 'lmcooper11', 10051),
('ee', 505, 2009, 'summer', '002', 'alang42', 10052),
('ee', 615, 2010, 'fall', '002', 'lmcooper11', 10053),
('me', 111, 2010, 'spring', '001', 'kcavallaro', 10054),
('me', 344, 2010, 'summer', '003', 'kcavallaro', 10055),
('me', 501, 2012, 'fall', '001', 'mbyer55', 10056),
('me', 627, 2011, 'fall', '001', 'mbyer55', 10057),
('me', 712, 2011, 'spring', '001', 'mbyer55', 10058),
('be', 112, 2011, 'spring', '001', 'hbarone', 10059),
('be', 308, 2011, 'summer', '002', 'hbarone', 10060),
('be', 415, 2010, 'spring', '002', 'pblum21', 10061),
('be', 509, 2012, 'summer', '001', 'pblum21', 10062),
('arthis', 202, 2011, 'fall', '003', 'emurphy55', 10063),
('arthis', 712, 2011, 'fall', '001', 'icampbell12', 10064),
('arthis', 340, 2011, 'spring', '001', 'icampbell12', 10065),
('arthis', 710, 2011, 'spring', '001', 'emurphy55', 10066),
('arthis', 809, 2011, 'summer', '001', 'emurphy55', 10067),
('arthis', 623, 2009, 'fall', '002', 'icampbell12', 10068),
('stdart', 411, 2009, 'spring', '002', 'acaspar', 10069),
('stdart', 512, 2010, 'fall', '001', 'egasner', 10070),
('stdart', 614, 2009, 'summer', '003', 'egasner', 10071),
('stdart', 509, 2012, 'summer', '001', 'acaspar', 10072),
('stdart', 333, 2010, 'spring', '001', 'acaspar', 10073),
('tched', 122, 2010, 'summer', '001', 'dbundt31', 10074),
('tched', 155, 2011, 'fall', '001', 'elhill4', 10075),
('tched', 367, 2011, 'fall', '002', 'elhill4', 10076),
('tched', 501, 2011, 'spring', '002', 'dbundt31', 10077),
('tched', 609, 2011, 'spring', '001', 'dbundt31', 10078),
('edpol', 202, 2012, 'fall', '003', 'kmarkman', 10079),
('edpol', 313, 2009, 'fall', '001', 'kmarkman', 10080),
('edpol', 505, 2009, 'spring', '001', 'rrosenfeld31', 10081),
('edpol', 617, 2010, 'fall', '001', 'rrosenfeld31', 10082),
('corpfi', 234, 2009, 'summer', '001', 'astone77', 10083),
('corpfi', 404, 2010, 'fall', '002', 'astone77', 10084),
('corpfi', 601, 2012, 'summer', '002', 'astone77', 10085),
('acc', 200, 2010, 'summer', '001', 'dfallon23', 10086),
('acc', 315, 2011, 'fall', '003', 'dfallon23', 10087),
('acc', 426, 2011, 'fall', '001', 'dfallon23', 10088),
('acc', 527, 2011, 'spring', '001', 'dfallon23', 10089),
('acc', 606, 2011, 'spring', '001', 'dfallon23', 10090),
('capmrk', 712, 2011, 'summer', '001', 'jflug29', 10091),
('capmrk', 808, 2012, 'summer', '002', 'jflug29', 10092),
('capmrk', 818, 2010, 'spring', '002', 'jflug29', 10093),
('capmrk', 756, 2011, 'spring', '001', 'jflug29', 10094)
;


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
  +--------------------+   |     /----| number           PK |    |
  | class       PK, FK |>--/     |    | name             NN |    |
  | student     PK, FK |---------/    | gender           NN |    |
  | status             |     .        | dob              NN |    |
  | grade              |    .         | school          FK1 |\___/
  +--------------------+   .          | program         FK2 |/
                                      | start_date       NN |
    students may enroll in            | is_active        NN |
    one or more classes               +---------------------+

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TYPE ed.enrollment_status_t AS ENUM ('enr', 'inc', 'ngr');
CREATE TYPE ed.student_gender_t AS ENUM ('f', 'i', 'm');

CREATE TABLE ed.student (
    number      INTEGER NOT NULL,
    name        VARCHAR(64) NOT NULL,
    gender      ed.student_gender_t NOT NULL,
    dob         DATE NOT NULL,
    school      VARCHAR(16),
    program     VARCHAR(16),
    start_date  DATE NOT NULL,
    is_active   BOOLEAN NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (number),
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
    status      ed.enrollment_status_t NOT NULL,
    grade       DECIMAL(3,2),
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student, class),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student)
      REFERENCES ed.student(number),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class)
      REFERENCES cd.class(class_seq)
);

INSERT INTO ed.student (number, name, gender, dob, school, program, start_date, is_active) VALUES
('25371', 'John L. Hanley', 'm', '1990-04-28', 'eng', 'gbuseng', '2009-07-15', TRUE),
('29878', 'Ellen Lansburgh', 'f', '1992-02-01', 'bus', 'uacct', '2008-01-05', TRUE),
('37278', 'Ming Wang', 'm', '1988-03-15', 'la', 'gengl', '2002-11-27', FALSE),
('92039', 'Syed Ishaq', 'm', '1992-10-23', 'art', 'gart', '2010-09-02', TRUE),
('37283', 'Janine Sylvia', 'f', '1993-12-02', 'ns', 'uastro', '2009-08-14', TRUE),
('17385', 'Valeria Rinaldi', 'f', '1985-09-02', 'bus', 'pcap', '2004-09-01', FALSE),
('28371', 'Ken Tanaka', 'm', '1992-11-03', 'art', 'gart', '2010-09-08', TRUE),
('21837', 'Jalene Flambeau', 'f', '1989-03-23', 'art', 'gart', '2010-06-11', TRUE),
('38187', 'Mary Ann Flenderson', 'f', '1993-05-16', 'ns', 'uphys', '2010-08-26', TRUE),
('43278', 'Trina Wood Campbell', 'f', '1990-02-12', 'eng', 'gme', '2007-09-01', TRUE),
('27138', 'Artem Karpov', 'm', '1991-10-16', 'eng', 'gbe', '2009-08-22', TRUE),
('12837', 'Christine Leung', 'f', '1991-06-06', 'eng', 'gme', '2009-08-17', TRUE),
('38721', 'Alicia Montez-Galliano', 'f', '1994-07-11', 'ns', 'uchem', '2010-09-10', TRUE),
('37182', 'Amy Yang', 'f', '1992-12-17', 'ns', 'uphys', '2002-08-10', FALSE),
('32718', 'Raisa Antonova', 'f', '1992-12-09', 'eng', 'gbe', '2008-09-15', FALSE),
('32711', 'Peter Zajac Jr.', 'm', '1994-01-23', 'bus', 'ucorpfi', '2009-09-10', TRUE),
('33278', 'Andrea Kaminski', 'f', '1981-04-20', 'bus', 'pcap', '2009-01-15', TRUE),
('17283', 'Lucy Ryong', 'f', '1988-01-25', 'edu', 'gedu', '2009-01-27', TRUE),
('12738', 'Helmut Dietmark', 'm', '1989-11-27', 'edu', 'psci', '2008-03-17', TRUE),
('23817', 'Benjamin Wen', 'm', '1993-12-16', 'la', 'uhist', '2009-01-12', TRUE),
('57382', 'Paul Duncan Ulam', 'm', '2001-05-05', 'la', 'uspan', '2009-05-21', TRUE),
('13723', 'Narissa Maya', 'f', '1992-04-30', 'la', 'upsych', '2007-11-21', FALSE),
('31332', 'Dara Subramanya', 'f', '1994-11-16', 'la', 'upsych', '2008-09-10', TRUE),
('35572', 'Corinna Ellis', 'f', '1995-07-22', 'edu', 'glited', '2007-05-14', TRUE),
('12328', 'Karen Yuen', 'f', '1991-09-10', 'ns', 'uphys', '2007-05-16', TRUE),
('32214', 'Joseph Tan', 'm', '1992-08-01', 'eng', 'gbuseng', '2008-01-06', TRUE),
('22313', 'James Earl Sims III', 'm', '2002-07-06', 'eng', 'umech', '2004-08-16', TRUE),
('24431', 'Annette Dupree', 'f', '1987-01-28', 'eng', 'umech', '2006-01-16', TRUE),
('38794', 'Bailey Melvin', 'm', '1988-03-13', 'la', 'psciwri', '2005-04-20', TRUE),
('37855', 'Amina N. Elsaeed', 'f', '1987-10-29', 'la', 'uhist', '2005-09-02', TRUE),
('35523', 'Nikki Agbo', 'm', '1985-05-05', 'la', 'gengl', '2006-02-25', TRUE),
('20927', 'Glenn L. McNair', 'm', '1987-12-13', 'eng', 'gee', '2009-08-23', TRUE),
('35183', 'Teisha Worth Day', 'f', '1983-12-31', 'edu', 'gedlead', '2009-08-21', TRUE),
('25723', 'Kumar Suresh', 'm', '1994-09-11', 'eng', 'ucompsci', '2009-08-23', TRUE),
('24672', 'Mahesh Basa', 'm', '1995-08-21', 'eng', 'ucompsci', '2008-04-15', FALSE),
('23137', 'Rachel Feld', 'f', '1992-09-27', 'ns', 'uchem', '2008-12-23', TRUE),
('35163', 'Nicola Ralls Jr.', 'f', '1993-06-02', 'bus', 'uacct', '2010-01-12', TRUE),
('21135', 'Luis Riviera Espinoza', 'm', '1993-05-21', 'eng', 'gbe', '2010-02-19', TRUE),
('31735', 'Demetrios Kanakis', 'm', '1995-04-17', 'eng', 'ucompsci', '2009-05-21', TRUE),
('21166', 'Laura Elmer Long', 'f', '1991-02-14', 'ns', 'uastro', '2009-01-31', TRUE),
('31331', 'Khadija Hamad Azzan', 'f', '1992-11-26', 'ns', 'uastro', '2008-09-21', FALSE),
('36446', 'Milton Mahanga', 'm', '1991-11-06', 'art', 'gart', '2009-05-05', TRUE),
('26764', 'Bernard Careval', 'm', '1992-08-23', 'art', 'gart', '2008-07-30', TRUE),
('26743', 'Ulf Knudsen', 'm', '1990-11-14', 'ns', 'uphys', '2008-04-27', TRUE),
('31835', 'Paavo Kekkonen', 'm', '2000-09-08', 'ns', 'uphys', '2008-06-11', TRUE),
('29301', 'Eduardo Serrano', 'm', '1991-09-09', 'art', 'uhist', '2006-01-14', TRUE),
('21263', 'Ari Ben David', 'm', '1989-03-15', 'la', 'gengl', '2006-12-15', TRUE),
('37744', 'Scott Blank', 'm', '1988-06-12', 'bus', 'ucorpfi', '2007-12-15', TRUE),
('28382', 'Martha O''Mally', 'f', '1995-05-14', 'bus', 'pacc', '2005-01-01', TRUE),
('27281', 'José N. Marteñes', 'm', '1993-11-19', 'eng', 'ucompsci', '2007-06-15', TRUE),
('27817', 'Niall Crawford', 'm', '1998-12-14', 'bus', 'pacc', '2010-01-02', TRUE)
;

INSERT INTO ed.enrollment (student, class, status, grade) VALUES
('25371', 10086, 'ngr', NULL),
('25371', 10051, 'enr', 3.7),
('29878', 10086, 'inc', NULL),
('37278', 10018, 'enr', 2.6),
('92039', 10071, 'enr', 3.1)
;


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

  +-----------------------+
  | PREREQUISITE          |            two foreign keys denote a
  +-----------------------+          . dependency *by* a course,
  | by_department FK1,PK1 |\_______ .  such as chem.100, *on*
  | by_course     FK2,PK2 |/       \   another, such as mth.101
  | on_department FK1,PK3 |\______ |
  | on_course     FK2,PK4 |/     | |  +---------------------+
  +-----------------------+   /- \-\--| COURSE              |
                              |       +---------------------+
   a course can be a member   |
   of several classifications |       +---------------------+
                           .  |       | CLASSIFICATION      |
  +-----------------------+ . |       +---------------------+
  | COURSE_CLASSIFICATION |   | /- /--| code             PK |----\
  +-----------------------+   | |  |  | type             NN |    |
  | department    FK1,PK1 |\__/ |  |  | title            NN |    |
  | course        FK2,PK2 |/    |  |  | description         |    |
  | classification FK,PK3 |>----/  |  | part_of          FK |>---/
  +-----------------------+  .     |  +---------------------+  .
                            .      |                          .
    a classification is used       |    a classification may be
    to tag multiple courses   /----/    part of a broader category
                              |
  +-----------------------+   | . courses, by classification, are
  | PROGRAM_REQUIREMENT   |   |.  required by a given program
  +-----------------------+   |
  | school        FK1,PK1 |\__|____     +---------------------+
  | program       FK2,PK2 |/  |    `----| PROGRAM             |
  | classification FK,PK3 |>--/   .     +---------------------+
  | credit_hours       NN |      .
  | rationale             |    programs require class credits
  +-----------------------+    specified via classifications

  PK - Primary Key   UK - Unique Key         FK - Foreign Key
  NN - Not Null      CK - Check Constraint
$_$;

CREATE TABLE rd.prerequisite (
    of_department   VARCHAR(16) NOT NULL,
    of_course       INTEGER NOT NULL,
    on_department   VARCHAR(16) NOT NULL,
    on_course       INTEGER NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department, of_course, on_department, on_course),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department, on_course)
      REFERENCES ad.course(department, number),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department, of_course)
      REFERENCES ad.course(department, number)
);

CREATE TYPE rd.classification_type_t
  AS ENUM ('department', 'school', 'university');

CREATE TABLE rd.classification (
    code        VARCHAR(16) NOT NULL,
    type        rd.classification_type_t,
    title       VARCHAR(64) NOT NULL,
    description TEXT,
    part_of     VARCHAR(16),
    CONSTRAINT classification_pk
      PRIMARY KEY (code),
    CONSTRAINT classification_title_uk
      UNIQUE (title),
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
      REFERENCES ad.course(department, number),
    CONSTRAINT course_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES rd.classification(code)
);

CREATE TABLE rd.program_requirement (
    school          VARCHAR(16) NOT NULL,
    program         VARCHAR(16) NOT NULL,
    classification  VARCHAR(16) NOT NULL,
    credit_hours    INTEGER NOT NULL,
    rationale       TEXT,
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school, program, classification),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school, program)
      REFERENCES ad.program(school, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES rd.classification(code)
);

INSERT INTO rd.prerequisite (of_department, of_course, on_department, on_course) VALUES
('astro', 142, 'astro', 137),
('chem', 314, 'chem', 115),
('chem', 110, 'chem', 100),
('phys', 303, 'phys', 211),
('phys', 713, 'phys', 512),
('eng', 412, 'eng', 276),
('hist', 212, 'hist', 112),
('lang', 203, 'lang', 201),
('lang', 305, 'lang', 207),
('poli', 402, 'poli', 113),
('comp', 710, 'comp', 102),
('ee', 412, 'ee', 107),
('me', 344, 'me', 111),
('be', 415, 'be', 112),
('arthis', 710, 'arthis', 202),
('stdart', 614, 'stdart', 333),
('tched', 609, 'tched', 122),
('edpol', 313, 'edpol', 202),
('corpfi', 601, 'corpfi', 404),
('acc', 527, 'acc', 200),
('capmrk', 818, 'acc', 315)
;

INSERT INTO rd.classification (code, type, title, description, part_of) VALUES
('cross', NULL, 'Cross-Cutting Requirements', NULL, NULL),
  ('writing', 'university', 'Writing Intensive', 'Writing intensive courses involve 3 or more papers per semester; at least one of which is a research paper of 20 pages or more.', 'cross'),
  ('reasoning', 'university', 'Quantitative Reasoning', 'Quantitative resoning courses focus on numerical analysis to evaluate, describe and justify outcomes of complex decisions.', 'cross'),
  ('diversity', 'university', 'Region and Ethnic Diversity', 'Courses which provide a rich exposure to foreign cultures and regions qualify for this classification.', 'cross'),
  ('research', 'university', 'Research Experience', 'Research courses focus on the scientific method to create hypothesis and test them in a structured laboratory environment.', 'cross'),
('humanities', 'university', 'Arts, Letters, and the Humanities', NULL, NULL),
  ('arthistory', 'school', 'Art', NULL, 'humanities'),
    ('ancient', 'department', 'Ancient Art', NULL, 'arthistory'),
      ('classical', 'department', 'Classical Art', NULL, 'ancient'),
      ('eastern', 'department', 'Near Eastern Art', NULL, 'ancient'),
    ('modern', 'department', 'Modern Art', NULL, 'arthistory'),
    ('artbus', 'department', 'Business of Art', NULL, 'arthistory'),
  ('literature', 'school', 'English and World Literature', NULL, 'humanities'),
    ('englit', 'department', 'EnglishLanguage Literature', NULL, 'humanities'),
    ('eurolit', 'department', 'European Literature in Translation', NULL, 'humanities'),
    ('nonfiction', 'department', 'NonFiction Writing', NULL, 'literature'),
    ('journalism', 'department', 'Journalistic Writing', NULL, 'literature'),
    ('fiction', 'department', 'Fiction Writing', NULL, 'literature'),
    ('poetry', 'department', 'Poetry Writing', NULL, 'literature'),
  ('history', 'school', 'American and World History', NULL, 'humanities'),
    ('amhistory', 'department', 'American History', NULL, 'humanities'),
    ('eurohistory', 'department', 'European History', NULL, 'humanities'),
    ('nonwesternhist', 'department', 'NonWestern History', NULL, 'humanities'),
    ('dischistory', 'department', 'Interdisciplinary History', NULL, 'humanities'),
  ('language', 'school', 'World Languages', NULL, 'humanities'),
    ('modlanguage', 'department', 'Modern Languages', NULL, 'language'),
      ('french', 'department', 'French', NULL, 'modlanguage'),
      ('german', 'department', 'German', NULL, 'modlanguage'),
      ('spanish', 'department', 'Spanish', NULL, 'modlanguage'),
    ('anclanguage', 'department', 'Ancient Languages', NULL, 'language'),
    ('linguistics', 'department', 'Linguistics', NULL, 'language'),
  ('polisci', 'school', 'Political Science', NULL, 'humanities'),
    ('government', 'department', 'Government', NULL, 'polisci'),
    ('intrelations', 'department', 'International Relations', NULL, 'polisci'),
    ('poliecon', 'department', 'Political Economy', NULL, 'polisci'),
  ('psychology', 'school', 'Psychology', NULL, 'humanities'),
    ('cogpsych', 'department', 'Cognitive Science', NULL, 'psychology'),
    ('behpsych', 'department', 'Behavioral Science', NULL, 'psychology'),
    ('chipsych', 'department', 'Child Psychology and Development', NULL, 'psychology'),
('science', 'university', 'Natural Sciences', NULL, NULL),
  ('astronomy', 'school', 'Astronomy', NULL, 'science'),
    ('astrotheory', 'department', 'Astrophysics Theory', NULL, 'astronomy'),
    ('astrolab', 'department', 'Astronomy Laboratory', NULL, 'astronomy'),
    ('observation', 'department', 'Observing Skills', NULL, 'astronomy'),
  ('chemistry', 'school', 'Chemistry', NULL, 'science'),
    ('chemtheory', 'department', 'Theoretical Chemistry', NULL, 'chemistry'),
    ('chemlab', 'department', 'Chemistry Laboratory', NULL, 'chemistry'),
    ('chemcomputation', 'department', 'Algorithms and Data Visualization for Chemists', NULL, 'chemistry'),
  ('physics', 'school', 'Physics', NULL, 'science'),
    ('phystheory', 'department', 'Theoretical Physics', NULL, 'physics'),
    ('physlab', 'department', 'Practical Physics', NULL, 'physics'),
    ('physcomputer', 'department', 'Computer Languages for Physics', NULL, 'physics'),
  ('math', 'school', 'Mathematics', NULL, 'science'),
    ('analysis', 'department', 'Real and Complex Analysis', NULL, 'math'),
    ('algebra', 'department', 'Abstract Algebra', NULL, 'math'),
    ('statistics', 'department', 'Probability and Statistics', NULL, 'math'),
('artdesign', 'university', 'Art and Design', NULL, NULL),
  ('studio', 'school', 'Studio Arts', NULL, 'artdesign'),
  ('artstudio', 'school', 'Studio Art', NULL, 'artdesign'),
    ('drawing', 'department', 'Drawing', NULL, 'artstudio'),
    ('sculpture', 'department', 'Sculpture', NULL, 'artstudio'),
    ('printmaking', 'department', 'Printmaking', NULL, 'artstudio'),
  ('industrial', 'school', 'Industrial Design', NULL, 'artdesign'),
  ('digital', 'school', 'Digital Media', NULL, 'artdesign'),
  ('society', 'school', 'Art and Society', NULL, 'artdesign'),
('engineering', 'university', 'Engineering', NULL, NULL),
  ('compsci', 'school', 'Computer Science', NULL, 'engineering'),
    ('comptheory', 'department', 'Computationial Science', NULL, 'compsci'),
    ('softeng', 'department', 'Software Engineering', NULL, 'compsci'),
    ('compai', 'department', 'Artificial Intelligence', NULL, 'compsci'),
  ('ee', 'school', 'Electrical Engineering', NULL, 'engineering'),
    ('eetheory', 'department', 'Electrical Engineering Theory', NULL, 'ee'),
    ('eeconcentration', 'department', 'EE Concentrations', NULL, 'ee'),
      ('signal', 'department', 'Signal Processing', NULL, 'eeconcentration'),
      ('power', 'department', 'Power Electronics', NULL, 'eeconcentration'),
      ('eecom', 'department', 'Communications', NULL, 'eeconcentration'),
      ('eenetworking', 'department', 'Electrical Networking', NULL, 'eeconcentration'),
  ('me', 'school', 'Mechanical Engineering', NULL, 'engineering'),
    ('memechanics', 'department', 'Mechanics', NULL, 'me'),
    ('medesign', 'department', 'Design and Manufacturing', NULL, 'me'),
    ('mesystems', 'department', 'Systems and Controls', NULL, 'me'),
  ('be', 'school', 'Biomedical Engineering', NULL, 'engineering'),
    ('begeneral', 'department', 'General Biomedical Engineering', NULL, 'be'),
    ('beclinical', 'department', 'Clinical Engineering', NULL, 'be'),
    ('nanotech', 'department', 'Nanotechnology', NULL, 'be'),
    ('biomaterials', 'department', 'Biomaterials', NULL, 'be'),
('education', 'university', 'Education', NULL, NULL),
  ('teached', 'school', 'Teacher Education', NULL, 'education'),
    ('methods', 'department', 'Teaching Methods', NULL, 'teached'),
    ('edmanagement', 'department', 'Education Management', NULL, 'teached'),
    ('earlyed', 'department', 'Early Education', NULL, 'teached'),
  ('edpol', 'school', 'Educational Policy', NULL, 'education'),
    ('edstudies', 'department', 'Policy Studies', NULL, 'edpol'),
    ('edresearch', 'department', 'Educational Research', NULL, 'edpol'),
('business', 'university', 'Business', NULL, NULL),
  ('ethics', 'school', 'Business Ethics', NULL, 'business'),
  ('financial', 'school', 'Financial Analysis', NULL, 'financial'),
    ('accounting', 'department', 'Accounting', NULL, 'financial'),
    ('investment', 'department', 'Investment', NULL, 'financial'),
      ('personal', 'department', 'Personal Investment', NULL, 'investment'),
      ('institutional', 'department', 'Institutional Investment', NULL, 'investment'),
    ('markets', 'school', 'Capital Markets', NULL, 'financial'),
  ('management', 'school', 'Management', NULL, 'business'),
('remedial', 'university', 'Remedial Courses', 'Classes for which credit is not typically given for degree programs in the same school; e.g.  College Algebra courses do not earn credit for those in the School of Natural Science.', NULL)
;

INSERT INTO rd.course_classification (department, course, classification) VALUES
('astro', 137, 'astronomy'),
('astro', 142, 'astrolab'),
('astro', 155, 'observation'),
('astro', 254, 'astrotheory'),
('mth', 101, 'remedial'),
('chem', 100, 'remedial'),
('chem', 110, 'science'),
('chem', 115, 'chemlab'),
('chem', 655, 'chemtheory'),
('phys', 211, 'science'),
('phys', 303, 'phystheory'),
('phys', 388, 'physlab'),
('phys', 388, 'reasoning'),
('phys', 512, 'phystheory'),
('phys', 713, 'phystheory'),
('phys', 819, 'phystheory'),
('eng', 101, 'remedial'),
('eng', 247, 'eurolit'),
('eng', 311, 'nonfiction'),
('eng', 175, 'journalism'),
('eng', 175, 'writing'),
('eng', 276, 'nonfiction'),
('eng', 276, 'writing'),
('eng', 412, 'nonfiction'),
('eng', 412, 'writing'),
('hist', 112, 'amhistory'),
('hist', 212, 'amhistory'),
('hist', 415, 'dischistory'),
('hist', 415, 'diversity'),
('hist', 415, 'earlyed'),
('hist', 333, 'dischistory'),
('lang', 201, 'modlanguage'),
('lang', 203, 'modlanguage'),
('lang', 207, 'linguistics'),
('lang', 207, 'earlyed'),
('lang', 305, 'linguistics'),
('psych', 102, 'remedial'),
('psych', 304, 'cogpsych'),
('psych', 304, 'reasoning'),
('psych', 450, 'behpsych'),
('psych', 560, 'cogpsych'),
('psych', 560, 'compai'),
('psych', 610, 'chipsych'),
('psych', 610, 'earlyed'),
('psych', 610, 'research'),
('poli', 113, 'government'),
('poli', 402, 'government'),
('poli', 644, 'intrelations'),
('poli', 431, 'government'),
('poli', 715, 'intrelations'),
('comp', 102, 'remedial'),
('comp', 230, 'comptheory'),
('comp', 350, 'comptheory'),
('comp', 615, 'compai'),
('comp', 819, 'comptheory'),
('comp', 710, 'softeng'),
('comp', 810, 'softeng'),
('ee', 107, 'eetheory'),
('ee', 202, 'eetheory'),
('ee', 412, 'eetheory'),
('ee', 505, 'eetheory'),
('ee', 615, 'eecom'),
('me', 344, 'memechanics'),
('me', 111, 'memechanics'),
('me', 627, 'memechanics'),
('me', 501, 'mesystems'),
('me', 712, 'medesign'),
('me', 712, 'industrial'),
('be', 112, 'begeneral'),
('be', 308, 'begeneral'),
('be', 415, 'beclinical'),
('be', 509, 'beclinical'),
('arthis', 712, 'artbus'),
('arthis', 202, 'modern'),
('arthis', 712, 'management'),
('arthis', 340, 'eastern'),
('arthis', 710, 'modern'),
('arthis', 809, 'modern'),
('arthis', 623, 'modern'),
('arthis', 623, 'diversity'),
('stdart', 714, 'drawing'),
('stdart', 509, 'printmaking'),
('stdart', 411, 'sculpture'),
('stdart', 411, 'physlab'),
('stdart', 512, 'society'),
('stdart', 614, 'drawing'),
('stdart', 333, 'drawing'),
('stdart', 119, 'sculpture'),
('tched', 155, 'methods'),
('tched', 367, 'edmanagement'),
('tched', 367, 'management'),
('tched', 501, 'earlyed'),
('tched', 609, 'earlyed'),
('tched', 122, 'earlyed'),
('edpol', 202, 'edresearch'),
('edpol', 551, 'edresearch'),
('edpol', 313, 'edresearch'),
('edpol', 313, 'amhistory'),
('edpol', 617, 'edstudies'),
('edpol', 505, 'edresearch'),
('corpfi', 234, 'financial'),
('corpfi', 404, 'financial'),
('corpfi', 601, 'financial'),
('acc', 100, 'remedial'),
('acc', 200, 'accounting'),
('acc', 315, 'accounting'),
('acc', 426, 'accounting'),
('acc', 527, 'accounting'),
('acc', 606, 'accounting'),
('capmrk', 712, 'markets'),
('capmrk', 808, 'institutional'),
('capmrk', 818, 'institutional'),
('capmrk', 756, 'markets')
;

INSERT INTO rd.program_requirement (school, program, classification, credit_hours, rationale) VALUES
('ns', 'uastro', 'astrolab', 8, 'Astronomy students are expected to take a minimum of 8 credit hours in the astronomy laboratory.'),
('ns', 'uastro', 'observation', 12, 'Undergraduate astronomy students will take a minimum of 12 credit hours of observational astronomy.'),
('ns', 'uastro', 'astrotheory', 24, 'Undergraduate astronomy students will take a minimum of 12 credit hours of coursework on astronomy theory.'),
('ns', 'uastro', 'reasoning', 12, 'Undergraduate science students will take a minimum of 12 credit hours in general reasoning.'),
('ns', 'uastro', 'research', 12, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in research techniques.'),
('ns', 'uastro', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 16 credit hours in the humanities.'),
('ns', 'uastro', 'physics', 9, 'Undergraduate astronomy students will take a minimum of 9 credit hours in physics.'),
('ns', 'uchem', 'chemlab', 16, 'Undergraduate chemistry students must satisfy a minimum requirement for chemistry labwork.'),
('ns', 'uchem', 'chemtheory', 18, 'Undergraduate chemistry students must satisfy a minimum requirement for chemistry theory.'),
('ns', 'uchem', 'reasoning', 12, 'Undergraduate science students will take a minimum of 12 credit hours in general reasoning.'),
('ns', 'uchem', 'research', 12, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in research techniques.'),
('ns', 'uchem', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 16 credit hours in the humanities.'),
('ns', 'uphys', 'phystheory', 26, 'Candidates for the B.S. in physics must take a minimum of 26 hours of physics theory.'),
('ns', 'uphys', 'physlab', 12, 'Candidates for the B.S. in physics must take a minimum of 12 hours of physics labwork.'),
('ns', 'uphys', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in the humanities.'),
('ns', 'uphys', 'science', 12, 'Physics majors are expected to take a minimum of 12 credit hours in other scientific disciplines.'),
('la', 'upsych', 'psychology', 24, 'Psychology majors must take the minimum credit hours in one or more of the three major psychology concentrationscognitive, behavioral, or child.'),
('la', 'upsych', 'writing', 12, 'Psychology majors must take a minimum of 12 credit hours in writing.'),
('la', 'upsych', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.'),
('la', 'upsych', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.'),
('la', 'upolisci', 'government', 16, 'Political Science majors will take at least 16 credit hours of coursework in world government'),
('la', 'upolisci', 'intrelations', 12, 'Political Science majors will take at least 12 credit hours of coursework in international relations.'),
('la', 'upolisci', 'poliecon', 12, 'Political Science majors will take at least 12 credit hours of coursework in political economy.'),
('la', 'upolisci', 'writing', 12, 'Political science majors must take a minimum of 12 credit hours in writing.'),
('la', 'upolisci', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.'),
('la', 'upolisci', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.'),
('la', 'gscitch', 'science', 18, 'M.S. candidates for the science teaching degree who do not have undergraduate degrees in science must take a minimum of 18 credit hours in one scientific discipline.'),
('la', 'gscitch', 'methods', 18, 'M.S. candidates should complete a minimum of 18 credit hours focused on teaching methods.'),
('la', 'gscitch', 'earlyed', 12, 'M.S. candidates will take a minimum of 12 hours of early childhood education instruction.'),
('la', 'psciwri', 'science', 12, 'Candidates for the Certificate in Science Writing will take four threehour classes in general science.'),
('la', 'psciwri', 'journalism', 12, 'Candidates will take four threehour classes in writing focused on scientific journalism'),
('la', 'gengl', 'literature', 36, 'Candidates for the M.A. in English will take a minimum of 36 credit hours of literature courses.'),
('la', 'uengl', 'literature', 30, 'B.A. candidates in English are expected to take 30 credit hours in general literature.'),
('la', 'uengl', 'eurolit', 12, 'B.A. candidates in English are expected to take three courses in European literature.'),
('la', 'uengl', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.'),
('la', 'uengl', 'humanities', 16, 'B.A. candidates in the humanities will take a minimum of 16 credit hours in general humanities outside their major.'),
('la', 'uengl', 'modlanguage', 16, 'B.A. candidates in the humanities will take a minimum of two years of a modern language of their choice.'),
('la', 'uhist', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.'),
('la', 'uspan', 'spanish', 24, 'Spanish majors will take a minimum of 24 credit hours in the Spanish majors.'),
('la', 'uspan', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.'),
('la', 'uspan', 'humanities', 16, 'B.A. candidates in the humanities will take a minimum of 16 credit hours in general humanities outside their major.'),
('la', 'uspan', 'modlanguage', 16, 'B.A. candidates in the humanities will take a minimum of two years of a modern language of their choice.'),
('la', 'uspan', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.'),
('la', 'glang', 'language', 22, 'Candidates for the Master of Arts in Modern Languages must take a minimum of 22 credit hours in the modern language of their concentration.'),
('eng', 'uelec', 'eetheory', 18, 'Bachelor of Engineering candidates in Electrical Engineering are expected to take at least 18 hours of credit in EE theory.'),
('eng', 'uelec', 'eeconcentration', 22, 'Bachelor of Engineering candidates in Electrical Engineering are expected to take at least 22 hours of credit in their area of concentration.'),
('eng', 'uelec', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.'),
('eng', 'umech', 'me', 22, 'Bachelor of Engineering candidates in Mechanical Engineering are expected to take at least 22 hours of credit in their area of concentrationmechanics, de sign, or systems.'),
('eng', 'umech', 'mesystems', 9, 'Bachelor of Engineering candidates in Mechanical Engineering are expected to take at least 9 hours of credit in systems, regardless of their area of concentration.'),
('eng', 'umech', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.'),
('eng', 'umech', 'humanities', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in general humanities.'),
('eng', 'ubio', 'be', 24, 'Bachelor of Engineering candidates in Bioengineering are expected to take at least 24 hours of credit in their area of concentrationclinical, nanotech, orbiomaterials'),
('eng', 'ubio', 'mesystems', 10, 'Bachelor of Engineering candidates in Bioengineering are expected to take at least 9 hours of credit in biomaterials, regardless of their area of concentration.'),
('eng', 'ubio', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.'),
('eng', 'ubio', 'humanities', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in general humanities.'),
('eng', 'gbuseng', 'business', 16, 'Candidates for the Master of Science in Business and Engineering are required to take at least 16 credit hours in general business.'),
('eng', 'gbuseng', 'engineering', 22, 'Candidates for the Master of Science in Business and Engineering are required to take at least 22 credit hours in one or more relevant engineering disciplines.'),
('eng', 'gee', 'ee', 34, 'Candidates for the Master of Science in Electrical Engineering must take at least 34 credit hours in graduate electrical engineering.'),
('eng', 'gme', 'ee', 36, 'Candidates for the Master of Science in Electrical Engineering must take at least 36 credit hours in graduate electrical engineering.'),
('eng', 'gbe', 'ee', 38, 'Candidates for the Master of Science in Electrical Engineering must take at least 38 credit hours in graduate electrical engineering.'),
('edu', 'umath', 'teached', 20, 'Bachelor of Arts students in Math Education must take at least 20 credit hours of general teacher education.'),
('edu', 'umath', 'math', 16, 'Bachelor of Arts students in Science Education must take at least 16 hours of general math.'),
('edu', 'umath', 'diversity', 8, 'In acknowledgement of the importance of diversity in education, all candidates for education degrees must take at least 8 credit hours in diverse cultures and history.'),
('edu', 'usci', 'teached', 22, 'Bachelor of Arts students in Science Education must take at least 22 credit hours of general science education.'),
('edu', 'usci', 'science', 14, 'Bachelor of Arts students in Science Education must take at least 14 hours of general science.'),
('edu', 'usci', 'diversity', 8, 'In acknowledgement of the importance of diversity in education, all candidates for education degrees must take at least 8 credit hours in diverse cultures and history.'),
('edu', 'psci', 'methods', 12, 'Candidates for the Certificate in Science Teaching are required to take a minimum number of credit hours in teaching methods.'),
('edu', 'psci', 'science', 12, 'Candidates for the Certificate in Science Teaching are required to take a minimum number of credit hours in general science.'),
('edu', 'glited', 'teached', 28, 'Candidates for the Master of Arts in Literacy Education will take the majority of their credit hours in teacher education, focusing on literacy.'),
('edu', 'gedlead', 'edpol', 20, 'Candidates for the Master of Arts in Educational Leadership will concentrate in educational policy.'),
('edu', 'gedlead', 'management', 8, 'Candidates for the Master of Arts in Educational Leadership will take a minimum number of credit hours in management at the School of Business.'),
('edu', 'gedu', 'edresearch', 22, 'Candidates for the Master of Science in Education will focus on a core requirement of educationrelated research leading up to the master''s thesis.'),
('edu', 'gtch', 'teached', 28, 'Candidates for the Master of Arts in Teaching will concentrate on the study of teaching methods in their chosen concentration.'),
('bus', 'uacct', 'accounting', 24, 'Students pursuing the B.S. in Accounting will take the majority of their credit hours in accounting.'),
('bus', 'uacct', 'investment', 8, 'Students pursuing the B.S. in Accounting must take at least 8 credit hours in general investment topics.'),
('bus', 'uacct', 'analysis', 8, 'Students pursuing the B.S. in Accounting must take at least 8 credit hours of relevant mathematics.'),
('bus', 'uacct', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.'),
('bus', 'ucorpfi', 'financial', 18, 'Students pursuing the B.S. in Corporate Finance will take the majority of their credit hours in general financial classes.'),
('bus', 'ucorpfi', 'accounting', 10, 'Students pursuing the B.S. in Corporate Finance must take a minimum of 10 credit hours in accounting.'),
('bus', 'ucorpfi', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.'),
('bus', 'ucorpfi', 'management', 6, 'Students pursuing the B.S. in Corporate Finance must take a minimum number of credits in corporate management.'),
('bus', 'ubusad', 'management', 24, 'B.S. students in Business Administration will focus on corporate management.'),
('bus', 'ubusad', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.'),
('bus', 'pacc', 'accounting', 20, 'Requirements for the Graduate Certificate in Accounting require a minimum number of credit hours in accounting and accounting theory.'),
('bus', 'pcap', 'markets', 20, 'Requirements for the Certificate in Capital Markets include completion of a minimum number of credit hours in markets topics.'),
('art', 'gart', 'arthistory', 20, 'Candidates for the Post Baccalaureate in Art History must complete a minimum number of core art history courses.'),
('art', 'uhist', 'classical', 9, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of classical art.'),
('art', 'uhist', 'modern', 9, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of modern art.'),
('art', 'uhist', 'eastern', 6, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of eastern art.'),
('art', 'uhist', 'arthistory', 14, 'Students in the undergraduate Art History program are required to take 9 credit hours of elective classes in art history.'),
('art', 'ustudio', 'artstudio', 24, 'Students in the undergraduate Studio Art program will concentrate on their selected studio discipline.'),
('art', 'ustudio', 'drawing', 8, 'All Studio Art undergraduate students must take a minimum 8 hours of credit in freehand drawing.'),
('art', 'ustudio', 'society', 6, 'All Studio Art undergraduate students must take a minimum of 6 hours of credit in Art & Society.'),
('art', 'ustudio', 'digital', 6, 'All Studio Art undergraduate students must take a minimum of 6 hours of credit in Digital Art.')
;


