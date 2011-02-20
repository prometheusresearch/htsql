--
-- Copyright (c) 2006-2011, Prometheus Research, LLC
-- Authors: Clark C. Evans <cce@clarkevans.com>,
--          Kirill Simonov <xi@resolvent.net>
--


-- --------------------------------------------------------------------
-- The standard HTSQL regression schema for SQLite
--


-- --------------------------------------------------------------------
-- Administrative Directory
--

CREATE TABLE school (
    code        VARCHAR(16) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    CONSTRAINT school_pk
      PRIMARY KEY (code),
    CONSTRAINT name_uk
      UNIQUE (name)
);

CREATE TABLE department (
    code        VARCHAR(16) NOT NULL,
    name        VARCHAR(64) NOT NULL,
    school      VARCHAR(16),
    CONSTRAINT department_pk
      PRIMARY KEY (code),
    CONSTRAINT department_name_uk
      UNIQUE (name),
    CONSTRAINT department_school_fk
      FOREIGN KEY (school)
      REFERENCES school(code)
);

CREATE TABLE program (
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
      REFERENCES school(code),
    CONSTRAINT program_part_of_fk
      FOREIGN KEY (school, part_of )
      REFERENCES program(school, code)
);

CREATE TABLE course (
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
      REFERENCES department(code)
);

INSERT INTO school (code, name) VALUES
    ('ns', 'School of Natural Sciences');
INSERT INTO school (code, name) VALUES
    ('sc', 'School of Continuing Studies');
INSERT INTO school (code, name) VALUES
    ('la', 'School of Arts and Humanities');
INSERT INTO school (code, name) VALUES
    ('eng', 'School of Engineering');
INSERT INTO school (code, name) VALUES
    ('art', 'School of Art and Design');
INSERT INTO school (code, name) VALUES
    ('edu', 'College of Education');
INSERT INTO school (code, name) VALUES
    ('bus', 'School of Business');
INSERT INTO school (code, name) VALUES
    ('ph', 'Public Honorariums');
INSERT INTO school (code, name) VALUES
    ('mus', 'School of Music & Dance');

INSERT INTO department (code, name, school) VALUES
    ('astro', 'Astronomy', 'ns');
INSERT INTO department (code, name, school) VALUES
    ('chem', 'Chemistry', 'ns');
INSERT INTO department (code, name, school) VALUES
    ('phys', 'Physics', 'ns');
INSERT INTO department (code, name, school) VALUES
    ('mth', 'Mathematics', 'ns');
INSERT INTO department (code, name, school) VALUES
    ('eng', 'English', 'la');
INSERT INTO department (code, name, school) VALUES
    ('lang', 'Foreign Languages', 'la');
INSERT INTO department (code, name, school) VALUES
    ('hist', 'History', 'la');
INSERT INTO department (code, name, school) VALUES
    ('psych', 'Psychology', 'la');
INSERT INTO department (code, name, school) VALUES
    ('poli', 'Political Science', 'la');
INSERT INTO department (code, name, school) VALUES
    ('comp', 'Computer Science', 'eng');
INSERT INTO department (code, name, school) VALUES
    ('ee', 'Electrical Engineering', 'eng');
INSERT INTO department (code, name, school) VALUES
    ('me', 'Mechanical Engineering', 'eng');
INSERT INTO department (code, name, school) VALUES
    ('be', 'Bioengineering', 'eng');
INSERT INTO department (code, name, school) VALUES
    ('arthis', 'Art History', 'art');
INSERT INTO department (code, name, school) VALUES
    ('stdart', 'Studio Art', 'art');
INSERT INTO department (code, name, school) VALUES
    ('tched', 'Teacher Education', 'edu');
INSERT INTO department (code, name, school) VALUES
    ('edpol', 'Educational Policy', 'edu');
INSERT INTO department (code, name, school) VALUES
    ('corpfi', 'Corporate Finance', 'bus');
INSERT INTO department (code, name, school) VALUES
    ('acc', 'Accounting', 'bus');
INSERT INTO department (code, name, school) VALUES
    ('capmrk', 'Capital Markets', 'bus');
INSERT INTO department (code, name, school) VALUES
    ('voc', 'Vocals', 'mus');
INSERT INTO department (code, name, school) VALUES
    ('pia', 'Piano', 'mus');
INSERT INTO department (code, name, school) VALUES
    ('win', 'Wind', 'mus');
INSERT INTO department (code, name, school) VALUES
    ('str', 'Strings', 'mus');
-- Non-Academic Departments
INSERT INTO department (code, name, school) VALUES
    ('bursar', 'Bursar''s Office', NULL);
INSERT INTO department (code, name, school) VALUES
    ('career', 'Career Development', NULL);
INSERT INTO department (code, name, school) VALUES
    ('parent', 'Parents & Alumni', NULL);

INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'uastro', 'Bachelor of Science in Astronomy', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'uchem', 'Bachelor of Science in Chemistry', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'uphys', 'Bachelor of Science in Physics', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'pmth', 'Doctorate of Science in Mathematics', 'ph', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'gmth', 'Masters of Science in Mathematics', 'bs', 'pmth');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ns', 'umth', 'Bachelor of Science in Mathematics', 'bs', 'gmth');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'upsych', 'Bachelor of Arts in Psychology', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'upolisci', 'Bachelor of Arts in Political Science', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'gscitch', 'Master of Arts in Science Teaching', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'psciwri', 'Science Writing', 'ct', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'gengl', 'Master of Arts in English', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'uengl', 'Bachelor of Arts in English', 'ba', 'gengl');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'uhist', 'Bachelor of Arts in History', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'uspan', 'Bachelor of Arts in Spanish', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('la', 'glang', 'Master of Arts in Modern Languages', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'gbuseng', 'M.S. in Business and Engineering', 'ms', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'gee', 'M.S. in Electrical Engineering', 'ms', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'gme', 'M.S. in Mechanical Engineering', 'ms', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'gbe', 'M.S. in Bioengineering', 'ms', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'uelec', 'B.S. in Electrical Engineering', 'bs', 'gee');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'umech', 'B.S. in Mechanical Engineering', 'bs', 'gme');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'ubio', 'B.S. in Bioengineering', 'bs', 'gbe');
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('eng', 'ucompsci', 'B.S. in Computer Science', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'umath', 'Bachelor of Arts in Math Education', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'usci', 'Bachelor of Arts in Science Education', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'psci', 'Certificate in Science Teaching', 'ct', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'glited', 'Master of Arts in Literacy Education', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'gedlead', 'Master of Arts in Education Leadership', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'gedu', 'M.S. in Education', 'ms', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('edu', 'gtch', 'Master of Arts in Teaching', 'ma', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('bus', 'uacct', 'B.S. in Accounting', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('bus', 'ucorpfi', 'B.S. in Corporate Finance', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('bus', 'ubusad', 'B.S. in Business Administration', 'bs', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('bus', 'pacc', 'Graduate Certificate in Accounting', 'ct', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('bus', 'pcap', 'Certificate in Capital Markets', 'ct', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('art', 'gart', 'Post Baccalaureate in Art History', 'pb', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('art', 'uhist', 'Bachelor of Arts in Art History', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('art', 'ustudio', 'Bachelor of Arts in Studio Art', 'ba', NULL);
INSERT INTO program (school, code, title, degree, part_of) VALUES
    ('ph', 'phd', 'Honorary PhD', NULL, NULL);

INSERT INTO course (department, number, title, credits, description) VALUES
    ('astro', 137, 'The Solar System', 3, 'Introductory survey of the solar system, including structure and motion of the planets, properties of the sun, and comparison to extrasolar systems.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('astro', 142, 'Solar System Lab', 2, 'Laboratory studies that complement the lecture course ASTRO 137.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('astro', 155, 'Telescope Workshop', 1, 'Introduction to correct use of the 8-inch Schmidt-Cassegrain type telescope. You will learn about magnification, how to locate an object, and how setting circles work.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('astro', 254, 'Life in the Universe', 3, 'Nature and origin of large numbers in the cosmos, the formation of planets, formation of life, and the anthropic principle.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('mth', 101, 'College Algebra', 5, 'This course, taken mostly by members from the School of Business is a primary graduate student funding vehicle for the the Department of Mathematics.  This is a 1000 person lecture reviews basic concepts taught in high school.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('chem', 100, 'Principles of Chemistry', 3, 'This course offers an introduction to the biological, inorganic, and chemical molecules, with an emphasis on basic principles of atomic and molecular electronic structure.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('chem', 110, 'Organic Chemistry I', 3, 'This course offers a practical introduction to organic chemistry, including a full set of problem sets with solutions. Focus is on the basic principles of understanding the structure and reactivity of organic molecules.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('chem', 115, 'Organic Chemistry Laboratory I', 2, 'Complements Organic Chemistry I. Practical laboratory experiments.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('chem', 314, 'Laboratory Chemistry', 3, 'Experimental chemistry for students who are planning to major in chemistry. Covers principles and applications of chemical laboratory techniques, including preparation and analysis of chemical materials.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('chem', 655, 'Protein Folding Problem', 3, 'Focuses on the mechanisms through which the amino acid sequence of polypeptide chains determine their three-dimensional conformation.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 211, 'General Physics I', 3, 'This course deals primarily with motions of objects and the forces that underlie these motions, including free fall, collisions between objects, rolling, and spinning.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 303, 'Relativity & Cosmology', 3, 'The course will describe developments that led to special relativity and its implications about the nature of space and time, as well as general relativity and quantum mechanics.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 388, 'Experimental Physics I', 2, 'In this course students will perform several experiments in different areas of physics. They will also learn fundamental experimental techniques and how to record and report results and perform analysis.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 512, 'Quantum Field Theory', 3, 'This course will cover the basic quantization of bosonic and fermionic fields, discrete spacetime symmetries, perturbative methods in QFT, definition of the S-matrix for scattering and decay processes.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 713, 'Quarks, Nuclei, and Cosmology', 3, 'General topics covered will include the structure of the nucleon, beta decay and weak interactions, and nucleon-nucleon interaction.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('phys', 819, 'Superconductivity', 3, 'This course provides a phenomenological approach to superconductivity, emphasizing super-conducting electronics.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 101, 'Introduction to Composition', 3, 'Provides students with the fundamental skills to read, interpret, and write critically at the college level.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 247, 'Boccaccio''s Decameron', 3, 'Follows of the arc of of the career Boccaccio with an emphasis on the Decameron, which is read in light of its cultural density and contextualized in terms of its antecedents, especially the Commedia of Dante.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 311, 'Writing Styles', 3, 'This practical course aids in understanding and writing non-fiction. We will look at some of the ways that prominent English writers have constructed their sentences and paragraphs. Students will write weekly exercises exploring these stylistic patterns.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 175, 'College Newspaper', 2, 'Students will perform journalistic writing exercises for publication in the college newspaper.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 276, 'Introduction to Science Writing', 3, 'This course provides an introduction to science writing. Students will learn about the business of science writing and will become familiar with the craft of making complex scientific research understandable for the general public.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('eng', 412, 'Ecology Writing Workshop', 2, 'This monthly workshop will focus on creation of a course assignment, a feature-length magazine article covering a complex topic in ecology.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('hist', 112, 'The United States in World History', 3, 'Examines the meaning of empire in relationship to the historical development of the United States of America.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('hist', 212, 'Historical Perspective on the Constitution', 3, 'This course covers the development of the constitutional doctrine from 1787 to the present. The Constitution as an experiment in Republicanism.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('hist', 415, 'History of the Family in Global Perspective', 3, 'Has the family really declined? What has changed in the last 1000 years? Drawing on cross-cultural examples, primarily from Latin America, the U.S. and Europe, this seminar explores the varieties of domestic forms.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('hist', 505, 'Science and History', 3, 'Introduces students to approaches and methods in the history of science, technology, and medicine');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('hist', 333, 'History of American Education', 3, 'A study of informal and formal education in American history leading to an understanding of present educational theory and practice.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('lang', 201, 'Introduction to Spanish', 4, 'Introduction to the Spanish language, with an emphasis on every day conversation and basic grammar.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('lang', 203, 'Intermediate Spanish', 3, 'Continuation of fundamental Spanish language learning, including verb tenses, reading skills, and basic conversation.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('lang', 304, 'Spanish Conversation Group', 2, 'Informal weekly conversation group designed to improve understanding of spoken Spanish. Suitable for both majors and casual speakers who wish to improve their skills for personal enjoyment or travel.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('lang', 207, 'Child Second Language Development', 3, 'Examines issues in child second language acquisition, including the critical period hypothesis and universal grammar.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('lang', 305, 'Second Language Syntax', 3, 'This course examines the form and acquisition of nonnative syntax. Consideration of whether nonnative grammars are fundamentally different than native grammars.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('psych', 102, 'General Psychology', 3, 'This course introduces the student to the major topics in scientific psychology as applied to human behavior. Applications of these principles will be made to the human experience.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('psych', 304, 'Introduction to Cognitive Psychology', 3, 'An introduction to the basic concepts of cognitive psychology, including areas such as perception, attention, memory, language, and thought.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('psych', 450, 'Laboratory in Applied Behavioral Science', 4, 'This course will provide students with hands-on training in the application of behavioral research technology to a clinical population.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('psych', 560, 'Examination of Real-Time Language Processing', 4, 'This lab course examines methods for the real-time examination of language processing in normal and disordered  language populations.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('psych', 610, 'Applied Child Psychology', 3, 'Introduction to major concepts and models used in psychological assessment and psychotherapeutic intervention of children. Several modalities of psychotherapy (individual, group, and family) will be reviewed along with research on their efficacy.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 113, 'American Government and Politics', 3, 'This course examines the structure, services, functions, and problems of government and politics at the national level.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 347, 'American Foreign Policy', 3, 'Theories, processes, and problem of American foreign policy and the craft of diplomacy, with special attention to contemporary issues.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 402, 'Government Internship: Semester in Washington', 4, 'Junior or seniors with a 3.0 grade point average or higher may apply for a limited number of internship opportunities in Washington, DC. Interns will live at University House on Capitol Hill and continue their normal class schedule at the DC Campus.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 644, 'Research Seminar in Middle Eastern Affairs', 3, 'Government and Politics of the Middle East and North Africa Spring. Historical background, contemporary setting, political processes, and major problems of some of the countries of Middle East and North Africa.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 715, '#5 is the 50% Solution', 3, 'A history of the two-state solution and other approaches to Palestian Statehood');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('poli', 431, 'American Government and Corporate Interests', 3, 'This course will examine the methods by which American business exert influence over legislators and the legislative process.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 102, 'Introduction to Computer Science', 3, 'This course in an introduction to the discipline of computer science. Topics include algorithmic foundations, hardware concepts, virtual machine concepts, software systems, applications, and social issues.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 230, 'History of Computing', 3, 'This course will survey the history of the computing field from antiquity to the present, focusing on the era of the electronic digital computer. Topics will include historical developments in hardware, software, and the theoretical foundations of computer science.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 350, 'Introduction to Signal Processing', 3, 'This course covers the nature of information, signals, transforms, and applications. Topics include analog to digital and digital to analog conversion, data storage (such as the audio format MP3), data transforms, and filters.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 615, 'Introduction to Automata', 3, 'Theory of computing devices and the languages they recognize.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 710, 'Laboratory in Computer Science', 4, 'Independent research opportunity using the university computer lab. Requires instructor permission and may be repeated.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 810, 'Thesis Research', 3, 'Guided research leading to production of the thesis. Requires instructor permission and may be repeated.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('comp', 819, 'Advanced Algorithms in Bioinformatics', 3, 'This course is focused on fundamental algorithmic techniques in Bioinformatics, including classed methods such as dynamic programming, support vector machines and other statistical and learning optimization methods.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('ee', 107, 'Exploration of Electrical Engineering', 3, 'Exploration of electrical engineering through several hands-on activities that cover a broad spectrum of applications and fundamental concepts. ');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('ee', 202, 'Engineering Electromagnetics', 3, 'Static electric and magnetic fields; solutions to static field problems, electromagnetic waves, boundary conditions, engineering applications.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('ee', 412, 'Laboratory in Electrical Engineering', 4, 'Hands-on experience covering areas of optical transforms, electro-optics devices, signal processing, fiber optics transmission, and holography.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('ee', 505, 'Information Theory', 3, 'Mathematical measurement of information; information transfer in discrete systems; redundancy, efficiency, and channel capacity; encoding systems.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('ee', 615, 'Learning and Adaptive Systems', 3, 'Adaptive and learning control systems; system identification; performance indices; gradient, stochastic approximation, controlled random search methods; introduction to pattern recognition.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('me', 111, 'Introduction to Mechanical Engineering', 3, 'Topics include an overview of career opportunities, problem solving processes, an introduction to the basic engineering design process, professionalism, professional registration, and ethics.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('me', 344, 'Undergraduate Research', 4, 'Undergraduates will conduct independent research activities under the direction of their major adviser.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('me', 501, 'Advanced Welding', 3, 'Advanced applications of welding and machine tool technology. Computer numerical control, multi-axis machining set-up, gas tungsten arc welding, and gas metal arch welding.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('me', 627, 'Advanced Heating and Air Conditioning', 4, '');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('me', 712, 'Graphic Communication and Design', 3, 'Sketching and orthographic projection. Covers detail and assembly working drawings, dimensioning, tolerance specification, and design projects.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('be', 112, 'Introduction to Biomedical Engineering', 3, 'This course covers topics in multiple formats ranging from lectures by faculty or guest speakers to presentations by participating students.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('be', 308, 'Fundamentals of Biochemistry', 3, 'Fundamental aspects of human biochemistry are introduced in this course for students in the bioinstrumentation/biosensors, biomechanics, and medical-imaging tracks.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('be', 415, 'Internship in Biomedical Engineering', 8, 'The student will work twenty hours per week in an area firm to gain experience in the application of biomedical engineering principles in an industrial setting.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('be', 509, 'Systems of Drug Delivery', 3, 'The mathematics of diffusion through various types of biological media is discussed.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 202, 'History of Art Criticism', 3, 'An introductory survey course on Prehistoric through late-Medieval art history.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 712, 'Museum and Gallery Management', 4, 'Supervised independent field experience and practical work in all areas of Art Museum management in the university and greater metropolitan area communities.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 340, 'Arts of Asia', 3, 'An introduction to the history and criticism of Far Eastern art, including the art of China and Japan, fine and decorative arts.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 710, 'Methods in Art History', 3, 'This seminar focuses on basic types of art-historical method. Some meetings focus on a single author who exemplifies a particular approach.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 809, 'Materials and Construction in European Art', 3, 'A scientific examination of the materials and manufacturing techniques employed in Europe over the last two centuries.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('arthis', 623, 'Contemporary Latin American Art', 3, 'A survey of the last twenty years of Latin American art with a focus on the Caribbean and Central America.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 714, 'Peer Portfolio Review', 0, 'An opportunity to practice giving and receiving constructive criticism.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 411, 'Underwater Basket Weaving', 4, 'This course provides a novel perspective on the traditional art of basketry as it is experienced in reduced gravity and in the context of fluid dynamics. Requires instructor permission and a valid c-card.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 512, 'Art in Therapy', 3, 'Surveys methods and results of using art and craft therapy with developmentally disabled adults.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 614, 'Drawing Master Class', 5, 'For fine arts majors only, an intensive studio study including field trips to local parks and museums and a final group art show.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 509, 'Twentieth Century Printmaking', 4, 'Development of personalized concepts and individual aesthetic expression in printmaking with reference to various styles and trends in Twentieth Century printmaking.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 333, 'Drawing', 3, 'Exploration of the structure and interrelationships of visual form in drawing, painting, and sculpture. Principal historical modes of drawing are examined.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('tched', 122, 'Theory and Practice of Early Childhood Education', 3, 'Emphasis on the skills and processes needed for the design and implementation of optimal learning environments. Exploration of issues related to societal and cultural influences on the education of young children.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('tched', 155, 'Methods of Early Science Education', 3, 'A study of the curriculum methods, concepts, techniques, and materials in the teaching of general science to children in the early grades.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('tched', 367, 'Problems in Education Management', 3, 'This course is designed to assist the student to prepare for management of educational organizations and programs. Emphasis will be placed upon identifying specific problems and developing specific techniques by which to solve them.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('tched', 501, 'Challenges of Teaching the Gifted and Talented', 3, 'The nature and needs of the talented and gifted in all areas of development are explored.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('tched', 609, 'Supervised Internship in Education', 4, 'Supervised Internship I provides on-site, supervised instructional experience within a public school setting under the leadership of an appropriate, competent professional.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('edpol', 202, 'Technology in the Classroom', 3, 'Theories and practice of using educational technologies to support problem-based learning.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('edpol', 551, 'Classroom Visit', NULL, 'Elective visit to a local classroom for observation.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('stdart', 119, 'Spring Basket Weaving Workshop', NULL, 'A just-for-fun chance to learn the basics of basket weaving.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('edpol', 313, 'Technology, Society and Schools', 3, 'Examination of theories and history of interaction of society and technology with implications for instructional technology and schooling. Resources for constructing personal definitions of technology.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('edpol', 505, 'Qualitative Research for Educators', 3, 'This course provides an introduction to qualitative research at the Master level.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('edpol', 617, 'Educational Policy Analysis', 3, 'Frameworks for analyzing, designing policy proposals, and implementing plans.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('corpfi', 234, 'Accounting Information Systems', 3, 'This course bridges the gap between two disciplines critical to business operations.  This course of study teaches students to design and deploy information technology to improve the accounting systems of an organization.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('corpfi', 404, 'Corporate Financial Management', 3, 'This course covers advanced topics in corporate financial management, including its role in corporate governance.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('corpfi', 601, 'Case Studies in Corporate Finance', 3, 'A course designed to use case studies and financial analysis to further knowledge and ability to make financial management decisions.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 100, 'Practical Bookkeeping', 2, NULL);
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 200, 'Principles of Accounting I', 3, 'The initial course in the theory and practice of financial accounting. Topics emphasized include the preparation, reporting, and analysis of financial data.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 315, 'Financial Accounting', 5, 'Integration of the conceptual and computational aspects of asset, liability and stockholders equity accounting.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 426, 'Corporate Taxation', 3, 'Concepts and methods of determining federal tax liability of corporations.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 527, 'Advanced Accounting', 3, 'Theory and practical applications of accounting for consolidated entities and partnerships; includes foreign currency transactions, hedging and derivatives.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('acc', 606, 'Corporate Financial Law', 3, 'Law governing business corporations; fiduciary duties of managers and directors in situations such as mergers, acquisitions, securities offerings, market domination, litigation.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('capmrk', 712, 'International Financial Markets', 3, 'Offers an understanding of the international financial structure and studies its impact on business and individuals in various nations.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('capmrk', 808, 'Principles of Portfolio Management', 3, 'Comprehensive coverage of the theory and practice of money management as well as in-depth analysis of the theory and practice involved when securities are combined into portfolios.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('capmrk', 818, 'Financial Statement Analysis', 3, 'This course presents techniques for analyzing current and projected financial statements for the purposes of credit analysis, security analysis, and internal financial analysis, and cash flow forecasting.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('capmrk', 756, 'Capital Risk Management', 3, 'This course introduces fundamental principles and techniques of financial risk management.');


-- --------------------------------------------------------------------
-- Instructor Directory
--

CREATE TABLE instructor (
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

CREATE TABLE confidential (
    instructor  VARCHAR(16) NOT NULL,
    SSN         CHAR(11) NOT NULL,
    pay_grade   INTEGER NOT NULL,
    home_phone  VARCHAR(16),
    CONSTRAINT confidential_pk
      PRIMARY KEY (instructor),
    CONSTRAINT confidential_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES instructor(code)
);

CREATE TABLE appointment (
    department  VARCHAR(16) NOT NULL,
    instructor  VARCHAR(16) NOT NULL,
    percent     FLOAT,
    CONSTRAINT appointment_pk
      PRIMARY KEY (department, instructor),
    CONSTRAINT appointment_department_fk
      FOREIGN KEY (department)
      REFERENCES department(code),
    CONSTRAINT appointment_instructor_fk
      FOREIGN KEY (instructor)
      REFERENCES instructor(code)
);

INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('cfergus12', 'prof', 'Adam Ferguson', NULL, 'cfergus12@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('evargas112', 'prof', 'Elena Vargas', '555-1572', 'evargas112@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('afrenski', 'mr', 'Andre Frenski', '555-1723', 'afrenski@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('wyu112', 'mr', 'Walter Yu', '555-2954', 'wyu112@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('amiller213', 'ms', 'Antoinette Miller', '555-7728', 'amiller213@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('dsims51', 'mr', 'Dante Sims', NULL, 'dsims51@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('srandrews', 'mr', 'Stuart Andrews', '555-2113', 'srandrews@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('sbyrne202', 'ms', 'Susan Byrne', '555-9002', NULL);
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('sbadhreya', 'prof', 'Said Badhreya', '555-2873', 'sbadhreya@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('lmcooper11', 'prof', 'Louisa M. Cooper', '555-2112', 'lmscooper11@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('mbyer55', 'prof', 'Michael L. Byer', '555-1287', NULL);
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('pblum21', 'ms', 'Petra Blum', '555-2873', 'pblum21@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('icampbell12', 'prof', 'Ian W. Campbell', '555-2275', 'icampbell12@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('tobrien21', 'prof', 'Timothy O''Brien', NULL, 'tobrien21@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('acaspar', 'dr', 'Alesia Caspar', NULL, NULL);
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('dbundt31', 'dr', 'David Bundt', '555-1553', 'dbundt31@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('kmarkman', 'ms', 'Kristen Maison', NULL, 'kmarkman@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('kmaas11', 'prof', 'Kari Maas', '555-1027', 'kmaas11@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('hbenmahem', 'mr', 'Hani Ben-Mahem', '555-1827', 'hbenmahem@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('bburling', 'prof', 'Benjamin Burling', '555-1823', 'bburling@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('mcardana', 'prof', 'Maximo Cardana', '555-1738', 'mcardana@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('jconnell51', 'dr', 'Jamie Connell', '555-2157', 'jconnell51@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('vball77', 'ms', 'Vivienne Ball', '555-2830', NULL);
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('kmurray44', 'mr', 'Kevin Murray', '555-1753', 'kmurray44@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('lbrooks61', 'prof', 'Lynn L. Brooks', '555-8872', 'lbrooks61@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('anabib', 'prof', 'Ashish Nabib', '555-1667', 'anabib@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('alang42', 'prof', 'Adrian Laang', '555-0973', 'alang42@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('kcavallaro', 'prof', 'Katherine Cavallaro', '555-4325', 'kvallaro@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('hbarone', 'prof', 'Harold Barone', '555-0911', 'hbarone@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('emurphy55', 'prof', 'Erin L. Murphy', NULL, 'emurphy55@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('egasner', 'dr', 'Ernst Gasner', '555-6652', 'egasner@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('elhill4', 'dr', 'Ellen Last Hill', '555-1995', 'ehill4@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('rrosenfeld31', 'ms', 'Rebecca L. Rosenfeld', '555-8816', 'rrosenfeld31@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('astone77', 'mr', 'Alan P. Stone', '555-1738', 'astone77@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('dfallon23', 'prof', 'David N. Fallon', '555-1666', 'dfallon23@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('jflug29', 'dr', 'Jason Flug', '555-6672', 'jflug23@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('asacco', 'prof', 'Andrea Sacco', '555-1381', 'asacco@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('bsacks66', 'prof', 'Benjamin Sacks', '555-2212', 'bsacks66@example.com');
INSERT INTO instructor (code, title, full_name, phone, email) VALUES
    ('mscott51', 'prof', 'Mindy Scott', '555-3521', 'mscott51@example.com');

INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('cfergus12', '987-65-4320', 6, '702-555-1738');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('afrenski', '987-65-4321', 4, NULL);
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('wyu112', '987-65-4323', 5, '702-555-2954');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('amiller213', '987-65-4324', 7, '452-555-7728');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('dsims51', '987-65-4325', 5, '452-555-9273');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('srandrews', '987-65-4326', 8, '702-555-3627');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('sbyrne202', '987-65-4327', 5, '617-555-8382');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('sbadhreya', '987-65-4328', 4, '702-555-66738');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('lmcooper11', '987-65-4329', 8, '702-555-9992');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('mbyer55', '000-22-4320', 6, '452-555-7311');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('pblum21', '000-33-2783', 5, '702-555-6522');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('icampbell12', '000-52-8758', 5, NULL);
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('tobrien21', '000-38-2875', 4, NULL);
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('bburling', '672-88-0000', 5, NULL);
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('dbundt31', '000-53-2873', 7, '202-555-1738');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('kmarkman', '000-72-1875', 4, '702-555-8211');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('kmaas11', '371-55-0000', 7, '702-555-1875');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('hbenmahem', '674-57-0000', 4, '702-555-0115');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('jconnell51', '717-67-0000', 8, '702-555-1672');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('vball77', '799-11-0000', 4, '702-555-1425');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('kmurray44', '152-62-0000', 7, '702-555-6612');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('lbrooks61', '673-11-0000', 8, '452-555-7276');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('anabib', '787-22-0000', 7, '702-55-1627');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('alang42', '788-33-0000', 6, '702-555-1721');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('kcavallaro', '123-74-0000', 5, '702-555-1670');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('hbarone', '511-66-0000', 4, '702-555-1089');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('emurphy55', '787-22-0000', 8, '452-555-7849');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('egasner', '784-44-0000', 8, '702-555-8995');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('elhill4', '933-55-0000', 4, '702-555-8829');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('rrosenfeld31', '857-22-0000', 7, '702-555-0989');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('astone77', '688-33-0000', 8, '702-555-0173');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('dfallon23', '274-66-0000', 4, '702-555-1778');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('jflug29', '578-41-0000', 6, '702-555-7727');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('asacco', '783-78-0000', 5, '702-555-1692');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('bsacks66', '782-78-0000', 7, '202-555-7283');
INSERT INTO confidential (instructor, SSN, pay_grade, home_phone) VALUES
    ('mscott51', '126-33-0000', 7, '702-555-7819');

INSERT INTO appointment (department, instructor, percent) VALUES
    ('stdart', 'acaspar', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('phys', 'afrenski', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('ee', 'alang42', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('lang', 'amiller213', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('comp', 'anabib', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('astro', 'asacco', NULL);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('corpfi', 'astone77', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('phys', 'bburling', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('chem', 'bsacks66', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('astro', 'cfergus12', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('tched', 'dbundt31', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('acc', 'dfallon23', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('hist', 'dsims51', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('stdart', 'egasner', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('tched', 'elhill4', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('arthis', 'emurphy55', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('chem', 'evargas112', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('be', 'hbarone', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('chem', 'hbenmahem', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('arthis', 'icampbell12', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('lang', 'jconnell51', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('capmrk', 'jflug29', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('me', 'kcavallaro', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('astro', 'kmaas11', NULL);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('edpol', 'kmarkman', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('psych', 'kmurray44', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('poli', 'lbrooks61', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('ee', 'lmcooper11', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('me', 'mbyer55', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('eng', 'mcardana', NULL);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('phys', 'mscott51', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('be', 'pblum21', 0.50);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('edpol', 'rrosenfeld31', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('psych', 'srandrews', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('comp', 'sbadhreya', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('poli', 'sbyrne202', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('hist', 'vball77', 1.00);
INSERT INTO appointment (department, instructor, percent) VALUES
    ('eng', 'wyu112', 0.50);


-- --------------------------------------------------------------------
-- Class Directory
--

CREATE TABLE semester (
    year        INTEGER NOT NULL,
    season      VARCHAR(6) NOT NULL,
    begin_date  DATE NOT NULL,
    end_date    DATE NOT NULL,
    CONSTRAINT semester_pk
      PRIMARY KEY (year, season),
    CONSTRAINT semester_season_ck
      CHECK (season IN ('fall', 'spring', 'summer'))
);

CREATE TABLE class (
    department  VARCHAR(16) NOT NULL,
    course      INTEGER NOT NULL,
    year        INTEGER NOT NULL,
    season      VARCHAR(6) NOT NULL,
    section     CHAR(3) NOT NULL,
    instructor  VARCHAR(16),
    class_seq   INTEGER NOT NULL,
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
       REFERENCES course(department, number),
    CONSTRAINT class_semester_fk
       FOREIGN KEY (year, season)
       REFERENCES semester(year, season),
    CONSTRAINT class_instructor_fk
       FOREIGN KEY (instructor)
       REFERENCES instructor(code)
);

INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2009, 'spring', '2010-01-01', '2010-05-15');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2010, 'spring', '2011-01-01', '2011-05-15');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2011, 'spring', '2012-01-01', '2012-05-15');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2012, 'spring', '2013-01-01', '2013-05-15');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2009, 'fall', '2009-08-15', '2009-12-31');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2010, 'fall', '2010-08-15', '2010-12-31');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2011, 'fall', '2011-08-15', '2011-12-31');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2012, 'fall', '2012-08-15', '2012-12-31');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2009, 'summer', '2010-06-01', '2010-08-01');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2010, 'summer', '2011-06-01', '2011-08-01');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2011, 'summer', '2012-06-01', '2012-08-01');
INSERT INTO semester (year, season, begin_date, end_date) VALUES
    (2012, 'summer', '2013-06-01', '2013-08-01');

INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('astro', 137, 2009, 'fall', '001', 'egasner', 10001);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('astro', 142, 2009, 'spring', '001', 'asacco', 10002);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('astro', 155, 2010, 'fall', '001', 'cfergus12', 10003);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('astro', 254, 2009, 'summer', '002', 'cfergus12', 10004);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('chem', 100, 2010, 'fall', '002', 'bsacks66', 10005);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('chem', 110, 2010, 'spring', '001', 'evargas112', 10006);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('chem', 115, 2012, 'summer', '003', 'hbenmahem', 10007);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('chem', 314, 2011, 'fall', '001', 'evargas112', 10008);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('chem', 655, 2011, 'fall', '001', 'bsacks66', 10009);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 211, 2011, 'spring', '001', 'afrenski', 10010);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 303, 2012, 'fall', '001', 'bburling', 10011);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 388, 2011, 'summer', '002', 'mscott51', 10012);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 512, 2009, 'fall', '002', 'afrenski', 10013);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 713, 2009, 'spring', '001', 'bburling', 10014);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('phys', 819, 2010, 'fall', '003', 'bsacks66', 10015);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 101, 2009, 'summer', '001', 'mcardana', 10016);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 247, 2010, 'fall', '001', 'wyu112', 10017);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 311, 2010, 'spring', '001', 'wyu112', 10018);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 175, 2010, 'summer', '001', 'wyu112', 10019);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 276, 2012, 'fall', '002', 'mcardana', 10020);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('eng', 412, 2011, 'fall', '002', 'mcardana', 10021);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('hist', 112, 2011, 'spring', '001', 'dsims51', 10022);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('hist', 212, 2011, 'spring', '003', 'vball77', 10023);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('hist', 415, 2012, 'fall', '001', 'dsims51', 10024);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('hist', 505, 2009, 'fall', '001', 'vball77', 10025);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('hist', 333, 2009, 'spring', '001', 'dsims51', 10026);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('lang', 201, 2010, 'fall', '001', 'amiller213', 10027);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('lang', 203, 2012, 'spring', '002', 'amiller213', 10028);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('lang', 304, 2010, 'fall', '002', 'jconnell51', 10029);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('lang', 207, 2010, 'spring', '001', 'jconnell51', 10030);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('lang', 305, 2010, 'summer', '003', 'jconnell51', 10031);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('psych', 102, 2011, 'fall', '001', 'kmurray44', 10032);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('psych', 304, 2011, 'fall', '001', 'srandrews', 10033);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('psych', 450, 2011, 'spring', '001', 'srandrews', 10034);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('psych', 560, 2012, 'fall', '001', 'kmurray44', 10035);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('psych', 610, 2011, 'summer', '002', 'kmurray44', 10036);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('poli', 113, 2009, 'fall', '002', 'lbrooks61', 10037);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('poli', 347, 2012, 'spring', '001', 'lbrooks61', 10038);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('poli', 402, 2010, 'fall', '003', 'sbyrne202', 10039);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('poli', 644, 2009, 'summer', '001', 'sbyrne202', 10040);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('poli', 431, 2010, 'fall', '001', 'sbyrne202', 10041);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 102, 2010, 'spring', '001', 'anabib', 10042);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 230, 2010, 'summer', '001', 'anabib', 10043);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 350, 2011, 'fall', '002', 'sbadhreya', 10044);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 615, 2011, 'fall', '002', 'sbadhreya', 10045);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 710, 2011, 'spring', '001', 'sbadhreya', 10046);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 810, 2011, 'spring', '003', 'anabib', 10047);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('comp', 819, 2011, 'summer', '001', 'anabib', 10048);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('ee', 107, 2009, 'fall', '001', 'alang42', 10049);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('ee', 202, 2012, 'spring', '001', 'alang42', 10050);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('ee', 412, 2010, 'fall', '001', 'lmcooper11', 10051);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('ee', 505, 2009, 'summer', '002', 'alang42', 10052);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('ee', 615, 2010, 'fall', '002', 'lmcooper11', 10053);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('me', 111, 2010, 'spring', '001', 'kcavallaro', 10054);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('me', 344, 2010, 'summer', '003', 'kcavallaro', 10055);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('me', 501, 2012, 'fall', '001', 'mbyer55', 10056);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('me', 627, 2011, 'fall', '001', 'mbyer55', 10057);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('me', 712, 2011, 'spring', '001', 'mbyer55', 10058);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('be', 112, 2011, 'spring', '001', 'hbarone', 10059);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('be', 308, 2011, 'summer', '002', 'hbarone', 10060);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('be', 415, 2010, 'spring', '002', 'pblum21', 10061);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('be', 509, 2012, 'summer', '001', 'pblum21', 10062);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 202, 2011, 'fall', '003', 'emurphy55', 10063);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 712, 2011, 'fall', '001', 'icampbell12', 10064);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 340, 2011, 'spring', '001', 'icampbell12', 10065);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 710, 2011, 'spring', '001', 'emurphy55', 10066);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 809, 2011, 'summer', '001', 'emurphy55', 10067);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('arthis', 623, 2009, 'fall', '002', 'icampbell12', 10068);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('stdart', 411, 2009, 'spring', '002', 'acaspar', 10069);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('stdart', 512, 2010, 'fall', '001', 'egasner', 10070);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('stdart', 614, 2009, 'summer', '003', 'egasner', 10071);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('stdart', 509, 2012, 'summer', '001', 'acaspar', 10072);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('stdart', 333, 2010, 'spring', '001', 'acaspar', 10073);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('tched', 122, 2010, 'summer', '001', 'dbundt31', 10074);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('tched', 155, 2011, 'fall', '001', 'elhill4', 10075);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('tched', 367, 2011, 'fall', '002', 'elhill4', 10076);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('tched', 501, 2011, 'spring', '002', 'dbundt31', 10077);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('tched', 609, 2011, 'spring', '001', 'dbundt31', 10078);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('edpol', 202, 2012, 'fall', '003', 'kmarkman', 10079);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('edpol', 313, 2009, 'fall', '001', 'kmarkman', 10080);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('edpol', 505, 2009, 'spring', '001', 'rrosenfeld31', 10081);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('edpol', 617, 2010, 'fall', '001', 'rrosenfeld31', 10082);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('corpfi', 234, 2009, 'summer', '001', 'astone77', 10083);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('corpfi', 404, 2010, 'fall', '002', 'astone77', 10084);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('corpfi', 601, 2012, 'summer', '002', 'astone77', 10085);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('acc', 200, 2010, 'summer', '001', 'dfallon23', 10086);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('acc', 315, 2011, 'fall', '003', 'dfallon23', 10087);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('acc', 426, 2011, 'fall', '001', 'dfallon23', 10088);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('acc', 527, 2011, 'spring', '001', 'dfallon23', 10089);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('acc', 606, 2011, 'spring', '001', 'dfallon23', 10090);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('capmrk', 712, 2011, 'summer', '001', 'jflug29', 10091);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('capmrk', 808, 2012, 'summer', '002', 'jflug29', 10092);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('capmrk', 818, 2010, 'spring', '002', 'jflug29', 10093);
INSERT INTO class (department, course, year, season, section, instructor, class_seq) VALUES
    ('capmrk', 756, 2011, 'spring', '001', 'jflug29', 10094);


-- --------------------------------------------------------------------
-- Enrollment Directory
--

CREATE TABLE student (
    number      INTEGER NOT NULL,
    name        VARCHAR(64) NOT NULL,
    gender      CHAR(1) NOT NULL,
    dob         DATE NOT NULL,
    school      VARCHAR(16),
    program     VARCHAR(16),
    start_date  DATE NOT NULL,
    is_active   BOOLEAN NOT NULL,
    CONSTRAINT student_pk
      PRIMARY KEY (number),
    CONSTRAINT student_gender_ck
       CHECK (gender IN ('f', 'i', 'm')),
    CONSTRAINT student_is_active_ck
       CHECK (is_active IN (0, 1)),
    CONSTRAINT student_school_fk
      FOREIGN KEY (school)
      REFERENCES school (code),
    CONSTRAINT student_program_fk
      FOREIGN KEY (school, program)
      REFERENCES program (school, code)
);

CREATE TABLE enrollment (
    student     INTEGER NOT NULL,
    class       INTEGER NOT NULL,
    status      CHAR(3) NOT NULL,
    grade       FLOAT,
    CONSTRAINT enrollment_pk
      PRIMARY KEY (student, class),
    CONSTRAINT enrollment_status_ck
       CHECK (status IN ('enr', 'inc', 'ngr')),
    CONSTRAINT enrollment_student_fk
      FOREIGN KEY (student)
      REFERENCES student(number),
    CONSTRAINT enrollment_class_fk
      FOREIGN KEY (class)
      REFERENCES class(class_seq)
);

INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('25371', 'John L. Hanley', 'm', '1990-04-28', 'eng', 'gbuseng', '2009-07-15', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('29878', 'Ellen Lansburgh', 'f', '1992-02-01', 'bus', 'uacct', '2008-01-05', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('37278', 'Ming Wang', 'm', '1988-03-15', 'la', 'gengl', '2002-11-27', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('92039', 'Syed Ishaq', 'm', '1992-10-23', 'art', 'gart', '2010-09-02', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('37283', 'Janine Sylvia', 'f', '1993-12-02', 'ns', 'uastro', '2009-08-14', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('17385', 'Valeria Rinaldi', 'f', '1985-09-02', 'bus', 'pcap', '2004-09-01', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('28371', 'Ken Tanaka', 'm', '1992-11-03', 'art', 'gart', '2010-09-08', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('21837', 'Jalene Flambeau', 'f', '1989-03-23', 'art', 'gart', '2010-06-11', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('38187', 'Mary Ann Flenderson', 'f', '1993-05-16', 'ns', 'uphys', '2010-08-26', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('43278', 'Trina Wood Campbell', 'f', '1990-02-12', 'eng', 'gme', '2007-09-01', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('27138', 'Artem Karpov', 'm', '1991-10-16', 'eng', 'gbe', '2009-08-22', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('12837', 'Christine Leung', 'f', '1991-06-06', 'eng', 'gme', '2009-08-17', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('38721', 'Alicia Montez-Galliano', 'f', '1994-07-11', 'ns', 'uchem', '2010-09-10', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('37182', 'Amy Yang', 'f', '1992-12-17', 'ns', 'uphys', '2002-08-10', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('32718', 'Raisa Antonova', 'f', '1992-12-09', 'eng', 'gbe', '2008-09-15', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('32711', 'Peter Zajac Jr.', 'm', '1994-01-23', 'bus', 'ucorpfi', '2009-09-10', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('33278', 'Andrea Kaminski', 'f', '1981-04-20', 'bus', 'pcap', '2009-01-15', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('17283', 'Lucy Ryong', 'f', '1988-01-25', 'edu', 'gedu', '2009-01-27', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('12738', 'Helmut Dietmark', 'm', '1989-11-27', 'edu', 'psci', '2008-03-17', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('23817', 'Benjamin Wen', 'm', '1993-12-16', 'la', 'uhist', '2009-01-12', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('57382', 'Paul Duncan Ulam', 'm', '2001-05-05', 'la', 'uspan', '2009-05-21', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('13723', 'Narissa Maya', 'f', '1992-04-30', 'la', 'upsych', '2007-11-21', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('31332', 'Dara Subramanya', 'f', '1994-11-16', 'la', 'upsych', '2008-09-10', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('35572', 'Corinna Ellis', 'f', '1995-07-22', 'edu', 'glited', '2007-05-14', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('12328', 'Karen Yuen', 'f', '1991-09-10', 'ns', 'uphys', '2007-05-16', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('32214', 'Joseph Tan', 'm', '1992-08-01', 'eng', 'gbuseng', '2008-01-06', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('22313', 'James Earl Sims III', 'm', '2002-07-06', 'eng', 'umech', '2004-08-16', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('24431', 'Annette Dupree', 'f', '1987-01-28', 'eng', 'umech', '2006-01-16', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('38794', 'Bailey Melvin', 'm', '1988-03-13', 'la', 'psciwri', '2005-04-20', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('37855', 'Amina N. Elsaeed', 'f', '1987-10-29', 'la', 'uhist', '2005-09-02', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('35523', 'Nikki Agbo', 'm', '1985-05-05', 'la', 'gengl', '2006-02-25', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('20927', 'Glenn L. McNair', 'm', '1987-12-13', 'eng', 'gee', '2009-08-23', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('35183', 'Teisha Worth Day', 'f', '1983-12-31', 'edu', 'gedlead', '2009-08-21', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('25723', 'Kumar Suresh', 'm', '1994-09-11', 'eng', 'ucompsci', '2009-08-23', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('24672', 'Mahesh Basa', 'm', '1995-08-21', 'eng', 'ucompsci', '2008-04-15', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('23137', 'Rachel Feld', 'f', '1992-09-27', 'ns', 'uchem', '2008-12-23', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('35163', 'Nicola Ralls Jr.', 'f', '1993-06-02', 'bus', 'uacct', '2010-01-12', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('21135', 'Luis Riviera Espinoza', 'm', '1993-05-21', 'eng', 'gbe', '2010-02-19', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('31735', 'Demetrios Kanakis', 'm', '1995-04-17', 'eng', 'ucompsci', '2009-05-21', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('21166', 'Laura Elmer Long', 'f', '1991-02-14', 'ns', 'uastro', '2009-01-31', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('31331', 'Khadija Hamad Azzan', 'f', '1992-11-26', 'ns', 'uastro', '2008-09-21', 0);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('36446', 'Milton Mahanga', 'm', '1991-11-06', 'art', 'gart', '2009-05-05', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('26764', 'Bernard Careval', 'm', '1992-08-23', 'art', 'gart', '2008-07-30', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('26743', 'Ulf Knudsen', 'm', '1990-11-14', 'ns', 'uphys', '2008-04-27', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('31835', 'Paavo Kekkonen', 'm', '2000-09-08', 'ns', 'uphys', '2008-06-11', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('29301', 'Eduardo Serrano', 'm', '1991-09-09', 'art', 'uhist', '2006-01-14', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('21263', 'Ari Ben David', 'm', '1989-03-15', 'la', 'gengl', '2006-12-15', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('37744', 'Scott Blank', 'm', '1988-06-12', 'bus', 'ucorpfi', '2007-12-15', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('28382', 'Martha O''Mally', 'f', '1995-05-14', 'bus', 'pacc', '2005-01-01', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('27281', 'José N. Marteñes', 'm', '1993-11-19', 'eng', 'ucompsci', '2007-06-15', 1);
INSERT INTO student (number, name, gender, dob, school, program, start_date, is_active) VALUES
    ('27817', 'Niall Crawford', 'm', '1998-12-14', 'bus', 'pacc', '2010-01-02', 1);

INSERT INTO enrollment (student, class, status, grade) VALUES
    ('25371', 10086, 'ngr', NULL);
INSERT INTO enrollment (student, class, status, grade) VALUES
    ('25371', 10051, 'enr', 3.7);
INSERT INTO enrollment (student, class, status, grade) VALUES
    ('29878', 10086, 'inc', NULL);
INSERT INTO enrollment (student, class, status, grade) VALUES
    ('37278', 10018, 'enr', 2.6);
INSERT INTO enrollment (student, class, status, grade) VALUES
    ('92039', 10071, 'enr', 3.1);


-- --------------------------------------------------------------------
-- Requirement Directory
--

CREATE TABLE prerequisite (
    of_department   VARCHAR(16) NOT NULL,
    of_course       INTEGER NOT NULL,
    on_department   VARCHAR(16) NOT NULL,
    on_course       INTEGER NOT NULL,
    CONSTRAINT prerequisite_pk
      PRIMARY KEY (of_department, of_course, on_department, on_course),
    CONSTRAINT prerequisite_on_course_fk
      FOREIGN KEY (on_department, on_course)
      REFERENCES course(department, number),
    CONSTRAINT prerequisite_of_course_fk
      FOREIGN KEY (of_department, of_course)
      REFERENCES course(department, number)
);

CREATE TABLE classification (
    code        VARCHAR(16) NOT NULL,
    type        VARCHAR(10),
    title       VARCHAR(64) NOT NULL,
    description TEXT,
    part_of     VARCHAR(16),
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
    department      VARCHAR(16) NOT NULL,
    course          INTEGER NOT NULL,
    classification  VARCHAR(16) NOT NULL,
    CONSTRAINT course_classification_pk
      PRIMARY KEY (department, course, classification),
    CONSTRAINT course_classification_course_fk
      FOREIGN KEY (department, course)
      REFERENCES course(department, number),
    CONSTRAINT course_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES classification(code)
);

CREATE TABLE program_requirement (
    school          VARCHAR(16) NOT NULL,
    program         VARCHAR(16) NOT NULL,
    classification  VARCHAR(16) NOT NULL,
    credit_hours    INTEGER NOT NULL,
    rationale       TEXT,
    CONSTRAINT program_classification_pk
      PRIMARY KEY (school, program, classification),
    CONSTRAINT program_classification_course_fk
      FOREIGN KEY (school, program)
      REFERENCES program(school, code),
    CONSTRAINT program_classification_classification_fk
      FOREIGN KEY (classification)
      REFERENCES classification(code)
);

INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('astro', 142, 'astro', 137);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('chem', 314, 'chem', 115);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('chem', 110, 'chem', 100);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('phys', 303, 'phys', 211);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('phys', 713, 'phys', 512);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('eng', 412, 'eng', 276);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('hist', 212, 'hist', 112);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('lang', 203, 'lang', 201);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('lang', 305, 'lang', 207);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('poli', 402, 'poli', 113);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('comp', 710, 'comp', 102);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('ee', 412, 'ee', 107);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('me', 344, 'me', 111);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('be', 415, 'be', 112);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('arthis', 710, 'arthis', 202);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('stdart', 614, 'stdart', 333);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('tched', 609, 'tched', 122);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('edpol', 313, 'edpol', 202);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('corpfi', 601, 'corpfi', 404);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('acc', 527, 'acc', 200);
INSERT INTO prerequisite (of_department, of_course, on_department, on_course) VALUES
    ('capmrk', 818, 'acc', 315);

INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('cross', NULL, 'Cross-Cutting Requirements', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('writing', 'university', 'Writing Intensive', 'Writing intensive courses involve 3 or more papers per semester; at least one of which is a research paper of 20 pages or more.', 'cross');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('reasoning', 'university', 'Quantitative Reasoning', 'Quantitative resoning courses focus on numerical analysis to evaluate, describe and justify outcomes of complex decisions.', 'cross');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('diversity', 'university', 'Region and Ethnic Diversity', 'Courses which provide a rich exposure to foreign cultures and regions qualify for this classification.', 'cross');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('research', 'university', 'Research Experience', 'Research courses focus on the scientific method to create hypothesis and test them in a structured laboratory environment.', 'cross');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('humanities', 'university', 'Arts, Letters, and the Humanities', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('arthistory', 'school', 'Art', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('ancient', 'department', 'Ancient Art', NULL, 'arthistory');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('classical', 'department', 'Classical Art', NULL, 'ancient');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('eastern', 'department', 'Near Eastern Art', NULL, 'ancient');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('modern', 'department', 'Modern Art', NULL, 'arthistory');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('artbus', 'department', 'Business of Art', NULL, 'arthistory');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('literature', 'school', 'English and World Literature', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('englit', 'department', 'EnglishLanguage Literature', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('eurolit', 'department', 'European Literature in Translation', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('nonfiction', 'department', 'NonFiction Writing', NULL, 'literature');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('journalism', 'department', 'Journalistic Writing', NULL, 'literature');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('fiction', 'department', 'Fiction Writing', NULL, 'literature');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('poetry', 'department', 'Poetry Writing', NULL, 'literature');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('history', 'school', 'American and World History', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('amhistory', 'department', 'American History', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('eurohistory', 'department', 'European History', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('nonwesternhist', 'department', 'NonWestern History', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('dischistory', 'department', 'Interdisciplinary History', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('language', 'school', 'World Languages', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('modlanguage', 'department', 'Modern Languages', NULL, 'language');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('french', 'department', 'French', NULL, 'modlanguage');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('german', 'department', 'German', NULL, 'modlanguage');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('spanish', 'department', 'Spanish', NULL, 'modlanguage');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('anclanguage', 'department', 'Ancient Languages', NULL, 'language');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('linguistics', 'department', 'Linguistics', NULL, 'language');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('polisci', 'school', 'Political Science', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('government', 'department', 'Government', NULL, 'polisci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('intrelations', 'department', 'International Relations', NULL, 'polisci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('poliecon', 'department', 'Political Economy', NULL, 'polisci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('psychology', 'school', 'Psychology', NULL, 'humanities');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('cogpsych', 'department', 'Cognitive Science', NULL, 'psychology');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('behpsych', 'department', 'Behavioral Science', NULL, 'psychology');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('chipsych', 'department', 'Child Psychology and Development', NULL, 'psychology');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('science', 'university', 'Natural Sciences', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('astronomy', 'school', 'Astronomy', NULL, 'science');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('astrotheory', 'department', 'Astrophysics Theory', NULL, 'astronomy');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('astrolab', 'department', 'Astronomy Laboratory', NULL, 'astronomy');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('observation', 'department', 'Observing Skills', NULL, 'astronomy');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('chemistry', 'school', 'Chemistry', NULL, 'science');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('chemtheory', 'department', 'Theoretical Chemistry', NULL, 'chemistry');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('chemlab', 'department', 'Chemistry Laboratory', NULL, 'chemistry');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('chemcomputation', 'department', 'Algorithms and Data Visualization for Chemists', NULL, 'chemistry');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('physics', 'school', 'Physics', NULL, 'science');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('phystheory', 'department', 'Theoretical Physics', NULL, 'physics');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('physlab', 'department', 'Practical Physics', NULL, 'physics');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('physcomputer', 'department', 'Computer Languages for Physics', NULL, 'physics');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('math', 'school', 'Mathematics', NULL, 'science');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('analysis', 'department', 'Real and Complex Analysis', NULL, 'math');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('algebra', 'department', 'Abstract Algebra', NULL, 'math');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('statistics', 'department', 'Probability and Statistics', NULL, 'math');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('artdesign', 'university', 'Art and Design', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('studio', 'school', 'Studio Arts', NULL, 'artdesign');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('artstudio', 'school', 'Studio Art', NULL, 'artdesign');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('drawing', 'department', 'Drawing', NULL, 'artstudio');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('sculpture', 'department', 'Sculpture', NULL, 'artstudio');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('printmaking', 'department', 'Printmaking', NULL, 'artstudio');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('industrial', 'school', 'Industrial Design', NULL, 'artdesign');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('digital', 'school', 'Digital Media', NULL, 'artdesign');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('society', 'school', 'Art and Society', NULL, 'artdesign');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('engineering', 'university', 'Engineering', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('compsci', 'school', 'Computer Science', NULL, 'engineering');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('comptheory', 'department', 'Computationial Science', NULL, 'compsci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('softeng', 'department', 'Software Engineering', NULL, 'compsci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('compai', 'department', 'Artificial Intelligence', NULL, 'compsci');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('ee', 'school', 'Electrical Engineering', NULL, 'engineering');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('eetheory', 'department', 'Electrical Engineering Theory', NULL, 'ee');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('eeconcentration', 'department', 'EE Concentrations', NULL, 'ee');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('signal', 'department', 'Signal Processing', NULL, 'eeconcentration');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('power', 'department', 'Power Electronics', NULL, 'eeconcentration');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('eecom', 'department', 'Communications', NULL, 'eeconcentration');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('eenetworking', 'department', 'Electrical Networking', NULL, 'eeconcentration');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('me', 'school', 'Mechanical Engineering', NULL, 'engineering');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('memechanics', 'department', 'Mechanics', NULL, 'me');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('medesign', 'department', 'Design and Manufacturing', NULL, 'me');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('mesystems', 'department', 'Systems and Controls', NULL, 'me');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('be', 'school', 'Biomedical Engineering', NULL, 'engineering');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('begeneral', 'department', 'General Biomedical Engineering', NULL, 'be');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('beclinical', 'department', 'Clinical Engineering', NULL, 'be');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('nanotech', 'department', 'Nanotechnology', NULL, 'be');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('biomaterials', 'department', 'Biomaterials', NULL, 'be');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('education', 'university', 'Education', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('teached', 'school', 'Teacher Education', NULL, 'education');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('methods', 'department', 'Teaching Methods', NULL, 'teached');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('edmanagement', 'department', 'Education Management', NULL, 'teached');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('earlyed', 'department', 'Early Education', NULL, 'teached');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('edpol', 'school', 'Educational Policy', NULL, 'education');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('edstudies', 'department', 'Policy Studies', NULL, 'edpol');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('edresearch', 'department', 'Educational Research', NULL, 'edpol');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('business', 'university', 'Business', NULL, NULL);
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('ethics', 'school', 'Business Ethics', NULL, 'business');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('financial', 'school', 'Financial Analysis', NULL, 'financial');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('accounting', 'department', 'Accounting', NULL, 'financial');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('investment', 'department', 'Investment', NULL, 'financial');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('personal', 'department', 'Personal Investment', NULL, 'investment');
INSERT INTO classification (code, type, title, description, part_of) VALUES
          ('institutional', 'department', 'Institutional Investment', NULL, 'investment');
INSERT INTO classification (code, type, title, description, part_of) VALUES
        ('markets', 'school', 'Capital Markets', NULL, 'financial');
INSERT INTO classification (code, type, title, description, part_of) VALUES
      ('management', 'school', 'Management', NULL, 'business');
INSERT INTO classification (code, type, title, description, part_of) VALUES
    ('remedial', 'university', 'Remedial Courses', 'Classes for which credit is not typically given for degree programs in the same school; e.g.  College Algebra courses do not earn credit for those in the School of Natural Science.', NULL);

INSERT INTO course_classification (department, course, classification) VALUES
    ('astro', 137, 'astronomy');
INSERT INTO course_classification (department, course, classification) VALUES
    ('astro', 142, 'astrolab');
INSERT INTO course_classification (department, course, classification) VALUES
    ('astro', 155, 'observation');
INSERT INTO course_classification (department, course, classification) VALUES
    ('astro', 254, 'astrotheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('mth', 101, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('chem', 100, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('chem', 110, 'science');
INSERT INTO course_classification (department, course, classification) VALUES
    ('chem', 115, 'chemlab');
INSERT INTO course_classification (department, course, classification) VALUES
    ('chem', 655, 'chemtheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 211, 'science');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 303, 'phystheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 388, 'physlab');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 388, 'reasoning');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 512, 'phystheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 713, 'phystheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('phys', 819, 'phystheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 101, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 247, 'eurolit');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 311, 'nonfiction');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 175, 'journalism');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 175, 'writing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 276, 'nonfiction');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 276, 'writing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 412, 'nonfiction');
INSERT INTO course_classification (department, course, classification) VALUES
    ('eng', 412, 'writing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 112, 'amhistory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 212, 'amhistory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 415, 'dischistory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 415, 'diversity');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 415, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('hist', 333, 'dischistory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('lang', 201, 'modlanguage');
INSERT INTO course_classification (department, course, classification) VALUES
    ('lang', 203, 'modlanguage');
INSERT INTO course_classification (department, course, classification) VALUES
    ('lang', 207, 'linguistics');
INSERT INTO course_classification (department, course, classification) VALUES
    ('lang', 207, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('lang', 305, 'linguistics');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 102, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 304, 'cogpsych');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 304, 'reasoning');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 450, 'behpsych');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 560, 'cogpsych');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 560, 'compai');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 610, 'chipsych');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 610, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('psych', 610, 'research');
INSERT INTO course_classification (department, course, classification) VALUES
    ('poli', 113, 'government');
INSERT INTO course_classification (department, course, classification) VALUES
    ('poli', 402, 'government');
INSERT INTO course_classification (department, course, classification) VALUES
    ('poli', 644, 'intrelations');
INSERT INTO course_classification (department, course, classification) VALUES
    ('poli', 431, 'government');
INSERT INTO course_classification (department, course, classification) VALUES
    ('poli', 715, 'intrelations');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 102, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 230, 'comptheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 350, 'comptheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 615, 'compai');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 819, 'comptheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 710, 'softeng');
INSERT INTO course_classification (department, course, classification) VALUES
    ('comp', 810, 'softeng');
INSERT INTO course_classification (department, course, classification) VALUES
    ('ee', 107, 'eetheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('ee', 202, 'eetheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('ee', 412, 'eetheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('ee', 505, 'eetheory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('ee', 615, 'eecom');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 344, 'memechanics');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 111, 'memechanics');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 627, 'memechanics');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 501, 'mesystems');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 712, 'medesign');
INSERT INTO course_classification (department, course, classification) VALUES
    ('me', 712, 'industrial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('be', 112, 'begeneral');
INSERT INTO course_classification (department, course, classification) VALUES
    ('be', 308, 'begeneral');
INSERT INTO course_classification (department, course, classification) VALUES
    ('be', 415, 'beclinical');
INSERT INTO course_classification (department, course, classification) VALUES
    ('be', 509, 'beclinical');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 712, 'artbus');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 202, 'modern');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 712, 'management');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 340, 'eastern');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 710, 'modern');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 809, 'modern');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 623, 'modern');
INSERT INTO course_classification (department, course, classification) VALUES
    ('arthis', 623, 'diversity');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 714, 'drawing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 509, 'printmaking');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 411, 'sculpture');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 411, 'physlab');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 512, 'society');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 614, 'drawing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 333, 'drawing');
INSERT INTO course_classification (department, course, classification) VALUES
    ('stdart', 119, 'sculpture');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 155, 'methods');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 367, 'edmanagement');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 367, 'management');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 501, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 609, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('tched', 122, 'earlyed');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 202, 'edresearch');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 551, 'edresearch');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 313, 'edresearch');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 313, 'amhistory');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 617, 'edstudies');
INSERT INTO course_classification (department, course, classification) VALUES
    ('edpol', 505, 'edresearch');
INSERT INTO course_classification (department, course, classification) VALUES
    ('corpfi', 234, 'financial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('corpfi', 404, 'financial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('corpfi', 601, 'financial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 100, 'remedial');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 200, 'accounting');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 315, 'accounting');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 426, 'accounting');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 527, 'accounting');
INSERT INTO course_classification (department, course, classification) VALUES
    ('acc', 606, 'accounting');
INSERT INTO course_classification (department, course, classification) VALUES
    ('capmrk', 712, 'markets');
INSERT INTO course_classification (department, course, classification) VALUES
    ('capmrk', 808, 'institutional');
INSERT INTO course_classification (department, course, classification) VALUES
    ('capmrk', 818, 'institutional');
INSERT INTO course_classification (department, course, classification) VALUES
    ('capmrk', 756, 'markets');

INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'astrolab', 8, 'Astronomy students are expected to take a minimum of 8 credit hours in the astronomy laboratory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'observation', 12, 'Undergraduate astronomy students will take a minimum of 12 credit hours of observational astronomy.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'astrotheory', 24, 'Undergraduate astronomy students will take a minimum of 12 credit hours of coursework on astronomy theory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'reasoning', 12, 'Undergraduate science students will take a minimum of 12 credit hours in general reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'research', 12, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in research techniques.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 16 credit hours in the humanities.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uastro', 'physics', 9, 'Undergraduate astronomy students will take a minimum of 9 credit hours in physics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uchem', 'chemlab', 16, 'Undergraduate chemistry students must satisfy a minimum requirement for chemistry labwork.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uchem', 'chemtheory', 18, 'Undergraduate chemistry students must satisfy a minimum requirement for chemistry theory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uchem', 'reasoning', 12, 'Undergraduate science students will take a minimum of 12 credit hours in general reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uchem', 'research', 12, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in research techniques.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uchem', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 16 credit hours in the humanities.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uphys', 'phystheory', 26, 'Candidates for the B.S. in physics must take a minimum of 26 hours of physics theory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uphys', 'physlab', 12, 'Candidates for the B.S. in physics must take a minimum of 12 hours of physics labwork.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uphys', 'humanities', 16, 'B.S. candidates in the sciences will take a minimum of 12 credit hours in the humanities.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('ns', 'uphys', 'science', 12, 'Physics majors are expected to take a minimum of 12 credit hours in other scientific disciplines.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upsych', 'psychology', 24, 'Psychology majors must take the minimum credit hours in one or more of the three major psychology concentrationscognitive, behavioral, or child.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upsych', 'writing', 12, 'Psychology majors must take a minimum of 12 credit hours in writing.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upsych', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upsych', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'government', 16, 'Political Science majors will take at least 16 credit hours of coursework in world government');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'intrelations', 12, 'Political Science majors will take at least 12 credit hours of coursework in international relations.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'poliecon', 12, 'Political Science majors will take at least 12 credit hours of coursework in political economy.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'writing', 12, 'Political science majors must take a minimum of 12 credit hours in writing.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'upolisci', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'gscitch', 'science', 18, 'M.S. candidates for the science teaching degree who do not have undergraduate degrees in science must take a minimum of 18 credit hours in one scientific discipline.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'gscitch', 'methods', 18, 'M.S. candidates should complete a minimum of 18 credit hours focused on teaching methods.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'gscitch', 'earlyed', 12, 'M.S. candidates will take a minimum of 12 hours of early childhood education instruction.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'psciwri', 'science', 12, 'Candidates for the Certificate in Science Writing will take four threehour classes in general science.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'psciwri', 'journalism', 12, 'Candidates will take four threehour classes in writing focused on scientific journalism');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'gengl', 'literature', 36, 'Candidates for the M.A. in English will take a minimum of 36 credit hours of literature courses.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uengl', 'literature', 30, 'B.A. candidates in English are expected to take 30 credit hours in general literature.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uengl', 'eurolit', 12, 'B.A. candidates in English are expected to take three courses in European literature.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uengl', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uengl', 'humanities', 16, 'B.A. candidates in the humanities will take a minimum of 16 credit hours in general humanities outside their major.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uengl', 'modlanguage', 16, 'B.A. candidates in the humanities will take a minimum of two years of a modern language of their choice.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uhist', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uspan', 'spanish', 24, 'Spanish majors will take a minimum of 24 credit hours in the Spanish majors.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uspan', 'reasoning', 6, 'B.A. candidates in the humanities will take a minimum of two courses emphasizing reasoning.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uspan', 'humanities', 16, 'B.A. candidates in the humanities will take a minimum of 16 credit hours in general humanities outside their major.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uspan', 'modlanguage', 16, 'B.A. candidates in the humanities will take a minimum of two years of a modern language of their choice.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'uspan', 'diversity', 16, 'In recognition of the importance of diversity in modern society, undergraduate humanities majors must take a minimum of 16 credits hours of course emphasizing cultural diversity.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('la', 'glang', 'language', 22, 'Candidates for the Master of Arts in Modern Languages must take a minimum of 22 credit hours in the modern language of their concentration.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'uelec', 'eetheory', 18, 'Bachelor of Engineering candidates in Electrical Engineering are expected to take at least 18 hours of credit in EE theory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'uelec', 'eeconcentration', 22, 'Bachelor of Engineering candidates in Electrical Engineering are expected to take at least 22 hours of credit in their area of concentration.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'uelec', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'umech', 'me', 22, 'Bachelor of Engineering candidates in Mechanical Engineering are expected to take at least 22 hours of credit in their area of concentrationmechanics, de sign, or systems.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'umech', 'mesystems', 9, 'Bachelor of Engineering candidates in Mechanical Engineering are expected to take at least 9 hours of credit in systems, regardless of their area of concentration.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'umech', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'umech', 'humanities', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in general humanities.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'ubio', 'be', 24, 'Bachelor of Engineering candidates in Bioengineering are expected to take at least 24 hours of credit in their area of concentrationclinical, nanotech, orbiomaterials');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'ubio', 'mesystems', 10, 'Bachelor of Engineering candidates in Bioengineering are expected to take at least 9 hours of credit in biomaterials, regardless of their area of concentration.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'ubio', 'compsci', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in computer science and/or programming related to their major.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'ubio', 'humanities', 12, 'Bachelor of Engineering candidates are expected to take at least 12 hours of credit in general humanities.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'gbuseng', 'business', 16, 'Candidates for the Master of Science in Business and Engineering are required to take at least 16 credit hours in general business.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'gbuseng', 'engineering', 22, 'Candidates for the Master of Science in Business and Engineering are required to take at least 22 credit hours in one or more relevant engineering disciplines.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'gee', 'ee', 34, 'Candidates for the Master of Science in Electrical Engineering must take at least 34 credit hours in graduate electrical engineering.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'gme', 'ee', 36, 'Candidates for the Master of Science in Electrical Engineering must take at least 36 credit hours in graduate electrical engineering.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('eng', 'gbe', 'ee', 38, 'Candidates for the Master of Science in Electrical Engineering must take at least 38 credit hours in graduate electrical engineering.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'umath', 'teached', 20, 'Bachelor of Arts students in Math Education must take at least 20 credit hours of general teacher education.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'umath', 'math', 16, 'Bachelor of Arts students in Science Education must take at least 16 hours of general math.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'umath', 'diversity', 8, 'In acknowledgement of the importance of diversity in education, all candidates for education degrees must take at least 8 credit hours in diverse cultures and history.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'usci', 'teached', 22, 'Bachelor of Arts students in Science Education must take at least 22 credit hours of general science education.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'usci', 'science', 14, 'Bachelor of Arts students in Science Education must take at least 14 hours of general science.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'usci', 'diversity', 8, 'In acknowledgement of the importance of diversity in education, all candidates for education degrees must take at least 8 credit hours in diverse cultures and history.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'psci', 'methods', 12, 'Candidates for the Certificate in Science Teaching are required to take a minimum number of credit hours in teaching methods.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'psci', 'science', 12, 'Candidates for the Certificate in Science Teaching are required to take a minimum number of credit hours in general science.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'glited', 'teached', 28, 'Candidates for the Master of Arts in Literacy Education will take the majority of their credit hours in teacher education, focusing on literacy.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'gedlead', 'edpol', 20, 'Candidates for the Master of Arts in Educational Leadership will concentrate in educational policy.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'gedlead', 'management', 8, 'Candidates for the Master of Arts in Educational Leadership will take a minimum number of credit hours in management at the School of Business.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'gedu', 'edresearch', 22, 'Candidates for the Master of Science in Education will focus on a core requirement of educationrelated research leading up to the master''s thesis.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('edu', 'gtch', 'teached', 28, 'Candidates for the Master of Arts in Teaching will concentrate on the study of teaching methods in their chosen concentration.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'uacct', 'accounting', 24, 'Students pursuing the B.S. in Accounting will take the majority of their credit hours in accounting.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'uacct', 'investment', 8, 'Students pursuing the B.S. in Accounting must take at least 8 credit hours in general investment topics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'uacct', 'analysis', 8, 'Students pursuing the B.S. in Accounting must take at least 8 credit hours of relevant mathematics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'uacct', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ucorpfi', 'financial', 18, 'Students pursuing the B.S. in Corporate Finance will take the majority of their credit hours in general financial classes.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ucorpfi', 'accounting', 10, 'Students pursuing the B.S. in Corporate Finance must take a minimum of 10 credit hours in accounting.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ucorpfi', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ucorpfi', 'management', 6, 'Students pursuing the B.S. in Corporate Finance must take a minimum number of credits in corporate management.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ubusad', 'management', 24, 'B.S. students in Business Administration will focus on corporate management.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'ubusad', 'ethics', 12, 'Students pursuing any undergraduate degree in business will be required to meet a core requirement in business ethics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'pacc', 'accounting', 20, 'Requirements for the Graduate Certificate in Accounting require a minimum number of credit hours in accounting and accounting theory.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('bus', 'pcap', 'markets', 20, 'Requirements for the Certificate in Capital Markets include completion of a minimum number of credit hours in markets topics.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'gart', 'arthistory', 20, 'Candidates for the Post Baccalaureate in Art History must complete a minimum number of core art history courses.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'uhist', 'classical', 9, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of classical art.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'uhist', 'modern', 9, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of modern art.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'uhist', 'eastern', 6, 'Students in the undergraduate Art History program are required to take 9 credit hours of study of eastern art.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'uhist', 'arthistory', 14, 'Students in the undergraduate Art History program are required to take 9 credit hours of elective classes in art history.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'ustudio', 'artstudio', 24, 'Students in the undergraduate Studio Art program will concentrate on their selected studio discipline.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'ustudio', 'drawing', 8, 'All Studio Art undergraduate students must take a minimum 8 hours of credit in freehand drawing.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'ustudio', 'society', 6, 'All Studio Art undergraduate students must take a minimum of 6 hours of credit in Art & Society.');
INSERT INTO program_requirement (school, program, classification, credit_hours, rationale) VALUES
    ('art', 'ustudio', 'digital', 6, 'All Studio Art undergraduate students must take a minimum of 6 hours of credit in Digital Art.');
