--
-- Copyright (c) 2006-2010, Prometheus Research, LLC
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
    school      VARCHAR(16) NOT NULL,
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
    CONSTRAINT program_pk
      PRIMARY KEY (school, code),
    CONSTRAINT program_title_uk
      UNIQUE (title),
    CONSTRAINT program_degree_ck
      CHECK (degree IN ('bs', 'pb', 'ma', 'ba', 'ct', 'ms')),
    CONSTRAINT program_school_fk
      FOREIGN KEY (school)
      REFERENCES school(code)
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
    ('la', 'School of Arts, Letters, and the Humanities');
INSERT INTO school (code, name) VALUES 
    ('egn', 'School of Engineering');
INSERT INTO school (code, name) VALUES 
    ('art', 'School of Art and Design');
INSERT INTO school (code, name) VALUES 
    ('edu', 'College of Education');
INSERT INTO school (code, name) VALUES 
    ('bus', 'School of Business');
INSERT INTO school (code, name) VALUES 
    ('mart', 'School of Modern Art');
INSERT INTO school (code, name) VALUES 
    ('mus', 'Musical School');

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
    ('comp', 'Computer Science', 'egn');
INSERT INTO department (code, name, school) VALUES
    ('ee', 'Electrical Engineering', 'egn');
INSERT INTO department (code, name, school) VALUES
    ('me', 'Mechanical Engineering', 'egn');
INSERT INTO department (code, name, school) VALUES
    ('be', 'Bioengineering', 'egn');
INSERT INTO department (code, name, school) VALUES
    ('arthis', 'Art History', 'art');
INSERT INTO department (code, name, school) VALUES
    ('artstd', 'Studio Art', 'art');
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

INSERT INTO program (school, code, title, degree) VALUES
    ('ns', 'uastro', 'Bachelor of Science in Astronomy', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('ns', 'uchem', 'Bachelor of Science in Chemistry', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('ns', 'uphys', 'Bachelor of Science in Physics', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'upsych', 'Bachelor of Arts in Psychology', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'upolisci', 'Bachelor of Arts in Political Science', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'gscitch', 'Master of Arts in Science Teaching', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'psciwri', 'Science Writing', 'ct');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'gengl', 'Master of Arts in English', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'uengl', 'Bachelor of Arts in English', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'uhist', 'Bachelor of Arts in History', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'uspan', 'Bachelor of Arts in Spanish', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('la', 'glang', 'Master of Arts in Modern Languages', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'uelec', 'Bachelor of Science in Electrical Engineering', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'umech', 'Bachelor of Science in Mechanical Engineering', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'ubio', 'Bachelor of Science in Bioengineering', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'ucompsci', 'Bachelor of Science in Computer Science', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'gbuseng', 'Master of Science in Business and Engineering', 'ms');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'gee', 'Master of Science in Electrical Engineering', 'ms');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'gme', 'Master of Science in Mechanical Engineering', 'ms');
INSERT INTO program (school, code, title, degree) VALUES
    ('egn', 'gbe', 'Master of Science in Bioengineering', 'ms');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'umath', 'Bachelor of Arts in Math Education', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'usci', 'Bachelor of Arts in Science Education', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'psci', 'Certificate in Science Teaching', 'ct');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'glited', 'Master of Arts in Literacy Education', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'gedlead', 'Master of Arts in Educational Leadership', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'gedu', 'Master of Science in Education', 'ms');
INSERT INTO program (school, code, title, degree) VALUES
    ('edu', 'gtch', 'Master of Arts in Teaching', 'ma');
INSERT INTO program (school, code, title, degree) VALUES
    ('bus', 'uacct', 'Bachelor of Science in Accounting', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('bus', 'ucorpfi', 'Bachelor of Science in Corporate Finance', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('bus', 'ubusad', 'Bachelor of Science in Business Administration', 'bs');
INSERT INTO program (school, code, title, degree) VALUES
    ('bus', 'pacc', 'Graduate Certificate in Accounting', 'ct');
INSERT INTO program (school, code, title, degree) VALUES
    ('bus', 'pcap', 'Certificate in Capital Markets', 'ct');
INSERT INTO program (school, code, title, degree) VALUES
    ('art', 'gart', 'Post Baccalaureate in Art History', 'pb');
INSERT INTO program (school, code, title, degree) VALUES
    ('art', 'uhist', 'Bachelor of Arts in Art History', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('art', 'ustudio', 'Bachelor of Arts in Studio Art', 'ba');
INSERT INTO program (school, code, title, degree) VALUES
    ('mart', 'bmart', 'Bachelor of Modern Art', 'ba');

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
    ('me', 627, 'Advanced Heating and Air Conditioning', 4, 'Open to juniors and seniors. Requires permission of instructor.');
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
    ('artstd', 714, 'Peer Portfolio Review', 0, 'An opportunity to practice giving and receiving constructive criticism.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('artstd', 411, 'Underwater Basket Weaving', 4, 'This course provides a novel perspective on the traditional art of basketry as it is experienced in reduced gravity and in the context of fluid dynamics. Requires instructor permission and a valid c-card.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('artstd', 512, 'Art in Therapy', 3, 'Surveys methods and results of using art and craft therapy with developmentally disabled adults.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('artstd', 614, 'Drawing Master Class', 5, 'For fine arts majors only, an intensive studio study including field trips to local parks and museums and a final group art show.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('artstd', 509, 'Twentieth Century Printmaking', 4, 'Development of personalized concepts and individual aesthetic expression in printmaking with reference to various styles and trends in Twentieth Century printmaking.');
INSERT INTO course (department, number, title, credits, description) VALUES
    ('artstd', 333, 'Drawing', 3, 'Exploration of the structure and interrelationships of visual form in drawing, painting, and sculpture. Principal historical modes of drawing are examined.');
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
    ('artstd', 119, 'Spring Basket Weaving Workshop', NULL, 'A just-for-fun chance to learn the basics of basket weaving.');
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
    ('acc', 100, 'Practical Bookkeeping', 2, 'A introduction to business with practical bookkeeping application.');
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


-- TODO: port the remaining schemata.


