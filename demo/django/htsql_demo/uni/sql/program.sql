--  ALTER TABLE uni_program 
--        ADD CONSTRAINT uni_program_primary_uk
--        UNIQUE(school, code);
--  ALTER TABLE uni_program 
--        ADD CONSTRAINT uni_program_part_of_fk
--        FOREIGN KEY (school, part_of ) 
--        REFERENCES uni_program(school, code);

INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'uastro', 'Bachelor of Science in Astronomy', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'uchem', 'Bachelor of Science in Chemistry', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'uphys', 'Bachelor of Science in Physics', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'pmth', 'Doctorate of Science in Mathematics', 'ph', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'gmth', 'Masters of Science in Mathematics', 'bs', 'pmth');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ns', 'umth', 'Bachelor of Science in Mathematics', 'bs', 'gmth');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'upsych', 'Bachelor of Arts in Psychology', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'upolisci', 'Bachelor of Arts in Political Science', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'gscitch', 'Master of Arts in Science Teaching', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'psciwri', 'Science Writing', 'ct', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'gengl', 'Master of Arts in English', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'uengl', 'Bachelor of Arts in English', 'ba', 'gengl');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'uhist', 'Bachelor of Arts in History', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'uspan', 'Bachelor of Arts in Spanish', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('la', 'glang', 'Master of Arts in Modern Languages', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'gbuseng', 'M.S. in Business and Engineering', 'ms', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'gee', 'M.S. in Electrical Engineering', 'ms', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'gme', 'M.S. in Mechanical Engineering', 'ms', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'gbe', 'M.S. in Bioengineering', 'ms', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'uelec', 'B.S. in Electrical Engineering', 'bs', 'gee');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'umech', 'B.S. in Mechanical Engineering', 'bs', 'gme');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'ubio', 'B.S. in Bioengineering', 'bs', 'gbe');
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('eng', 'ucompsci', 'B.S. in Computer Science', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'umath', 'Bachelor of Arts in Math Education', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'usci', 'Bachelor of Arts in Science Education', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'psci', 'Certificate in Science Teaching', 'ct', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'glited', 'Master of Arts in Literacy Education', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'gedlead', 'Master of Arts in Education Leadership', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'gedu', 'M.S. in Education', 'ms', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('edu', 'gtch', 'Master of Arts in Teaching', 'ma', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('bus', 'uacct', 'B.S. in Accounting', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('bus', 'ucorpfi', 'B.S. in Corporate Finance', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('bus', 'ubusad', 'B.S. in Business Administration', 'bs', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('bus', 'pacc', 'Graduate Certificate in Accounting', 'ct', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('bus', 'pcap', 'Certificate in Capital Markets', 'ct', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('art', 'gart', 'Post Baccalaureate in Art History', 'pb', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('art', 'uhist', 'Bachelor of Arts in Art History', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('art', 'ustudio', 'Bachelor of Arts in Studio Art', 'ba', NULL);
INSERT INTO uni_program (school, code, title, degree, part_of) VALUES
    ('ph', 'phd', 'Honorary PhD', NULL, NULL);

