
import random, csv, datetime, math
import os, sys

RANDOM_SEED = 0
INSTRUCTOR_NAME_SEED = 100
STUDENT_NAME_SEED = 200

CURDATE = datetime.datetime.strptime('2011-06-01', '%Y-%m-%d').date()


class NameGenerator(object):

    def generate_male(self):
        pass

    def generate_female(self):
        pass

    def generate(self, gender):
        if gender == 'm':
            return self.generate_male()
        else:
            return self.generate_female()

class StatNameData(object):

    MAN_NAMES_FILE = "dist.male.first"      # male first name statistics file
    WOMAN_NAMES_FILE = "dist.female.first"  # female first name statistics file
    LAST_NAMES_FILE = "dist.all.last"       # last name statistics file

    def __init__(self):
        basedir = os.path.dirname(__file__)
        fname = os.path.join(basedir, self.MAN_NAMES_FILE)
        (self.man_names, self.man_total) = self.load_names(fname)
        fname = os.path.join(basedir, self.WOMAN_NAMES_FILE)
        (self.woman_names, self.woman_total) = self.load_names(fname)
        fname = os.path.join(basedir, self.LAST_NAMES_FILE)
        (self.last_names, self.last_total) = self.load_names(fname)

    def load_names(self, file_name):
        names = []
        dialect = csv.Dialect
        dialect.skipinitialspace = True
        dialect.quoting = csv.QUOTE_MINIMAL
        dialect.quotechar = '"'
        dialect.lineterminator = '\r\n'
        fileReader = csv.reader(open(file_name), dialect, delimiter=' ')
        for row in fileReader:
            names.append( { "name": row[0], "bound": float(row[2]) } )
        total = names[len(names) - 1]["bound"]
        return names, total


class StatNameGenerator(NameGenerator):

    def __init__(self, name_data, seed=0):
        self.name_data = name_data
        self.rg = random.Random(seed)

    def binary_search(self, names, value, high, low):
        if high == low or (high - low == 1):
            return names[high]["name"]
        shift = int(low + round(float(high - low)/2.0))
        shift_bound = names[shift]["bound"]
        if value >= shift_bound:
            return self.binary_search(names, value, high, shift)
        else:
            return self.binary_search(names, value, shift, low)

    def get_random_name(self, names, total):
        value = self.rg.uniform(0, float(total))
        return self.binary_search(names, value, len(names) - 1, 0)

    def generate_female(self):
        first = self.get_random_name(self.name_data.woman_names,
                                     self.name_data.woman_total).capitalize()
        last = self.get_random_name(self.name_data.last_names,
                                    self.name_data.last_total).capitalize()
        return first + " " + last

    def generate_male(self):
        first = self.get_random_name(self.name_data.man_names,
                                     self.name_data.man_total).capitalize()
        last = self.get_random_name(self.name_data.last_names,
                                    self.name_data.last_total).capitalize()
        return first + " " + last


class Dictionary(object):

    def __init__(self):
        self.departments = {}           # map dep_code -> course_count
        self.semesters = []             # list of tuples (year, season, begin_date, end_date)
        self.school_programs = {}       # map school_code -> list of program_code
        self.program_req = {}
        self.classification_tree = {}
        self.courses = {}
        self.course_prereq = {}
        self.classified_courses = {}
        self.course_classification = {}


class CollectionDictionaryLoader(object):

    def __init__(self):
        self.d = Dictionary()

    def dequote(self, val):
        if val.startswith("'") and val.endswith("'"):
            return val[1:-1]
        return val

    def get(self, columns, record, colname):
        idx = columns.index(colname)
        val = record[idx]
        if val == 'null':
            return None
        if isinstance(val, (str, unicode)):
            return self.dequote(val)
        return val

    def get_date(self, columns, record, colname):
        val = self.get(columns, record, colname)
        if val is not None and isinstance(val, (str, unicode)):
            return datetime.datetime.strptime(val, '%Y-%m-%d').date()
        return val

    def load(self, content):
        for table_content in content:
            table_name = table_content['table']
            columns = table_content['columns']
            data = table_content['data']
            if table_name == 'ad.program':
                self.load_school_programs(columns, data)
            elif table_name == 'ad.course':
                self.load_courses(columns, data)
            elif table_name == 'cd.semester':
                self.load_semesters(columns, data)
            elif table_name == 'rd.prerequisite':
                self.load_course_prereq(columns, data)
            elif table_name == 'rd.classification':
                self.load_classification(columns, data)
            elif table_name == 'rd.course_classification':
                self.load_classified_courses(columns, data)
            elif table_name == 'rd.program_requirement':
                self.load_program_req(columns, data)
        return self.d

    def load_school_programs(self, columns, data):
        for record in data:
            school = self.get(columns, record, 'school_code')
            program = self.get(columns, record, 'code')
            if school not in self.d.school_programs:
                self.d.school_programs[school] = []
            self.d.school_programs[school].append(program)

    def load_courses(self, columns, data):
        for record in data:
            dep = self.get(columns, record, 'department_code')
            course = self.get(columns, record, 'no')
            credits = self.get(columns, record, 'credits')
            if credits is None:
                credits = 0
            self.d.courses[(dep, course)] = credits
            # count courses per department
            if dep not in self.d.departments:
                self.d.departments[dep] = 1
            else:
                self.d.departments[dep] = self.d.departments[dep] + 1

    def load_semesters(self, columns, data):
        for record in data:
            self.d.semesters.append({
                'year': self.get(columns, record, 'year'),
                'season': self.get(columns, record, 'season'),
                'begin_date': self.get_date(columns, record, 'begin_date'),
                'end_date': self.get_date(columns, record, 'end_date')
                })
        self.d.semesters = sorted(self.d.semesters, key=lambda x: x['begin_date'])

    def load_classification(self, columns, data):
        for record in data:
            code = self.get(columns, record, 'code')
            part_of = self.get(columns, record, 'part_of')
            if code not in self.d.classification_tree:
                self.d.classification_tree[code] = []
            if part_of not in self.d.classification_tree:
                self.d.classification_tree[part_of] = []
            self.d.classification_tree[part_of].append(code)

    def load_program_req(self, columns, data):
        for record in data:
            school = self.get(columns, record, 'school_code')
            program = self.get(columns, record, 'program_code')
            key = (school, program)
            req = self.get(columns, record, 'classification_code')
            credits = self.get(columns, record, 'credit_hours')
            if key not in self.d.program_req:
                self.d.program_req[key] = {}
            map = self.d.program_req[key]
            map[req] = credits

    def load_course_prereq(self, columns, data):
        for record in data:
            of_dep = self.get(columns, record, 'of_department_code')
            of_course = self.get(columns, record, 'of_course_no')
            on_dep = self.get(columns, record, 'on_department_code')
            on_course = self.get(columns, record, 'on_course_no')
            key = (of_dep, of_course)
            value = (on_dep, on_course)
            if key not in self.d.course_prereq:
                self.d.course_prereq[key] = []
            self.d.course_prereq[key].append(value)

    def load_classified_courses(self, columns, data):
        for record in data:
            dep_code = self.get(columns, record, 'department_code')
            course_no = self.get(columns, record, 'course_no')
            course_key = (dep_code, course_no)
            classif = self.get(columns, record, 'classification_code')
            if classif not in self.d.classified_courses:
                self.d.classified_courses[classif] = []
            self.d.classified_courses[classif].append(course_key)
            if course_key not in self.d.course_classification:
                self.d.course_classification[course_key] = []
            self.d.course_classification[course_key].append(classif)


class DbDictionaryLoader(object):

    SELECT_DEPARTMENTS = 'SELECT department_code, count(*) FROM ad.course GROUP BY department_code'
    SELECT_SEMESTERS = 'SELECT year, season, begin_date, end_date FROM cd.semester'
    SELECT_SCHOOL_PROGRAMS = 'SELECT s.code, p.code FROM ad.school s INNER JOIN ad.program p ON p.school_code = s.code'
    SELECT_CLASSIFICATION = 'SELECT code, part_of FROM rd.classification'
    SELECT_PROGRAM_REQ = 'SELECT school_code, program_code, classification_code, credit_hours FROM rd.program_requirement'
    SELECT_COURSE_PREREQ = 'SELECT of_department_code, of_course_no, on_department_code, on_course_no FROM rd.prerequisite'
    SELECT_COURSES = 'SELECT department_code, no, credits FROM ad.course'
    SELECT_CLASSIFIED_COURSES = 'SELECT department_code, course_no, classification_code FROM rd.course_classification'

    # TBD

class BaseDataGenerator(object):

    """Generic parameters"""
    NULL_PERCENT = 5                        # percent of nulls in nullable columns
    MAN_WOMAN_BORDER = 40                   # percent of men

    def quote(self, val):
        if val is None:
            return 'null'
        if isinstance(val, str):
            return "'" + val + "'"
        if isinstance(val, (datetime.datetime, datetime.date)):
            return "'" + str(val) + "'"
        return str(val)

    def get(self, columns, record, colname):
        idx = columns.index(colname)
        return record[idx]

    def make_unique_code(self, map, code):
        if code not in map:
            return code
        i = 1
        while (code + str(i)) in map:
            i += 1
        return (code + str(i))

    def generate_gender(self, gen):
        if gen.randint(1,100) <= self.MAN_WOMAN_BORDER:
            return "m"
        return "f"

    def get_rand_item(self, arr, gen):
        return arr[gen.randint(0, len(arr) - 1)]

    def generate_date(self, start, end, gen):
        delta = end - start
        return start + datetime.timedelta(gen.randint(1, delta.days))

    def make_insert_map(self, table, columns, data):
        return {
            "table": table,
            "columns": columns,
            "data": data
        }

    def generate_content(self):
        pass

    def get_content(self):
        pass


class InstructorGenerator(BaseDataGenerator):

    """Generate instructors"""
    INSTRUCTOR_TITLES = ['mr', 'dr', 'prof', 'ms']
    HALF_TIME_PERCENT = 15
    COURSES_PER_INSTRUCTOR = (2, 4)

    INSTRUCTOR_TABLE = "id.instructor"
    CONFIDENTIAL_TABLE = "id.confidential"
    APPOINTMENT_TABLE = "id.appointment"
    INSTRUCTOR_COLUMNS = ["code", "full_name", "title", "phone", "email"]
    CONFIDENTIAL_COLUMNS = ["instructor_code", "SSN", "pay_grade", "home_phone"]
    APPOINTMENT_COLUMNS = ["instructor_code", "department_code", "fraction"]

    def __init__(self, name_generator, dictionary):
        self.name_generator = name_generator
        self.dictionary = dictionary
        self.instructors = set()
        self.dep_app = {}
        self.instructor_data = []
        self.confidential_data = []
        self.appointment_data = []
        self.gender_gen = random.Random(RANDOM_SEED)
        self.title_gen = random.Random(RANDOM_SEED)
        self.phone_gen = random.Random(RANDOM_SEED)
        self.email_gen = random.Random(RANDOM_SEED)
        self.ssn_gen = random.Random(RANDOM_SEED)
        self.paygrade_gen = random.Random(RANDOM_SEED)
        self.fraction_gen = random.Random(RANDOM_SEED)

    def generate_phone(self):
        if self.phone_gen.randint(1,100) <= self.NULL_PERCENT:
            return None
        # 7-digit phone
        tel = str(self.phone_gen.randint(1000000, 9999999))
        return tel[:3] + '-' + tel[3:]

    def generate_email(self, code):
        if self.email_gen.randint(1,100) <= self.NULL_PERCENT:
            return None
        return code + '@example.com'

    def generate_code(self, full_name):
        names = full_name.split(' ')
        code = ''
        for i in range(0, len(names) - 1):
            code += names[i][0]
        code = (code + names[len(names) - 1]).lower()
        return self.make_unique_code(self.instructors, code)

    def generate_ssn(self):
        # 9-digit ssn
        ssn = str(self.ssn_gen.randint(100000000, 999999999))
        return ssn[:3] + '-' + ssn[3:5] + '-' + ssn[5:]

    def generate_home_phone(self):
        phone = self.generate_phone()
        if phone is not None:
            return '702-' + phone
        return None

    def generate_instructor(self):
        gender = self.generate_gender(self.gender_gen)
        full_name = self.name_generator.generate(gender)
        code = self.generate_code(full_name)
        title = self.get_rand_item(self.INSTRUCTOR_TITLES, self.title_gen)
        while (title == 'mr' and gender == 'f') \
                or (title == 'ms' and gender == 'm'):
            title = self.get_rand_item(self.INSTRUCTOR_TITLES, self.title_gen)
        record = [
            code,
            full_name,
            title,
            self.generate_phone(),
            self.generate_email(code)
        ]
        return (code, record)

    def generate_confidential(self, instructor_code):
        return [
            instructor_code,
            self.generate_ssn(),
            self.paygrade_gen.randint(4,8),
            self.generate_home_phone()
        ]

    def generate_appointment(self, instructor_code, depcode):
        fraction = None
        if self.fraction_gen.randint(1,100) > self.NULL_PERCENT:
            if self.fraction_gen.randint(1,100) <= self.HALF_TIME_PERCENT:
                fraction = 0.50
            else:
                fraction = 1.00
        return [
            instructor_code,
            depcode,
            fraction
        ]

    def generate_content(self):
        load_gen = random.Random(RANDOM_SEED)
        for depcode in sorted(self.dictionary.departments):
            load = load_gen.uniform(self.COURSES_PER_INSTRUCTOR[0], self.COURSES_PER_INSTRUCTOR[1])
            total_courses = self.dictionary.departments[depcode]
            count = int(math.ceil(total_courses / load))
            if depcode not in self.dep_app:
                self.dep_app[depcode] = []
            for i in range(0, count):
                (instructor_code, record) = self.generate_instructor()
                self.instructor_data.append(record)
                self.instructors.add(instructor_code)
                confidential = self.generate_confidential(instructor_code)
                self.confidential_data.append(confidential)
                appointment = self.generate_appointment(instructor_code, depcode)
                self.appointment_data.append(appointment)
                self.dep_app[depcode].append(appointment)

    def get_content(self):
        return [
            self.make_insert_map(self.INSTRUCTOR_TABLE, self.INSTRUCTOR_COLUMNS, self.instructor_data),
            self.make_insert_map(self.CONFIDENTIAL_TABLE, self.CONFIDENTIAL_COLUMNS, self.confidential_data),
            self.make_insert_map(self.APPOINTMENT_TABLE, self.APPOINTMENT_COLUMNS, self.appointment_data),
        ]


class ClassGenerator(BaseDataGenerator):

    CLASS_FILL_LIMIT = 40
    YEARLY_CLASS_PERCENT = 80
    MISSING_CLASS_PERCENT = 5
    NULL_INSTRUCTOR_PERCENT = 2

    CLASS_TABLE = 'cd.class'
    CLASS_COLUMNS = ['department_code', 'course_no', 'year', 'season', 'section', 'instructor_code', 'class_seq']
    CLASS_SEQ_OFFSET = 1000

    def __init__(self, meta, dep_app):
        self.dictionary = meta
        self.dep_app = dep_app
        self.class_data = []
        self.class_fill_map = {}
        self.class_seq = self.CLASS_SEQ_OFFSET
        self.instructor_gen = random.Random(RANDOM_SEED)
        self.course_gen = random.Random(RANDOM_SEED)
        self.class_gen = random.Random(RANDOM_SEED)

    def get_class_fill(self, class_seq):
        if class_seq in self.class_fill_map:
            return self.class_fill_map[class_seq]
        else:
            self.class_fill_map[class_seq] = 0
            return 0

    def take_class(self, class_seq):
        if class_seq in self.class_fill_map:
            self.class_fill_map[class_seq] += 1
        else:
            self.class_fill_map[class_seq] = 1

    def next_section(self, section):
        val = int(section)
        return '%03d' % (val + 1)

    def _is_empty_class(self, class_record):
        class_seq = self.get(self.CLASS_COLUMNS, class_record, 'class_seq')
        return class_seq not in self.class_fill_map

    def drop_empty_classes(self):
        self.class_data = [c for c in self.class_data if not self._is_empty_class(c)]

    def generate_class(self, inst_code, course_key, semester, section):
        # instructor is nullable ?!
        if self.instructor_gen.randint(1,100) <= self.NULL_INSTRUCTOR_PERCENT:
            inst = None
        else:
            inst = inst_code
        self.class_seq += 1
        clazz = [
            course_key[0],
            course_key[1],
            semester['year'],
            semester['season'],
            section,
            inst,
            self.class_seq
        ]
        self.class_data.append(clazz)
        return clazz

    def generate_content(self):
        for depcode in sorted(self.dep_app):
            dep_courses = []
            for course_key in sorted(self.dictionary.courses):
                if course_key[0] == depcode:
                    dep_courses.append(course_key)
            staff = 0
            for app in self.dep_app[depcode]:
                fraction = self.get(InstructorGenerator.APPOINTMENT_COLUMNS, app, 'fraction')
                if fraction is not None:
                    staff = staff + fraction
            if staff == 0:
                continue
            load = len(dep_courses)/staff
            for app in self.dep_app[depcode]:
                fraction = self.get(InstructorGenerator.APPOINTMENT_COLUMNS, app, 'fraction')
                inst_code = self.get(InstructorGenerator.APPOINTMENT_COLUMNS, app, 'instructor_code')
                course_count = 0
                if fraction is not None:
                    course_count = int(round(load/fraction))
                i = 0
                while i < course_count and len(dep_courses) > 0:
                    i += 1
                    course = self.get_rand_item(dep_courses, self.course_gen)
                    dep_courses.remove(course)
                    course_season = None
                    if self.class_gen.randint(0, 100) < self.YEARLY_CLASS_PERCENT:
                        if self.class_gen.randint(0, 100) < 50:
                            course_season = 'fall'
                        else:
                            course_season = 'spring'
                    for semester in self.dictionary.semesters:
                        if semester['begin_date'] < CURDATE and semester['season'] != 'summer':
                            if course_season is not None and course_season != semester['season']:
                                continue
                            if self.class_gen.randint(0, 100) < self.MISSING_CLASS_PERCENT:
                                continue
                            self.generate_class(inst_code, course, semester, '001')

    def get_content(self):
        return [
            self.make_insert_map(self.CLASS_TABLE, self.CLASS_COLUMNS, self.class_data)
        ]


class EnrollmentGenerator(BaseDataGenerator):

    CREDITS_PER_SEMESTER = (15, 25)
    """ Decrease male grades using this rate
    According the request:
    'Women on average receive higher grades than men'
    """
    MALE_MISFORTUNE = 0.95
#    SELECT_CLASSES_BY_SEMESTER = """SELECT class_seq, department_code, course_no FROM cd.class WHERE
#    year=%s AND season=%s and section='001' """
    ENROLLMENT_STATUS = ['enr', 'inc', 'ngr']

    ENROLLMENT_TABLE = 'ed.enrollment'
    ENROLLMENT_COLUMNS = ['class_seq', 'student_id', 'status', 'grade']

    def __init__(self, dictionary, student, classgen, rand):
        self.dictionary = dictionary
        # make a student map
        self.student = {}
        for (name, val) in zip(StudentGenerator.STUDENT_COLUMNS, student):
            self.student[name] = val
#        self.classes = classes
        assert isinstance(classgen, ClassGenerator)
        self.classgen = classgen
        self.fill_req_map = {}
        self.taken_courses = []
        self.min_level = 0
        self.distribution = {}
        self.courses_by_level = {}
        self.free_courses = []
        self.counter = 0
        self.enrollment_data = []
        self.semester_classes = {}
        self.grade_gen = random.Random(rand.randint(0, 100000))
        self.course_gen = random.Random(rand.randint(0, 100000))
        self.status_gen = random.Random(rand.randint(0, 100000))
        self.sorted_courses = sorted(dictionary.courses)

    # returns random course by specified classification (or its children if required),
    # None if no suitable courses found
    def get_course_by_classification(self, classification):
        if classification not in self.dictionary.classified_courses:
            return None
        course_list = self.dictionary.classified_courses[classification]
        candidate_list = []
        for course in course_list:
            if course not in self.taken_courses:
                candidate_list.append(course)
        if len(candidate_list) > 0:
            return self.get_rand_item(candidate_list, self.course_gen)
        else:
            for child in self.dictionary.classification_tree[classification]:
                return self.get_course_by_classification(child)
        return None

    # adds course to the list of taken courses with all prerequisites
    def add_course(self, course_key, classif_list):
        self.taken_courses.append(course_key)
        credits = self.dictionary.courses[course_key]
        for classif in classif_list:
            if classif in self.fill_req_map:
                self.fill_req_map[classif] = self.fill_req_map[classif] + credits
            else:
                self.fill_req_map[classif] = credits
        # add requirements
        if course_key in self.dictionary.course_prereq:
            for req_course in self.dictionary.course_prereq[course_key]:
                if req_course not in self.taken_courses:
                    self.add_course(req_course, self.dictionary.course_classification[req_course])

    def fill_required_courses(self):
        if (self.student['school_code'], self.student['program_code']) not in self.dictionary.program_req:
            return
        r = self.dictionary.program_req[(self.student['school_code'], self.student['program_code'])]
        for req in r:
            self.fill_req_map[req] = 0
        for req in sorted(r):
            while self.fill_req_map[req] < r[req]:
                course_key = self.get_course_by_classification(req)
                if course_key is None:
                    break
                self.add_course(course_key, [req])

    def correct_req_level(self, course_key, level):
        if course_key not in self.distribution:
            cur_level = 0
        else:
            cur_level = self.distribution[course_key]
        if cur_level > level:
            self.distribution[course_key] = level
            # update min level
            if level < self.min_level:
                self.min_level = level
            if course_key in self.dictionary.course_prereq:
                for req_course in self.dictionary.course_prereq[course_key]:
                    self.correct_req_level(req_course, level - 1)

    def distribute_courses(self):
        for course_key in self.taken_courses:
            if course_key in self.dictionary.course_prereq and course_key not in self.distribution:
                self.distribution[course_key] = 0
                for req_course_key in self.dictionary.course_prereq[course_key]:
                    self.correct_req_level(req_course_key, -1)
        for course_key in sorted(self.distribution):
            level = self.distribution[course_key] - self.min_level
            if level not in self.courses_by_level:
                self.courses_by_level[level] = []
            self.courses_by_level[level].append(course_key)
        for course_key in self.taken_courses:
            if course_key not in self.distribution:
                self.free_courses.append(course_key)

    def get_semester_classes(self):
        res = {}
        for record in self.classgen.class_data:
            dep = record[ClassGenerator.CLASS_COLUMNS.index('department_code')]
            no = record[ClassGenerator.CLASS_COLUMNS.index('course_no')]
            year = record[ClassGenerator.CLASS_COLUMNS.index('year')]
            season = record[ClassGenerator.CLASS_COLUMNS.index('season')]
            res.setdefault((year, season), {})
            res[year, season].setdefault((dep, no), [])
            res[year, season][dep, no].append(record)
        self.semester_classes = res

    def can_take(self, course_key):
        if course_key in self.taken_courses:
            return False
        if course_key in self.dictionary.course_prereq:
            for req_course_key in self.dictionary.course_prereq[course_key]:
                if req_course_key not in self.taken_courses:
                    return False
        return True

    def get_class_by_course(self, course_key, semester, classes_by_semester):
        if course_key not in classes_by_semester:
            return None
        sections = classes_by_semester[course_key]
        # more than one section means that previous classes are full
        # so we choose class with max section value
        sections.sort(key = lambda x: int(x[ClassGenerator.CLASS_COLUMNS.index('section')]))
        cls = sections[-1]
        class_seq = cls[ClassGenerator.CLASS_COLUMNS.index('class_seq')]
        if self.classgen.get_class_fill(class_seq) < ClassGenerator.CLASS_FILL_LIMIT:
            self.classgen.take_class(class_seq)
            return class_seq
        inst = cls[ClassGenerator.CLASS_COLUMNS.index('instructor_code')]
        section = cls[ClassGenerator.CLASS_COLUMNS.index('section')]
        section = self.classgen.next_section(section)
        cls = self.classgen.generate_class(inst, course_key, semester, section)
        sections.append(cls)
        class_seq = cls[ClassGenerator.CLASS_COLUMNS.index('class_seq')]
        self.classgen.take_class(class_seq)
        return class_seq

    def choose_class(self, level, semester, classes_by_semester):
        for i in range(100):    # just to avoid endless cycle
            if level in self.courses_by_level and len(self.courses_by_level[level]) > 0:
                c = self.get_rand_item(self.courses_by_level[level], self.course_gen)
                cls = self.get_class_by_course(c, semester, classes_by_semester)
                if cls is not None:
                    self.courses_by_level[level].remove(c)
                    return (cls, c)
            if len(self.free_courses) > 0:
                c = self.get_rand_item(self.free_courses, self.course_gen)
                cls = self.get_class_by_course(c, semester, classes_by_semester)
                if cls is not None:
                    self.free_courses.remove(c)
                    return (cls, c)
            c = self.get_rand_item(self.sorted_courses, self.course_gen)
            if self.can_take(c):
                cls = self.get_class_by_course(c, semester, classes_by_semester)
                if cls is not None:
                    self.taken_courses.append(c)
                    return (cls, c)

    def get_random_grade(self, semester):
        if CURDATE < semester['end_date']:
            return None
        if self.grade_gen.randint(1,100) <= self.NULL_PERCENT:
            return None
        # since python 2.6
#        val = self.grade_gen.triangular(0.0, 4.0, 2.5)
        val = self.grade_gen.normalvariate(2.5, 1.3)
        # cut both ends
        if val < 0:
            val = 0
        if val > 4:
            val = 4
        if self.student['gender'] == 'm':
            """ Women on average receive higher grades than men """
            val = val * self.MALE_MISFORTUNE
        return round(val, 1)

    def generate_enrollment(self, class_id, semester):
        status = self.get_rand_item(self.ENROLLMENT_STATUS, self.status_gen)
        return [
            class_id,
            self.student['id'],
            status,
            self.get_random_grade(semester)
        ]

    def generate_content(self):
        self.fill_required_courses()
        self.distribute_courses()
        self.get_semester_classes()
        level = 0
        credits_gen = random.Random(RANDOM_SEED)
        for semester in self.dictionary.semesters:
            study_time = (semester["end_date"] - self.student["start_date"]).days
            if study_time > 0 and study_time < 4 * 356 \
                    and semester['season'] != 'summer' \
                    and semester["begin_date"] < CURDATE:
                credits = credits_gen.uniform(self.CREDITS_PER_SEMESTER[0], self.CREDITS_PER_SEMESTER[1])
                classes_by_semester = self.semester_classes[semester['year'], semester['season']].copy()
                credits_taken = 0
                while credits_taken < credits:
                    (class_seq, course_key) = self.choose_class(level, semester, classes_by_semester)
                    credits_taken = credits_taken + self.dictionary.courses[course_key]
                    # make enrollment
                    enr = self.generate_enrollment(class_seq, semester)
                    self.enrollment_data.append(enr)

                # if all courses of the current lavel are off, move to the next
                if level in self.courses_by_level and len(self.courses_by_level[level]) == 0:
                    level = level + 1

    def get_content(self):
        return [
            self.make_insert_map(self.ENROLLMENT_TABLE, self.ENROLLMENT_COLUMNS, self.enrollment_data)
        ]


class StudentGenerator(BaseDataGenerator):

    """ Generate students """
    ADMISSION_SIZE = (100, 125)
    ADMISSION_AGE = (18, 25)
    STUDENT_ID_OFFSET = 1000
    INACTIVE_PERCENT = 5

    STUDENT_TABLE = "ed.student"
    STUDENT_COLUMNS = ["id", "name", "gender", "dob", "school_code", "program_code", "start_date", "is_active"]

    def __init__(self, name_generator, dictionary):
        self.name_generator = name_generator
        self.dictionary = dictionary
        self.student_counter = 0
        self.cur_year = datetime.datetime.now().year
        self.student_data = []
        self.gender_gen = random.Random(RANDOM_SEED)
        self.dob_gen = random.Random(RANDOM_SEED)
        self.active_gen = random.Random(RANDOM_SEED)
        self.school_gen = random.Random(RANDOM_SEED)
        self.program_gen = {}
        for school_code in sorted(self.dictionary.school_programs):
            self.program_gen[school_code] = random.Random(RANDOM_SEED)

    def generate_student(self, semester):
        gender = self.generate_gender(self.gender_gen)
        full_name = self.name_generator.generate(gender)
        dob_end = datetime.date(semester["year"] - self.ADMISSION_AGE[0], 12, 31)
        dob_start = datetime.date(semester["year"] - self.ADMISSION_AGE[1], 1, 1)
        dob = self.generate_date(dob_start, dob_end, self.dob_gen)
        school = self.get_rand_item(sorted(self.dictionary.school_programs), self.school_gen)
        program = self.get_rand_item(self.dictionary.school_programs[school], self.program_gen[school])
        self.student_counter += 1
        # what about masters and doctors?
        active = (CURDATE - semester["begin_date"]).days < 4 * 365
        if active and self.active_gen.randint(1,100) <= self.INACTIVE_PERCENT:
            active = False
        return [
            self.STUDENT_ID_OFFSET + self.student_counter,
            full_name,
            gender,
            dob,
            school,
            program,
            semester["begin_date"],
            active
        ]

    def generate_content(self):
        admission_gen = random.Random(RANDOM_SEED)
        for semester in self.dictionary.semesters:
            if semester["season"] == 'fall' and semester["begin_date"] <= CURDATE:
                # make admission
                student_count = admission_gen.randint(self.ADMISSION_SIZE[0], self.ADMISSION_SIZE[1])
                for i in range (0, student_count):
                    student = self.generate_student(semester)
                    self.student_data.append(student)

    def get_content(self):
        return [
            self.make_insert_map(self.STUDENT_TABLE, self.STUDENT_COLUMNS, self.student_data)
        ]


def generate(content):
    dictionary = CollectionDictionaryLoader().load(content)
    random.seed(RANDOM_SEED)
    name_data = StatNameData()
    inst_namegen = StatNameGenerator(name_data, INSTRUCTOR_NAME_SEED)

    instgen = InstructorGenerator(inst_namegen, dictionary)
    instgen.generate_content()
    result = instgen.get_content()

    classgen = ClassGenerator(dictionary, instgen.dep_app)
    classgen.generate_content()

    stud_namegen = StatNameGenerator(name_data, STUDENT_NAME_SEED)
    studgen = StudentGenerator(stud_namegen, dictionary)
    studgen.generate_content()
    result.extend(studgen.get_content())

    r = random.Random(RANDOM_SEED)
    enr_result = []
    for student in studgen.student_data:
        enrgen = EnrollmentGenerator(dictionary, student, classgen, r)
        enrgen.generate_content()
        enr_result.extend(enrgen.get_content())

    classgen.drop_empty_classes()
    result.extend(classgen.get_content())
    result.extend(enr_result)
    return result
