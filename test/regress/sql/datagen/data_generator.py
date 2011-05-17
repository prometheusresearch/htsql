
import random, csv, datetime, math
import os

INSERT = True
RANDOM_SEED = 0


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

class StatNameGenerator(NameGenerator):

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
        value = random.uniform(0, float(total))
        return self.binary_search(names, value, len(names) - 1, 0)

    def generate_female(self):
        first = self.get_random_name(self.woman_names, self.woman_total).capitalize()
        last = self.get_random_name(self.last_names, self.last_total).capitalize()
        return first + " " + last

    def generate_male(self):
        first = self.get_random_name(self.man_names, self.man_total).capitalize()
        last = self.get_random_name(self.last_names, self.last_total).capitalize()
        return first + " " + last


class Meta(object):

    SELECT_DEPARTMENTS = 'SELECT department_code, count(*) FROM ad.course GROUP BY department_code'
    SELECT_SEMESTERS = 'SELECT year, season, begin_date, end_date FROM cd.semester'
    SELECT_SCHOOL_PROGRAMS = 'SELECT s.code, p.code FROM ad.school s INNER JOIN ad.program p ON p.school_code = s.code'
    SELECT_CLASSIFICATION = 'SELECT code, part_of FROM rd.classification'
    SELECT_PROGRAM_REQ = 'SELECT school_code, program_code, classification_code, credit_hours FROM rd.program_requirement'
    SELECT_COURSE_PREREQ = 'SELECT of_department_code, of_course_no, on_department_code, on_course_no FROM rd.prerequisite'
    SELECT_COURSES = 'SELECT department_code, no, credits FROM ad.course'
#    SELECT_COURSES_BY_CLASSIFICATION = '''SELECT d.school, c.department, c."no" FROM ad.course c JOIN ad.departments d
#    ON c.department=d.code WHERE c.department, c."no" IN
#    (SELECT department, course FROM rd.course_classification WHERE classification IN (%s) )'''
    SELECT_CLASSIFIED_COURSES = 'SELECT department_code, course_no, classification_code FROM rd.course_classification'

    def __init__(self):
        self.departments = []
        self.semesters = []
        self.school_programs = {}
        self.program_req = {}
        self.classification_tree = {}
        self.courses = {}
        self.course_prereq = {}
        self.classified_courses = {}
        self.course_classification = {}

    def fetch(self, cursor, sql):
        cursor.execute(sql)
        res = []
        for items in cursor.fetchall():
            attributes = {}
            for kind, item in zip(cursor.description, items):
                name = kind[0]
                attributes[name] = item
            res.append(attributes)
        return res

    def load_school_programs(self, cursor):
        cursor.execute(self.SELECT_SCHOOL_PROGRAMS)
        for (school, program) in cursor.fetchall():
            if school not in self.school_programs:
                self.school_programs[school] = []
            self.school_programs[school].append(program)

    def load_classification(self, cursor):
        cursor.execute(self.SELECT_CLASSIFICATION)
        for (code, part_of) in cursor.fetchall():
            if code not in self.classification_tree:
                self.classification_tree[code] = []
            if part_of not in self.classification_tree:
                self.classification_tree[part_of] = []
            self.classification_tree[part_of].append(code)

    def load_program_req(self, cursor):
        cursor.execute(self.SELECT_PROGRAM_REQ)
        for (school, program, req, credits) in cursor.fetchall():
            key = (school, program)
            if key not in self.program_req:
                self.program_req[key] = {}
            map = self.program_req[key]
            map[req] = credits

    def load_courses(self, cursor):
        cursor.execute(self.SELECT_COURSES)
        for (dep, course, credits) in cursor.fetchall():
            if credits is None:
                credits = 0
            self.courses[(dep, course)] = credits

    def load_course_prereq(self, cursor):
        cursor.execute(self.SELECT_COURSE_PREREQ)
        for (of_dep, of_course, on_dep, on_course) in cursor.fetchall():
            key = (of_dep, of_course)
            value = (on_dep, on_course)
            if key not in self.course_prereq:
                self.course_prereq[key] = []
            self.course_prereq[key].append(value)

#    def load_courses(self, cursor, classifications):
#        values = ','.join(map(lambda x: self.quote(x), classifications))
#        cursor.execute(self.SELECT_COURSES_BY_CLASSIFICATION % values)
#        return list(cursor.fetchall())

    def load_classified_courses(self, cursor):
        cursor.execute(self.SELECT_CLASSIFIED_COURSES)
        for (dep, course, classif) in cursor.fetchall():
            course_key = (dep, course)
            if classif not in self.classified_courses:
                self.classified_courses[classif] = []
            self.classified_courses[classif].append(course_key)
            if course_key not in self.course_classification:
                self.course_classification[course_key] = []
            self.course_classification[course_key].append(classif)

    def load(self, cursor):
        cursor.execute(self.SELECT_DEPARTMENTS)
        self.departments = list(cursor.fetchall())
        self.semesters = self.fetch(cursor, self.SELECT_SEMESTERS)
        self.semesters = sorted(self.semesters, key=lambda x: x["begin_date"])

        self.load_school_programs(cursor)
        self.load_classification(cursor)
        self.load_program_req(cursor)
        self.load_courses(cursor)
        self.load_course_prereq(cursor)
        self.load_classified_courses(cursor)


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

    def make_unique_code(self, map, code):
        if code not in map:
            return code
        i = 1
        while (code + str(i)) in map:
            i += 1
        return (code + str(i))

    def generate_gender(self):
        if random.randint(1,100) <= self.MAN_WOMAN_BORDER:
            return "m"
        return "f"

    def get_rand_item(self, arr):
        return arr[random.randint(0, len(arr) - 1)]

    def generate_date(self, start, end):
        delta = end - start
        return start + datetime.timedelta(random.randint(1, delta.days))

    def insertFromDict(self, cursor, table, dict):
        """Take dictionary object dict and produce sql for
        inserting it into the named table"""
        sql = 'INSERT INTO ' + table
        sql += ' ('
        sql += ', '.join(dict.keys())
        sql += ') VALUES ('
        sql += ', '.join(map(lambda x: self.quote(x), dict.values()))
        sql += ');'
        if INSERT:
            cursor.execute(sql)
        return sql

    def run(self, connection, verbose=True):
        pass


class InstructorGenerator(BaseDataGenerator):

    """Generate instructors"""
    INSTRUCTOR_TITLES = ['mr', 'dr', 'prof', 'ms']
    HALF_TIME_PERCENT = 15
    COURSES_PER_INSTRUCTOR = (2, 4)
    INSTRUCTOR_TABLE = "id.instructor"
    CONFIDENTIAL_TABLE = "id.confidential"
    APPOINTMENT_TABLE = "id.appointment"

    def __init__(self, name_generator, meta):
        self.name_generator = name_generator
        self.meta = meta
        self.instructors = {}
        self.dep_app = {}

    def generate_phone(self):
        if random.randint(1,100) <= self.NULL_PERCENT:
            return None
        # 7-digit phone
        tel = str(random.randint(1000000, 9999999))
        return tel[:3] + '-' + tel[3:]

    def generate_email(self, code):
        if random.randint(1,100) <= self.NULL_PERCENT:
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
        ssn = str(random.randint(100000000, 999999999))
        return ssn[:3] + '-' + ssn[3:5] + '-' + ssn[5:]

    def generate_home_phone(self):
        phone = self.generate_phone()
        if phone is not None:
            return '702-' + phone
        return None

    def generate_instructor(self):
        gender = self.generate_gender()
        full_name = self.name_generator.generate(gender)
        code = self.generate_code(full_name)
        title = self.get_rand_item(self.INSTRUCTOR_TITLES)
        while (title == 'mr' and gender == 'f') \
                or (title == 'ms' and gender == 'm'):
            title = self.get_rand_item(self.INSTRUCTOR_TITLES)
        return {
            "code": code,
            "full_name": full_name,
            "title": title,
            "phone": self.generate_phone(),
            "email": self.generate_email(code)
        }

    def generate_confidential(self, instructor):
        return {
            "instructor_code": instructor["code"],
            "SSN": self.generate_ssn(),
            "pay_grade": random.randint(4,8),
            "home_phone": self.generate_home_phone()
        }

    def generate_appointment(self, instructor, depcode):
        fraction = None
        if random.randint(1,100) > self.NULL_PERCENT:
            if random.randint(1,100) <= self.HALF_TIME_PERCENT:
                fraction = 0.50
            else:
                fraction = 1.00
        return {
            "instructor_code": instructor["code"],
            "department_code": depcode,
            "fraction": fraction
        }

    def run(self, connection, verbose=True):
        cursor = connection.cursor()
        for (depcode, total_courses) in self.meta.departments:
            load = random.uniform(self.COURSES_PER_INSTRUCTOR[0], self.COURSES_PER_INSTRUCTOR[1])
            count = int(math.ceil(total_courses / load))
            if depcode not in self.dep_app:
                self.dep_app[depcode] = []
            for i in range(0, count):
                instructor = self.generate_instructor()
                self.insertFromDict(cursor, self.INSTRUCTOR_TABLE, instructor)
                self.instructors[instructor["code"]] = instructor
                self.insertFromDict(cursor, self.CONFIDENTIAL_TABLE, self.generate_confidential(instructor))
                appointment = self.generate_appointment(instructor, depcode)
                self.dep_app[depcode].append(appointment)
                self.insertFromDict(cursor, self.APPOINTMENT_TABLE, appointment)
        connection.commit()
        if (verbose):
            print(str(len(self.instructors)) + ' instructors generated.')


class ClassGenerator(BaseDataGenerator):

    CLASS_TABLE = 'cd.class'

    def __init__(self, meta, dep_app):
        self.meta = meta
        self.dep_app = dep_app
        self.curdate = datetime.date.today()

    def generate_class(self, inst_code, course_key, semester):
        # instructor is nullable ?!
        if random.randint(1,100) <= self.NULL_PERCENT:
            inst = None
        else:
            inst = inst_code
        return {
            'department_code': course_key[0],
            'course_no': course_key[1],
            'year': semester['year'],
            'season': semester['season'],
            'section': '001',
            'instructor_code': inst
        }

    def run(self, connection, verbose=True):
        cursor = connection.cursor()
        counter = 0
        for depcode in self.dep_app:
            dep_courses = []
            for course_key in self.meta.courses:
                if course_key[0] == depcode:
                    dep_courses.append(course_key)
            staff = 0
            for inst in self.dep_app[depcode]:
                if inst['fraction'] is not None:
                    staff = staff + inst['fraction']
            load = len(dep_courses)/staff
            for app in self.dep_app[depcode]:
                class_count = 0
                if app['fraction'] is not None:
                    class_count = int(round(load/app['fraction']))
                i = 0
                while i < class_count and len(dep_courses) > 0:
                    i += 1
                    course = self.get_rand_item(dep_courses)
                    dep_courses.remove(course)
                    for semester in self.meta.semesters:
                        if semester['begin_date'] < self.curdate \
                                and semester['season'] != 'summer':
                            self.insertFromDict(cursor, self.CLASS_TABLE,
                                    self.generate_class(app['instructor_code'], course, semester))
                            counter += 1
        connection.commit()
        if verbose:
            print(str(counter) + ' classes generated.')


class EnrollmentGenerator(BaseDataGenerator):

    CREDITS_PER_SEMESTER = (15, 25)
    """ Decrease male grades using this rate
    According the request:
    'Women on average receive higher grades than men'
    """
    MALE_MISFORTUNE = 0.95
    SELECT_CLASSES_BY_SEMESTER = """SELECT class_seq, department_code, course_no FROM cd.class WHERE
    year=%s AND season=%s and section='001' """
    ENROLLMENT_STATUS = ['enr', 'inc', 'ngr']
    ENROLLMENT_TABLE = 'ed.enrollment'

    def __init__(self, meta, student):
        self.meta = meta
        self.student = student
        self.curdate = datetime.date.today()
        self.fill_req_map = {}
        self.taken_courses = []
        self.min_level = 0
        self.distribution = {}
        self.courses_by_semester = {}
        self.free_courses = []
        self.counter = 0

    # returns random course by specified classification (or its children if required),
    # None if no suitable courses found
    def get_course_by_classification(self, classification):
        if classification not in self.meta.classified_courses:
            return None
        course_list = self.meta.classified_courses[classification]
        candidate_list = []
        for course in course_list:
            if course not in self.taken_courses:
                candidate_list.append(course)
        if len(candidate_list) > 0:
            return self.get_rand_item(candidate_list)
        else:
            for child in self.meta.classification_tree[classification]:
                return self.get_course_by_classification(child)
        return None

    # adds course to the list of taken courses with all prerequisites
    def add_course(self, course_key, classif_list):
        self.taken_courses.append(course_key)
        credits = self.meta.courses[course_key]
        for classif in classif_list:
            if classif in self.fill_req_map:
                self.fill_req_map[classif] = self.fill_req_map[classif] + credits
            else:
                self.fill_req_map[classif] = credits
        # add requirements
        if course_key in self.meta.course_prereq:
            for req_course in self.meta.course_prereq[course_key]:
                if req_course not in self.taken_courses:
                    self.add_course(req_course, self.meta.course_classification[req_course])

    def fill_required_courses(self):
        if (self.student['school_code'], self.student['program_code']) not in self.meta.program_req:
            return
        r = self.meta.program_req[(self.student['school_code'], self.student['program_code'])]
        for req in r:
            self.fill_req_map[req] = 0
        for req in r:
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
            if course_key in self.meta.course_prereq:
                for req_course in self.meta.course_prereq[course_key]:
                    self.correct_req_level(req_course, level - 1)

    def distribute_courses(self):
        for course_key in self.taken_courses:
            if course_key in self.meta.course_prereq and course_key not in self.distribution:
                self.distribution[course_key] = 0
                for req_course_key in self.meta.course_prereq[course_key]:
                    self.correct_req_level(req_course_key, -1)
        for course_key in self.distribution:
            level = self.distribution[course_key] - self.min_level
            if level not in self.courses_by_semester:
                self.courses_by_semester[level] = []
            self.courses_by_semester[level].append(course_key)
        for course_key in self.taken_courses:
            if course_key not in self.distribution:
                self.free_courses.append(course_key)

    def can_take(self, course):
        if course in self.taken_courses:
            return False
        if course in self.meta.course_prereq:
            for req_course in self.meta.course_prereq[course]:
                if req_course not in self.taken_courses:
                    return False
        return True

    def choose_course(self, level):
        if level in self.courses_by_semester and len(self.courses_by_semester[level]) > 0:
            res = self.get_rand_item(self.courses_by_semester[level])
            self.courses_by_semester[level].remove(res)
            return res
        if len(self.free_courses) > 0:
            res = self.get_rand_item(self.free_courses)
            self.free_courses.remove(res)
            return res
        while True:
            res = self.get_rand_item(self.meta.courses.keys())
            if self.can_take(res):
                self.taken_courses.append(res)
                return res

    def get_random_grade(self, semester):
        if self.curdate < semester['end_date']:
            return None
        if random.randint(1,100) <= self.NULL_PERCENT:
            return None
        val = random.triangular(0.0, 4.0, 2.5)
#        if val < 2.0:
#            """ Average course grade should be between 2 and 3, so
#            small grades are increased accordingly. """
#            val = val * 2
        if self.student['gender'] == 'm':
            """ Women on average receive higher grades than men """
            val = val * self.MALE_MISFORTUNE
        return round(val, 1)

    def generate_enrollment(self, class_id, semester):
        status = self.get_rand_item(self.ENROLLMENT_STATUS)
        return {
            'class_seq': class_id,
            'student_id': self.student['id'],
            'status': status,
            'grade': self.get_random_grade(semester)
        }

    def create_enrollments_per_semester(self, courses, semester, cursor):
        sql = self.SELECT_CLASSES_BY_SEMESTER % (semester["year"], self.quote(semester["season"]))
        cursor.execute(sql)
        class_list = []
        for (id, dep, no) in cursor.fetchall():
            if (dep, no) in courses:
                class_list.append(id)
        # generate enrollments
        for class_id in class_list:
            enr = self.generate_enrollment(class_id, semester)
            self.insertFromDict(cursor, self.ENROLLMENT_TABLE, enr)
            self.counter += 1

    def run(self, connection, verbose=True):
        self.fill_required_courses()
        self.distribute_courses()
        level = 0
        cursor = connection.cursor()
        for semester in self.meta.semesters:
            study_time = (semester["end_date"] - self.student["start_date"]).days
            if study_time > 0 and study_time < 4 * 356 \
                    and semester['season'] != 'summer' \
                    and semester["begin_date"] < self.curdate:
                credits = random.uniform(self.CREDITS_PER_SEMESTER[0], self.CREDITS_PER_SEMESTER[1])
                semester_courses = []
                credits_taken = 0
                while credits_taken < credits:
                    new_course = self.choose_course(level)
                    semester_courses.append(new_course)
                    credits_taken = credits_taken + self.meta.courses[new_course]
                # if all courses of the current lavel are off, move to the next
                if level in self.courses_by_semester and len(self.courses_by_semester[level]) == 0:
                    level = level + 1
                self.create_enrollments_per_semester(semester_courses, semester, cursor)
        connection.commit()

class StudentGenerator(BaseDataGenerator):

    """ Generate students """
    ADMISSION_SIZE = (30, 50)
    ADMISSION_AGE = (18, 25)
    STUDENT_ID_OFFSET = 1000
    STUDENT_TABLE = "ed.student"
    ENROLLMENT_TABLE = "ed.enrollment"

    def __init__(self, name_generator, meta):
        self.name_generator = name_generator
        self.meta = meta
        self.student_counter = 0
        self.cur_year = datetime.datetime.now().year
        self.curdate = datetime.date.today()
        self.students = []

    def generate_student(self, semester):
        gender = self.generate_gender()
        full_name = self.name_generator.generate(gender)
        dob_end = datetime.date(semester["year"] - self.ADMISSION_AGE[0], 12, 31)
        dob_start = datetime.date(semester["year"] - self.ADMISSION_AGE[1], 1, 1)
        dob = self.generate_date(dob_start, dob_end)
        school = self.get_rand_item(self.meta.school_programs.keys())
        program = self.get_rand_item(self.meta.school_programs[school])
        self.student_counter += 1
        # what about masters and doctors?
        active = (self.curdate - semester["begin_date"]).days < 4 * 365
        return {
            "id": self.STUDENT_ID_OFFSET + self.student_counter,
            "name": full_name,
            "gender": gender,
            "dob": dob,
            "school_code": school,
            "program_code": program,
            "start_date": semester["begin_date"],
            "is_active": active
        }

    # recursively get classification and all its children
#    def get_valid_classifications(self, classification):
#        res = [classification]
#        for child in self.meta.classification_tree[classification]:
#            res.extend(self.get_valid_classifications(child))
#        return res

    # recursively get all requirements of the specified course
#    def get_prereq(self, course_key):
#        res = []
#        if course_key in self.meta.course_prereq:
#            for req_course_key in self.meta.course_prereq[course_key]:
#                res.append(req_course_key)
#                res.extend(self.get_prereq(req_course_key))
#        return res

    def run(self, connection, verbose=True):
        cursor = connection.cursor()
        for semester in self.meta.semesters:
            if semester["season"] == 'fall' and semester["begin_date"] <= self.curdate:
                # make admission
                student_count = random.randint(self.ADMISSION_SIZE[0], self.ADMISSION_SIZE[1])
                for i in range (0, student_count):
                    student = self.generate_student(semester)
                    self.students.append(student)
                    self.insertFromDict(cursor, self.STUDENT_TABLE, student)
        connection.commit()
        if verbose:
            print(str(self.student_counter) + ' students generated.')


#def get_connection(db, host, port, user, password):
#    return psycopg2.connect(database = db, host = host, port = port,
#        user = user, password = password)

def generate(connection, verbose=True):
#    connection = get_connection(DB, HOST, PORT, USER, PASSWORD)
#    print("Connection successful")
    m = Meta()
    m.load(connection.cursor())
    random.seed(RANDOM_SEED)
    instgen = InstructorGenerator(StatNameGenerator(), m)
    instgen.run(connection, verbose)
    classgen = ClassGenerator(m, instgen.dep_app)
    classgen.run(connection, verbose)
    studgen = StudentGenerator(StatNameGenerator(), m)
    studgen.run(connection, verbose)
    enr_counter = 0
    for student in studgen.students:
        enrgen = EnrollmentGenerator(m, student)
        enrgen.run(connection, verbose)
        enr_counter += enrgen.counter
    if verbose:
        print(str(enr_counter) + ' enrollments generated.')
