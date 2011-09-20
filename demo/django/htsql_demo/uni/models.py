from django.db import models

# Note that this schema differs significantly from the HTSQL 
# regression test schema.  Django doesn't support multiple-column 
# FKs and has chosen to favor the empty string over NULL.

class School(models.Model):
    code = models.CharField(max_length=16, primary_key=True,
                            db_column='code')
    name = models.CharField(max_length=64, unique=True)

class Department(models.Model):
    code = models.CharField(max_length=16, primary_key=True,
                            db_column='code')
    name = models.CharField(max_length=64, unique=True)
    school = models.ForeignKey(School, db_column='school_code', 
                               blank=True, null=True)

class Program(models.Model):
    DEGREE_CHOICES = (
        (u'ba', u"Bachelor's of Arts"),
        (u'bs', u"Bachelor's of Science"),
        (u'ct', u"Certificate"),
        (u'pb', u"Postbaccalaureate"),
        (u'ma', u"Master's of Arts"),
        (u'ms', u"Master's of Science"),
        (u'ph', u"Doctor of Philosophy"))
    school = models.ForeignKey(School, db_column='school_code')
    code = models.CharField(max_length=16)
    title = models.CharField(max_length=64, unique=True)
    degree = models.CharField(max_length=2, 
                       choices=DEGREE_CHOICES, 
                       blank=True, null=True)
    part_of = models.CharField(max_length=16,
                               blank=True, null=True)

class Course(models.Model):
    department = models.ForeignKey(Department, db_column='department')
    no = models.IntegerField()
    title = models.CharField(max_length=64, unique=True)
    credits = models.IntegerField(blank=True, null=True)
    description = models.TextField(blank=True, null=True)

