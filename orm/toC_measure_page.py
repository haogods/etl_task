# coding=utf-8

from mongoengine import *


class toCMeasurePageModel(Document):
    jdId = StringField(max_length=100, required=True, unique=True)
    jdUrl = StringField(max_length=200, required=True)
    mobileJdUrl = StringField(required=False)
    channel = StringField(max_length=30, required=True)
    publishTime = LongField(required=True)
    measure_time = LongField(required=True)
    incSegmentId = StringField(required=False)
    incId = StringField(required=False)
    incIndustry = StringField(required=False)
    incIndustryId = IntField(required=False)
    incName = StringField(required=False)
    incIntro = StringField(required=False)
    incType = IntField(required=False)
    incScaleMin = IntField(required=False)
    incScaleMax = IntField(required=False)
    jobSalaryMin = IntField(required=False)
    jobSalaryMax = IntField(required=False)
    jobWorkLoc = StringField(required=False)
    jobWorkLocId = StringField(required=False)
    jobDiploma = StringField(required=False)
    jobDiplomaId = IntField(required=False)
    jobWorkAgeMin = IntField(required=False)
    jobWorkAgeMax = IntField(required=False)
    jobCate = StringField(required=False)
    jobWelfare = StringField(required=False)
    jobType = StringField(required=False)

    major = StringField(required=False)
    majorIds = ListField(StringField())
    majorIdsFlag = IntField(required=False)
    jobPosition = StringField(required=False)
    jobDescription = StringField(required=False)
    jd_measure_hash = StringField(required=False)
    jd_content_hash = StringField(required=False)
    salaryRatio = FloatField(required=False)
    total_ind_rank_postion = FloatField(required=False)
    tagList = ListField(StringField())
    isExpired = IntField(required=True, default=0)

    meta = {
        "db_alias": "2c_ipin",
        "collection": "jd_measure",
        "indexes": [
            {
                "fields": ("jdId",),
                "unique": True
            }
        ]
    }



