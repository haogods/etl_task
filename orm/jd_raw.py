# coding=utf-8

from mongoengine import *


class JdRaw(Document):
    jdId = StringField(required=True, unique=True)
    jdUrl = StringField(required=True, default='')
    jdFrom = StringField(required=True, default='')
    pubDate = StringField(required=True, default='')

    incName = StringField(required=True, default='')
    incIndustry = StringField(required=True, default='')
    incType = StringField(required=True, default='')
    incScale = StringField(required=True, default='')
    incIntro = StringField(required=True, default='')
    incUrl = StringField(required=True, default='')
    incStage = StringField(required=True, default='')

    jobPosition = StringField(required=True, default='')
    jobPositionExt = StringField(required=True, default='')
    jobSalary = StringField(required=True, default='')
    jobWorkLoc = StringField(required=True, default='')
    jobWorkLocExt = StringField(required=True, default='')
    jobDiploma = StringField(required=True, default='')
    jobWorkAge = StringField(required=True, default='')
    jobDescription = StringField(required=True, default='')
    jobCate = StringField(required=True, default='')
    jobType = StringField(required=True, default='')
    jobWelfare = StringField(required=True, default='')
    jobPersonNumber = StringField(required=True, default='')
    jobTags = StringField(required=True, default='')
    isExpired = IntField(required=True, default=0)

    meta = {
        "collection": "jd_raw",
        "db_alias": "jd_51job_raw",
    }



def convertToJdRaw(item):
    jdRaw = JdRaw()
    jdRaw.jdId = item.jdId
    jdRaw.jdFrom = item.jdFrom
    jdRaw.jdUrl = item.jdUrl
    jdRaw.incName = item.jdInc.incName.value
    jdRaw.incIntro = item.jdInc.incIntro.value
    jdRaw.incType = item.jdInc.incType.value
    jdRaw.incScale = item.jdInc.incScale.value
    jdRaw.incIndustry = item.jdInc.incIndustry.value
    jdRaw.incUrl = item.jdInc.incUrl.value


    jdRaw.jobCate = item.jdJob.jobCate.value
    jdRaw.jobType = item.jdJob.jobType.value
    jdRaw.jobSalary = item.jdJob.jobSalary.value
    jdRaw.jobDescription = item.jdJob.jobDescription.value
    jdRaw.jobWorkAge = item.jdJob.jobWorkAge.value
    jdRaw.jobDiploma = item.jdJob.jobDiploma







