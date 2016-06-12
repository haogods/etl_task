# coding=utf-8

from mongoengine import Document, StringField, IntField, LongField, connect


class JdMeasure(Document):

    jdId = StringField(required=True, unique=True)
    jdFrom = StringField(required=True)
    pubDateStamp = LongField(required=True)
    pubDateUtcOffset = IntField(required=True)
    pubDateFlag = IntField(required=True)
    jdMd5 = StringField(required=True)
    jdSimHash = StringField(required=True)

    incSegmentId = StringField(required=True)
    incSegmentIdFlag = IntField(required=True)
    incIndustryId = IntField(required=True)
    incIndustryIdFlag = IntField(required=True)
    incType = IntField(required=True)
    incTypeFlag = IntField(required=True)
    incScaleMin = IntField(required=True)
    incScaleMinFlag = IntField(required=True)

    incScaleMax = IntField(required=True)
    incScaleMaxFlag = IntField(required=True)

    jobLevel = IntField(required=True, default=-1)
    jobLevelFlag = IntField(required=True, default=0)
    jobSalaryMin = IntField(required=True)
    jobSalaryMinFlag = IntField(required=True)

    jobSalaryMax = IntField(required=True)
    jobSalaryMaxFlag = IntField(required=True)
    jobWorkLocId = IntField(required=True)
    jobWorkLocIdFlag = IntField(required=True)
    jobDiplomaId = IntField(required=True)
    jobDiplomaIdFlag = IntField(required=True)

    jobWorkAgeMin = LongField(required=True)
    jobWorkAgeMinFlag = IntField(required=True)
    jobWorkAgeMax = LongField(required=True)
    jobWorkAgeMaxFlag = IntField(required=True)

    jobCate = StringField(required=True)
    jobCateFlag = IntField(required=True)
    jobMajor = StringField(required=True)
    jobMajorFlag = IntField(required=True)

    isExpired = IntField(required=True, default=0)

    meta = {
        "collection": 'jd_measure',
        "db_alias":"jd_51job_measure",
    }

def convertToJdMeasure(owner, item):
    jdMeasure = JdMeasure()
    jdMeasure.jdId = item.jdId
    jdMeasure.jdFrom = owner
    jdMeasure.pubDateStamp = item.pubDate.timeStamp
    jdMeasure.pubDateUtcOffset = item.pubDate.utcOffset
    jdMeasure.pubDateFlag = item.pubDate.flag

    jdMeasure.incSegmentId = item.jdInc.incSegmentId.value
    jdMeasure.incSegmentIdFlag = item.jdInc.incSegmentId.flag
    jdMeasure.incIndustryId = item.jdInc.incIndustryId.value
    jdMeasure.incIndustryIdFlag = item.jdInc.incIndustryId.flag
    jdMeasure.incType = item.jdInc.incType.value
    jdMeasure.incTypeFlag = item.jdInc.incType.flag
    jdMeasure.incScaleMin = item.jdInc.incScaleMin.value
    jdMeasure.incScaleMinFlag = item.jdInc.incScaleMin.flag
    jdMeasure.incScaleMax = item.jdInc.incScaleMax.value
    jdMeasure.incScaleMaxFlag = item.jdInc.incScaleMax.flag

    jdMeasure.jobCate = item.jdJob.jobCate.value
    jdMeasure.jobCateFlag = item.jdJob.jobCate.flag
    jdMeasure.jobDiplomaId = item.jdJob.jobDiplomaId.value
    jdMeasure.jobDiplomaIdFlag = item.jdJob.jobDiplomaId.flag
    jdMeasure.jobMajor = item.jdJob.jobMajor.value
    jdMeasure.jobMajorFlag = item.jdJob.jobMajor.flag
    jdMeasure.jobSalaryMin = item.jdJob.jobSalaryMin.value
    jdMeasure.jobSalaryMinFlag = item.jdJob.jobSalaryMin.flag
    jdMeasure.jobSalaryMax = item.jdJob.jobSalaryMax.value
    jdMeasure.jobSalaryMaxFlag = item.jdJob.jobSalaryMax.flag
    jdMeasure.jobWorkLocId = item.jdJob.jobWorkLocId.value
    jdMeasure.jobWorkLocIdFlag = item.jdJob.jobWorkLocId.flag
    jdMeasure.jobWorkAgeMin = item.jdJob.jobWorkAgeMin.value
    jdMeasure.jobWorkAgeMinFlag = item.jdJob.jobWorkAgeMin.flag
    jdMeasure.jobWorkAgeMax = item.jdJob.jobWorkAgeMax.value
    jdMeasure.jobWorkAgeMaxFlag = item.jdJob.jobWorkAgeMax.flag

    return jdMeasure
