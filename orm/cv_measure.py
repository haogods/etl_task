#!/usr/bin/env python
# -*- coding:utf-8 -*-

from mongoengine import *
from ipin.rpc.etl.cv.cv_type.ttypes import CvBaseInfoMeasure, CvPrivateInfoMeasure, CvSkillItemMeasure, CvCertItemMeasure, CvProItemMeasure
from ipin.rpc.etl.cv.cv_type.ttypes import CvEduItemMeasure, CvJobExpMeasure, CvJobItemMeasure, CvTrainItemMeasure, CvLanguageItemMeasure, CvPositionItemMeasure
from ipin.rpc.etl.cv.cv_type.ttypes import CvMeasure as ThriftCvMeasure


class MCvBaseInfoMeasure(EmbeddedDocument):
     # cvId = StringField(required=True)
     # cvFrom = StringField(required=True)
     updateTime = LongField(required=True, default=-1)
     marriage = IntField(required=True, default=-1)
     age = IntField(required=True, default=-1)
     dob = LongField(required=True, default=-1)
     height = IntField(required=True, default=-1)
     nowWorkAgeMin = LongField(required=True, default=-1)
     nowWorkAgeMax = LongField(required=True, default=-1)
     nowDiploma = IntField(required=True, default=-1)
     nowLocationId = StringField(required=True, default='')
     nowPoliStatus = IntField(required=True, default=-1)
     nowPosition = StringField(required=True, default='')
     nowHukouLocId = StringField(required=True, default='')
     gender = IntField(required=True, default=-1)
     nowIncId = StringField(required=True, default='')
     nowIndustryId = IntField(required=True, default=-1)
     recentSchId = StringField(required=True, default='')
     recentMajorId = StringField(required=True, default='')
     nowOverSea = BooleanField(required=True)
     recentJobDuration = StringField(required=True, default='')
     nowSalaryMin = IntField(required=True, default=0)
     nowSalaryMax = IntField(required=True, default=0)
     selfEvaluation = StringField(required=True, default='')
     intro = StringField(required=True, default='')
     nowAddress = StringField(required=True, default='')

     #新增
     nowAddressDetail = StringField(required=True, default='')
     country = StringField(required=True, default='')
     baseSalaryMin = IntField(required=True, default='')
     baseSalaryMax = IntField(required=True, default='')
     bonus = IntField(required=True, default='')
     subsidy = IntField(required=True, default='')
     stock = StringField(required=True, default='')
     credit = StringField(required=True, default='')
     annualIncome = IntField(required=True, default='')


class MCvJobExpMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    expLocationIdList = ListField(StringField(), required=True)
    expJobTypeList = ListField(IntField(), required=True)
    expPositionList = ListField(StringField(), required=True)
    expIndustryIdList = ListField(IntField(), required=True)
    expSalaryMin = IntField(required=True, default=-99999999)
    expSalaryMax = IntField(required=True, default=99999999)
    ignoreIncs = StringField(required=True, default='')
    workStatus = IntField(required=True, default='')
    dutyTime = StringField(required=True, default='')
    expJobCates = StringField(required=True, default='')

    notes = StringField(required=True, default='')


class MCvEduItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True)
    eduStart = LongField(required=True, default=-1)
    eduEnd = LongField(required=True, default=-1)
    schId = StringField(required=True, default='')
    majorId = StringField(required=True, default='')
    eduDiploma = IntField(required=True, default=-1)
    eduTongzhao = BooleanField(required=True)
    eduType = IntField(required=True, default=-1)
    eduDesc = StringField(required=True, default='')


class MCvPositionItemMeasure(EmbeddedDocument):

    itemId = StringField(required=True, default='')
    positionStart = IntField(required=True)
    positionEnd = IntField(required=True)
    jobPosition = StringField(required=True, default='')
    location = StringField(required=True, default='')
    department = StringField(required=True, default='')
    report = StringField(required=True, default='')
    subTimeSize = IntField(required=True, default=0)
    salaryMin = IntField(required=True, default=0)
    salaryMax = IntField(required=True, default=0)
    jobDesc = StringField(required=True, default='')
    jobAchievement = StringField(required=True, default='')

class MCvJobItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True)
    incSegmentId = StringField(required=True, default='')
    incEmployeeMin = IntField(required=True, default=0)
    incEmployeeMax = IntField(required=True, default=0)
    incIndustryIdList = ListField(IntField(), required=True)
    incType = IntField(required=True, default=-1)
    incLocationId = StringField(required=True, default='')
    jobStart = LongField(required=True, default=-1)
    jobEnd = LongField(required=True, default=-1)
    jobDuration = LongField(required=True, default=-1)
    jobPosition = StringField(required=True, default='')
    jobDesc = StringField(required=True, default='')
    incDesc = StringField(required=True, default='')
    jobSubTeamSize = IntField(required=True, default=-1)
    jobAchievement = StringField(required=True, default='')
    jobSalaryMin = IntField(required=True, default=0)
    jobSalaryMax = IntField(required=True, default=0)

    # 新增
    reportTo = StringField(required=True, default='')
    underNum = IntField(required=True)
    witness = StringField(required=True, default='')
    leaveReason = StringField(required=True, default='')
    achieveDesc = StringField(required=True, default='')
    positionList = ListField(EmbeddedDocumentField(MCvPositionItemMeasure))
    directUnder = StringField(required=True, default='')
    yearlyIncome = StringField(required=True, default='')


class MCvProItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True)
    proStart = LongField(required=True, default=-1)
    proEnd = LongField(required=True, default=-1)
    proName = StringField(required=True, default='')
    proDuty = StringField(required=True, default='')
    proDesc = StringField(required=True, default='')
    softwareEnv = StringField(required=True, default='')
    hardwareEnv = StringField(required=True, default='')
    devTool = StringField(required=True, default='')
    proAchievement = StringField(required=True, default='')

    proIncName = StringField(required=True, default='')
    proRole = StringField(required=True, default='')


class MCvTrainItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True)
    trainStart = LongField(required=True, default=-1)
    trainEnd = LongField(required=True, default=-1)
    trainAgency = StringField(required=True, default='')
    trainTitle = StringField(required=True, default='')
    trainContent = StringField(required=True, default='')
    trainDesc = StringField(required=True, default='')
    trainLoc = StringField(required=True, default='')
    trainCert = StringField(required=True, default='')


class MCvLanguageItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    languageName = StringField(required=True, default='')
    languageLevel = StringField(required=True, default='')


class MCvCertItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    certTime = LongField(required=True, default=-1)
    certName = StringField(required=True, default='')
    certLevel = StringField(required=True, default='')


class MCvSkillItemMeasure(EmbeddedDocument):
    # cvId = StringField(required=True, default='')
    itemId = StringField(required=True, default='')
    skillName = StringField(required=True, default='')
    skillLevel = StringField(required=True, default='')
    skillDuration = LongField(required=True)


class MCvPrivateInfoMeasure(EmbeddedDocument):
    cvId = StringField(required=True)
    keywords = ListField(StringField(), required=True, default=[])
    userName = StringField(required=True, default='')
    phoneNumber = StringField(required=True, default='')
    email = StringField(required=True, default='')
    msn = StringField(required=True, default='')
    blog = StringField(required=True, default='')
    Qq = StringField(required=True, default='')
    idNumber = StringField(required=True, default='')

    homePage = StringField(required=True, default='')
    wechatNum = StringField(required=True, default='')


class MongoEngineCvMeasure(Document):
    cvId = StringField(required=True)
    baseInfo = MapField(EmbeddedDocumentField(MCvBaseInfoMeasure))
    jobExp = MapField(EmbeddedDocumentField(MCvJobExpMeasure))
    eduList = ListField(EmbeddedDocumentField(MCvEduItemMeasure))
    jobList = ListField(EmbeddedDocumentField(MCvJobItemMeasure))
    proList = ListField(EmbeddedDocumentField(MCvProItemMeasure))
    trainList = ListField(EmbeddedDocumentField(MCvTrainItemMeasure))
    languageList = ListField(EmbeddedDocumentField(MCvLanguageItemMeasure))
    certList = ListField(EmbeddedDocumentField(MCvCertItemMeasure))
    skillList = ListField(EmbeddedDocumentField(MCvSkillItemMeasure))
    privateInfo = MapField(EmbeddedDocumentField(MCvPrivateInfoMeasure))


# MONGO_FIELDS = ['cvId', 'baseInfo', 'jobExp', 'eduList', 'jobList', 'proList',
#                 'trainList', 'languageList', 'certList', 'skillList', 'privateInfo']


THRIFT_FIELD_CLS_MAP = {

    'cvId'         : str,
    'baseInfo'     : CvBaseInfoMeasure,
    'jobExp'       : CvJobExpMeasure,
    'eduList'      : CvEduItemMeasure,
    'jobList'      : CvJobItemMeasure,
    'proList'      : CvProItemMeasure,
    'trainList'    : CvTrainItemMeasure,
    'languageList' : CvLanguageItemMeasure,
    'certList'     : CvCertItemMeasure,
    'skillList'    : CvSkillItemMeasure,
    'privateInfo'  : CvPrivateInfoMeasure,

}

# thrift结构 转 mongo存储结构
def constructCvMeasureObj(thrift_cv_measure):
    if not isinstance(thrift_cv_measure, ThriftCvMeasure):
        raise Exception("type error, not Thrift_cv_measure")
    rs = MongoEngineCvMeasure()

    rs.cvId = thrift_cv_measure.cvId
    setBaseInfo(thrift_cv_measure, rs)
    setJobExp(thrift_cv_measure, rs)
    setEduList(thrift_cv_measure, rs)
    setJobList(thrift_cv_measure, rs)
    setProList(thrift_cv_measure, rs)
    setTrainList(thrift_cv_measure, rs)
    setLanguageList(thrift_cv_measure, rs)
    setCertList(thrift_cv_measure, rs)
    setSkillList(thrift_cv_measure, rs)
    setPrivateInfo(thrift_cv_measure, rs)

    return rs


def setBaseInfo(thrift_cv_measure, rs):

    if not thrift_cv_measure.baseInfo:
        return

    rs.baseInfo = MCvBaseInfoMeasure()
    for field, value in thrift_cv_measure.baseInfo.__dict__.items():
        setattr(rs.baseInfo, field, value)


def setJobExp(thrift_cv_measure, rs):

    if not thrift_cv_measure.jobExp:
        return

    rs.jobExp = MCvJobExpMeasure()
    for field, value in thrift_cv_measure.jobExp.__dict__.items():
        setattr(rs.jobExp, field, value)


def setEduList(thrift_cv_measure, rs):

    if not thrift_cv_measure.eduList:
        return

    total_list = []
    for item in thrift_cv_measure.eduList:
        tmp = MCvEduItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'eduList', total_list)


def setJobList(thrift_cv_measure, rs):

    if not thrift_cv_measure.jobList:
        return

    total_list = []
    for item in thrift_cv_measure.jobList:
        tmp = MCvJobItemMeasure()
        for key, value in item.__dict__.items():
            if key == 'positionList':
                positionList = []
                for positionItem in value:
                    position = MCvPositionItemMeasure()
                    for key2, value2 in positionItem.__dict__.items():
                        setattr(position, key2, value2)
                    positionList.append(position)
                setattr(tmp, key, positionList)

            else:
                setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'jobList', total_list)


def setProList(thrift_cv_measure, rs):

    if not thrift_cv_measure.proList:
        return

    total_list = []
    for item in thrift_cv_measure.proList:
        tmp = MCvProItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'proList', total_list)


def setTrainList(thrift_cv_measure, rs):

    if not thrift_cv_measure.trainList:
        return

    total_list = []
    for item in thrift_cv_measure.trainList:
        tmp = MCvTrainItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'trainList', total_list)


def setLanguageList(thrift_cv_measure, rs):

    if not thrift_cv_measure.languageList:
        return

    total_list = []
    for item in thrift_cv_measure.languageList:
        tmp = MCvLanguageItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'languageList', total_list)

def setCertList(thrift_cv_measure, rs):

    if not thrift_cv_measure.certList:
        return

    total_list = []
    for item in thrift_cv_measure.certList:
        tmp = MCvCertItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'certList', total_list)


def setSkillList(thrift_cv_measure, rs):

    if not thrift_cv_measure.skillList:
        return

    total_list = []
    for item in thrift_cv_measure.skillList:
        tmp = MCvSkillItemMeasure()
        for key, value in item.__dict__.items():
            setattr(tmp, key, value)

        total_list.append(tmp)

    setattr(rs, 'skillList', total_list)


def setPrivateInfo(thrift_cv_measure, rs):

    if not thrift_cv_measure.privateInfo:
        return

    rs.privateInfo = MCvPrivateInfoMeasure()
    for field, value in thrift_cv_measure.privateInfo.__dict__.items():
        setattr(rs.privateInfo, field, value)


if __name__ == '__main__':
    s = ThriftCvMeasure()
    s.baseInfo = CvBaseInfoMeasure()
    s.baseInfo.age = 1
    s.privateInfo = CvPrivateInfoMeasure()
    s.privateInfo.keywords = ["12345"]


    cvJobItemMeasure = CvJobItemMeasure()
    cvPositionItemMeasure = CvPositionItemMeasure()
    cvPositionItemMeasure.itemId = 1
    cvPositionItemMeasure.jobDesc = '123456'

    cvJobItemMeasure.positionList = [cvPositionItemMeasure]

    s.jobList = [cvJobItemMeasure]
    # s.jobList.positionList=[{'a':1,'b':2},{'c':1, 'd':2}]

    skill = CvSkillItemMeasure()
    skill.itemId = 1
    skill.skillLevel = 3
    s.skillList.append(skill)

    rs = constructCvMeasureObj(s)
    print rs.to_json()


