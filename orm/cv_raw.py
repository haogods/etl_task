#!/usr/bin/env python
# -*- coding:utf-8 -*-

from mongoengine import * #Document, EmbeddedDocument, StringField, MapField, EmbeddedDocumentField, ListField
from ipin.rpc.etl.cv.cv_type.ttypes import CvRaw, CvBaseInfoRaw, CvJobExpRaw, CvEduItemRaw, CvJobItemRaw, CvProItemRaw, CvTrainItemRaw
from ipin.rpc.etl.cv.cv_type.ttypes import CvLanguageItemRaw, CvCertItemRaw, CvSkillItemRaw, CvPrivateInfoRaw, CvPositionItemRaw
import mongoengine


class BaseInfo(EmbeddedDocument):

    age = StringField(required=True, default='')
    gender = StringField(required=True, default='')
    dob = StringField(required=True, default='')
    height = StringField(required=True, default='')
    marriage = StringField(required=True, default='')
    nowAddress = StringField(required=True, default='')
    nowDiploma = StringField(required=True, default='')
    nowHukou = StringField(required=True, default='')
    nowInc = StringField(required=True, default='')
    nowIndustry = StringField(required=True, default='')
    nowPoliStatus = StringField(required=True, default='')
    nowPosition = StringField(required=True, default='')
    nowSalary = StringField(required=True, default='')
    nowWorkAge = StringField(required=True, default='')
    nowZipCode = StringField(required=True, default='')
    nowLocation = StringField(required=True, default='')

    overSea = StringField(required=True, default='')
    recentMajorName = StringField(required=True, default='')
    recentSchName = StringField(required=True, default='')
    recentJobDuration = StringField(required=True, default='')
    updateTime = StringField(required=True, default='')

    # 新增
    nowAddressDetail = StringField(required=True, default='')
    country = StringField(required=True, default='')
    baseSalary = StringField(required=True, default='')
    bonus = StringField(required=True, default='')
    subsidy = StringField(required=True, default='')
    stock = StringField(required=True, default='')
    credit = StringField(required=True, default='')
    annualIncome = StringField(required=True, default='') # 年薪


class MCvJobExp(EmbeddedDocument):
    # cvId = StringField(required=True)
    dutyTime = StringField(required=True, default='')
    expIndustrys = StringField(required=True, default='')
    expJobCates = StringField(required=True, default='')
    expJobTypes = StringField(required=True, default='')
    expLocations = StringField(required=True, default='')
    expSalary = StringField(required=True, default='')
    ignoreIncs = StringField(required=True, default='')
    workStatus = StringField(required=True, default='')
    expPositions = StringField(required=True, default='')
    notes = StringField(required=True, default='')


class MCvEduItem(EmbeddedDocument):

    itemId = StringField(required=True, default='')
    eduStart = StringField(required=True, default='')
    eduEnd = StringField(required=True, default='')
    schName = StringField(required=True, default='')
    majorName = StringField(required=True, default='')
    eduDesc = StringField(required=True, default='')
    eduDiploma = StringField(required=True, default='')

    # 新增
    eduTongZhao = StringField(required=True, default='')
    eduType = StringField(required=True, default='')


class MCvPositionItem(EmbeddedDocument):

    itemId = StringField(required=True, default='')
    positionStart = StringField(required=True, default='')
    positionEnd = StringField(required=True, default='')
    jobPosition = StringField(required=True, default='')

    location = StringField(required=True, default='')
    department = StringField(required=True, default='')
    report = StringField(required=True, default='')
    subTimeSize = StringField(required=True, default='')
    salary = StringField(required=True, default='')
    jobDesc = StringField(required=True, default='')
    jobAchievement = StringField(required=True, default='')


class MCvJobItem(EmbeddedDocument):
    cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    incName = StringField(required=True, default='')
    incDesc = StringField(required=True, default='')
    incEmployee = StringField(required=True, default='')
    incIndustrys =  StringField(required=True, default='')
    incType = StringField(required=True, default='')
    incLocation = StringField(required=True, default='')
    jobStart = StringField(required=True, default='')
    jobEnd = StringField(required=True, default='')
    jobDuration =StringField(required=True, default='')
    jobSalary = StringField(required=True, default='')
    jobDepartment = StringField(required=True, default='')
    jobPosition = StringField(required=True, default='')
    jobDesc = StringField(required=True, default='')
    jobSubTeamSize = StringField(required=True, default='')
    jobAchievement = StringField(required=True, default='')

    #2016-03-23 新增
    reportTo = StringField(required=True, default='')
    underNum = StringField(required=True, default='')
    witness = StringField(required=True, default='')
    leaveReason = StringField(required=True, default='')
    achieveDesc = StringField(required=True, default='')
    positionList = ListField(EmbeddedDocumentField(MCvPositionItem))
    directUnder = StringField(required=True, default='')
    yearlyIncome = StringField(required=True, default='')


class MCvProItem(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    proStart = StringField(required=True, default='')
    proEnd = StringField(required=True, default='')
    proName = StringField(required=True, default='')
    proDuty = StringField(required=True, default='')
    proDesc = StringField(required=True, default='')
    softwareEnv = StringField(required=True, default='')
    hardwareEnv = StringField(required=True, default='')
    devTool = StringField(required=True, default='')
    proAchievement = StringField(required=True, default='')

    # 新增
    proIncName = StringField(required=True, default='')
    proRole = StringField(required=True, default='')


class MCvTrainItem(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    trainStart = StringField(required=True, default='')
    trainEnd = StringField(required=True, default='')
    trainAgency = StringField(required=True, default='')
    trainTitle = StringField(required=True, default='')
    trainContent = StringField(required=True, default='')
    trainDesc = StringField(required=True, default='')
    trainLoc = StringField(required=True, default='')
    trainCert = StringField(required=True, default='')


class MCvLanguageItem(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    languageName = StringField(required=True, default='')
    languageLevel = StringField(required=True, default='')


class MCvCertItem(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    certTime = StringField(required=True, default='')
    certName = StringField(required=True, default='')
    certLevel = StringField(required=True, default='')


class MCvSkillItem(EmbeddedDocument):
    # cvId = StringField(required=True)
    itemId = StringField(required=True, default='')
    skillName = StringField(required=True, default='')
    skillLevel = StringField(required=True, default='')
    skillDuration = StringField(required=True, default='')


class MCvPrivateInfo(EmbeddedDocument):
    # cvId = StringField(required=True)
    keywords = ListField(StringField(), required=True, default='')
    userName = StringField(required=True, default='')
    phoneNumber = StringField(required=True, default='')
    email = StringField(required=True, default='')
    qq = StringField(required=True, default='')
    idNumber = StringField(required=True, default='')

    #新增加
    wechatNum = StringField(required=True, default='')
    homePage = StringField(required=True, default='')


docMap = {

    'baseInfo':BaseInfo,
    'jobExp': MCvJobExp,
    'eduList': MCvEduItem,
    'jobList': MCvJobItem,
    'proList': MCvProItem,
    'trainList': MCvTrainItem,
    'languageList': MCvLanguageItem,
    'certList' : MCvCertItem,
    'skillList' : MCvSkillItem,
    'privateInfo': MCvPrivateInfo,

}


class MongoEngineCvRaw(Document):

    cvId = StringField(required=True, unique=True)
    cvFrom = StringField(required=True)

    updateTime = LongField(required=True)
    contentSign = StringField(required=True)

    baseInfo = MapField(EmbeddedDocumentField(BaseInfo))
    jobExp = MapField(EmbeddedDocumentField(MCvJobExp))
    eduList = ListField(EmbeddedDocumentField(MCvEduItem))
    jobList = ListField(EmbeddedDocumentField(MCvJobItem))
    proList = ListField(EmbeddedDocumentField(MCvProItem))
    trainList = ListField(EmbeddedDocumentField(MCvTrainItem))
    languageList = ListField(EmbeddedDocumentField(MCvLanguageItem))
    certList = ListField(EmbeddedDocumentField(MCvCertItem))
    skillList = ListField(EmbeddedDocumentField(MCvSkillItem))
    privateInfo = MapField(EmbeddedDocumentField(MCvPrivateInfo))
    others = MapField(StringField())

    meta = {
        "collection": "cv_raw",
        "db_alias": "cv_51job_raw",
    }


def constructCvRawObj(ps):
    raw = MongoEngineCvRaw()

    for key1, value1 in ps.items():

            #额外处理
        if key1 in ['others']:
            continue

        cls = docMap.get(key1, '')
        if not cls:
            continue

        if isinstance(value1, dict):
            obj = cls()
            for key2, value2 in value1.items():
                if value2 in [None, 'None']:
                    value2 = ''
                setattr(obj, key2, value2)
            setattr(raw, key1, obj)

        if isinstance(value1, list) and key1 != 'jobList':
            all_items = []
            for item in value1:
                obj = cls()
                if isinstance(item, dict):
                    for key3, value3 in item.items():
                        if value3 in [None, 'None']:
                            value3 = ''
                        setattr(obj, key3, value3)
                    all_items.append(obj)

            setattr(raw, key1, all_items)

        if key1 == 'jobList':
            all_items = []
            for item in value1:
                obj = MCvJobItem()
                for key2, value2 in item.items():

                    # 获取positionList
                    if key2 == 'positionList':
                        positionList = []
                        for positionItem in value2:
                            obj2 = MCvPositionItem()
                            for key3, value3 in positionItem.items():
                                if value3 in [None, 'None']:
                                    value3 = ''
                                setattr(obj2, key3, value3)
                            positionList.append(obj2)
                        setattr(obj, key2, positionList)
                        continue

                    elif value2 in [None, 'None']:
                        value2 = ''

                    setattr(obj, key2, value2)
                # jobItems
                all_items.append(obj)

            setattr(raw, key1, all_items)

    if "others" in ps:
        setattr(raw, 'others', ps.get('others'))

    return raw


def convertToCvRaw(cvRaw_obj):
    rs = CvRaw()
    rs.cvId = cvRaw_obj.cvId
    rs.cvFrom = cvRaw_obj.cvFrom

    rs.baseInfo = CvBaseInfoRaw()
    setCvBaseInfo(rs.baseInfo, cvRaw_obj)

    rs.jobExp = CvJobExpRaw()
    setCvJobExp(rs.jobExp, cvRaw_obj)

    setEduList(rs, cvRaw_obj)
    setJobList(rs, cvRaw_obj)
    setProList(rs, cvRaw_obj)
    setTrainList(rs, cvRaw_obj)
    setLanguageList(rs, cvRaw_obj)
    setCertList(rs, cvRaw_obj)
    setSkillList(rs, cvRaw_obj)

    rs.privateInfo = CvPrivateInfoRaw()
    setPrivateInfo(rs.privateInfo, cvRaw_obj)
    # rs.others = cvRaw_obj.others if cvRaw_obj.others else {}
    setOthers(rs, cvRaw_obj)

    return rs


def setOthers(rs, cvRaw_obj):
    ret = {}
    for key, value in cvRaw_obj.others.items():
        if isinstance(value, list):
            value = "#".join(value)

        ret[key] = value

    rs.others = ret




def setCvBaseInfo(baseInfo, cvRaw_obj):
    if not cvRaw_obj.baseInfo:
        return
    for key in baseInfo.__dict__:
        if key not in cvRaw_obj.baseInfo._fields:
            setattr(baseInfo, key, '')
            continue

        setattr(baseInfo, key, cvRaw_obj.baseInfo[key])


def setCvJobExp(jobExp, cvRaw_obj):
    if not cvRaw_obj.jobExp:
        return

    for key in jobExp.__dict__:
        if key not in cvRaw_obj.jobExp._fields:
            setattr(jobExp, key, '')
        else:
            setattr(jobExp, key, cvRaw_obj.jobExp[key])

def setEduList(rs, cvRaw_obj):
    if not cvRaw_obj.eduList:
        return

    total_list = []
    for eduItem in cvRaw_obj.eduList:
        tmp_eduItem = CvEduItemRaw()
        for key in tmp_eduItem.__dict__:
            if key not in eduItem:
                setattr(tmp_eduItem, key, '')
            else:
                setattr(tmp_eduItem, key, eduItem[key])

        total_list.append(tmp_eduItem)
    setattr(rs, 'eduList', total_list)

def setJobList(rs, cvRaw_obj):
    if not cvRaw_obj.jobList:
        return

    total_list = []
    for jobItem in cvRaw_obj.jobList:
        tmp_jobItem = CvJobItemRaw()
        for key in tmp_jobItem.__dict__:

            # positionList 额外处理
            if key == 'positionList':
                positionList = []
                for positionItem in jobItem.positionList:
                    tmp_positionItem = CvPositionItemRaw()
                    for key1 in tmp_positionItem.__dict__:
                        if key1 not in positionItem:
                            setattr(tmp_positionItem, key1, '')
                        else:
                            setattr(tmp_positionItem, key1, positionItem[key1])
                    positionList.append(tmp_positionItem)
                setattr(tmp_jobItem, key, positionList)
                continue

            if key not in jobItem:
                setattr(tmp_jobItem, key, '')
            else:
                setattr(tmp_jobItem, key, jobItem[key])

        total_list.append(tmp_jobItem)
    setattr(rs, 'jobList', total_list)

def setProList(rs, cvRaw_obj):
    if not cvRaw_obj.proList:
        return

    total_list = []
    for proItem in cvRaw_obj.proList:
        tmp_proItem = CvProItemRaw()
        for key in tmp_proItem.__dict__:
            if key not in proItem:
                setattr(tmp_proItem, key, '')
            else:
                setattr(tmp_proItem, key, proItem[key])

        total_list.append(tmp_proItem)
    setattr(rs, 'proList', total_list)

def setTrainList(rs, cvRaw_obj):
    if not cvRaw_obj.trainList:
        return

    total_list = []
    for trainItem in cvRaw_obj.trainList:
        tmp_trainItem = CvTrainItemRaw()
        for key in tmp_trainItem.__dict__:
            if key not in trainItem:
                setattr(tmp_trainItem, key, '')
            else:
                try:
                    setattr(tmp_trainItem, key, trainItem[key])
                except Exception as e:
                    print e
                    setattr(tmp_trainItem, key, getattr(trainItem, key))

        total_list.append(tmp_trainItem)
    setattr(rs, 'trainList', total_list)


def setLanguageList(rs, cvRaw_obj):
    if not cvRaw_obj.languageList:
        return

    total_list = []
    for languageItem in cvRaw_obj.languageList:
        tmp_languageItem = CvLanguageItemRaw()
        for key in tmp_languageItem.__dict__:
            if key not in languageItem:
                setattr(tmp_languageItem, key, '')
            else:
                setattr(tmp_languageItem, key, languageItem[key])

        total_list.append(tmp_languageItem)
    setattr(rs, 'languageList', total_list)


def setCertList(rs, cvRaw_obj):
    if not cvRaw_obj.certList:
        return

    total_list = []
    for certItem in cvRaw_obj.certList:
        tmp_certItem = CvCertItemRaw()
        for key in tmp_certItem.__dict__:
            if key not in certItem:
                setattr(tmp_certItem, key, '')
            else:
                setattr(tmp_certItem, key, certItem[key])

        total_list.append(tmp_certItem)
    setattr(rs, 'certList', total_list)


def setSkillList(rs, cvRaw_obj):
    if not cvRaw_obj.skillList:
        return

    total_list = []
    for skillItem in cvRaw_obj.skillList:
        tmp_skillItem = CvSkillItemRaw()
        for key in tmp_skillItem.__dict__:
            if key not in skillItem:
                setattr(tmp_skillItem, key, '')
            else:
                setattr(tmp_skillItem, key, skillItem[key])

        total_list.append(tmp_skillItem)
    setattr(rs, 'skillList', total_list)


def setPrivateInfo(privateInfo, cvRaw_obj):
    if not cvRaw_obj.privateInfo:
        return

    for key in privateInfo.__dict__:
        if key not in cvRaw_obj.privateInfo._fields:
            setattr(privateInfo, key, '')
        else:
            setattr(privateInfo, key, cvRaw_obj.privateInfo[key])

if __name__ == '__main__':
    ps = {"baseInfo": {
            'age': '1',
            "gender": '1',
            'dob':'1',
            },
        "cvId": "cv_51job://11111",
        "jobExp":{

            "expLocations":"深圳",
            "expSalary": "10000",
            "expJobTypes":"全职",

        },
        "eduList":[
            {
                "itemId":'1',
                "eduStart": '111',
                "eduEnd":'222',
                "schName":'haerbin',
            },
            {
                "itemId":'2',
                "eduStart": '111',
                "eduEnd":'222',
                "schName":'haerbin',
            }

        ],

        "jobList": [
            {
                "itemId":"1",
                "incName":"湖北",
                "incDesc":"天下第一",
                "incEmployee": "100-200",
                "incIndustrys": "internet",
                "positionList":[

                    {"1": 2, 'a': 'b'},
                    {"3": 4},

                ]
            },
            {
                "itemId":"2",
                "incName":"湖北",
                "incDesc":"天下第一",
                "incEmployee": "100-200",
                "incIndustrys": "internet",
            }
        ],
        "proList": [

            {
                "itemId":"1",
                "proStart":"1111",
                "proEnd": "2222",
            },
            {
                "itemId":"2",
                "proStart":"1111",
                "proEnd": "2222",
            }

        ],

        "trainList":[

            {
                "itemId":"1",
                "trainStart":"111",
                "trainEnd":"2222",
            },
            {
                "itemId":"2",
                "trainStart":"111",
                "trainEnd":"2222",

            }
        ],
        "languageList":[

            {
                "item":"1",
                "languageName": "en",
                "languageLevel": "2"
            },
            {
                "item":"2",
                "languageName": "en",
                "languageLevel": "2"
            }

        ],
        "certList":[
            {
                "itemId":"1",
                "certTime":"1111",
                "certName":"钓鱼",
                "certLevel": "2",
            },
            {
                "itemId":"2",
                "certTime":"1111",
                "certName":"钓鱼",
                "certLevel": "2",
            }
        ],
        "skillList":[
            {
                "itemId":"1",
                "skillName":"1111",
                "skillLevel":"钓鱼",
                "skillDuration": "2",
            },
            {
               "itemId":"2",
                "skillName":"1111",
                "skillLevel":"钓鱼",
                "skillDuration": "2",
            }
        ],
        "privateInfo":{

            "keywords": ["123,456", "555"],
            "userName": "jainghao",
            "phoneNumber":"15815562477"
        },
        "others":{
            "wo":"god",

        }

        }

    s = MongoEngineCvRaw()
    raw = constructCvRawObj(ps)
    raw.cvId = "cv_51job://11111"
    raw.cvFrom = "cv_51job"

    cvRaw = convertToCvRaw(raw)
    print cvRaw