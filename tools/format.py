# coding=utf-8

import re
from ipin.rpc.etl.jd.jd_type.ttypes import JdRaw, JdIncRaw, JdJobRaw
from ipin.rpc.etl.jd.remedy.ttypes import JdRemedyRaw
from ipin.rpc.common.datatype.ttypes import DtString

def content_format(r):
    r = re.sub('<.*?>', "\n", r)   #去掉tag
    r = re.sub(r'[ ]+', " ", r)   #去掉多个空格
    r = re.sub(r'\?xml:namespace|&#13;', '', r)
    r = re.sub(r"[\n\t\r]+ *", '\n', r)  #多个换行 替换为2个
    r = re.sub(r'^[\t\n\r ]+|[\t\n\r ]+$', '', r)
    return r

def getFlag(item):
    if item:
        return 1
    return 0

def construct_jd_raw(item):
    jd_raw = JdRaw()
    jd_raw.jdId = item.jdId
    jd_raw.jdFrom = item.jdFrom
    jd_raw.jdUrl = item.jdUrl
    jd_raw.pubDate = DtString(item.pubDate, getFlag(item.pubDate))

    # 公司信息
    jd_inc = JdIncRaw()
    jd_inc.incName = DtString(item.incName, getFlag(item.incName))
    jd_inc.incType = DtString(item.incType, getFlag(item.incType))
    jd_inc.incIntro = DtString(item.incIntro, getFlag(item.incIntro))
    jd_inc.incIndustry = DtString(item.incIndustry, getFlag(item.incIndustry))
    jd_inc.incScale = DtString(item.incScale, getFlag(item.incScale))
    jd_inc.incUrl = DtString() # 量化不需要

    # 职位信息
    job_info = JdJobRaw()
    job_info.jobPosition = DtString(item.jobPosition, getFlag(item.jobPosition))
    job_info.jobSalary = DtString(item.jobSalary, getFlag(item.jobSalary))
    job_info.jobWorkLoc = DtString(item.jobWorkLoc, getFlag(item.jobWorkLoc))
    job_info.jobDiploma = DtString(item.jobDiploma, getFlag(item.jobDiploma))
    job_info.jobWorkAge = DtString(item.jobWorkAge, getFlag(item.jobWorkAge))
    job_info.jobDescription = DtString() #量化不需要
    job_info.jobCate = DtString(item.jobCate, getFlag(item.jobCate))
    job_info.jobType = DtString() #量化不需要
    job_info.jobWelfare = DtString() #量化不需要

    jd_raw.jdJob = job_info
    jd_raw.jdInc = jd_inc

    remedyInfo = JdRemedyRaw()
    remedyInfo.age = DtString()
    remedyInfo.cert = [DtString()]
    remedyInfo.diploma = DtString()
    remedyInfo.endTime = DtString()
    remedyInfo.gender = DtString()
    remedyInfo.incName = DtString()
    remedyInfo.incTags = [DtString()]
    remedyInfo.jdId = ""
    remedyInfo.jobPosition = DtString()
    remedyInfo.jobTags = [DtString()]
    remedyInfo.jobWelfare = DtString()
    remedyInfo.major = DtString()
    remedyInfo.pubTime = DtString()
    remedyInfo.salary = DtString()
    remedyInfo.skills = [DtString()]
    remedyInfo.workDemand = DtString()
    remedyInfo.workLoc = DtString()
    remedyInfo.workDuty = DtString()
    remedyInfo.workExp = DtString()

    jd_raw.remedyInfo = remedyInfo

    return jd_raw