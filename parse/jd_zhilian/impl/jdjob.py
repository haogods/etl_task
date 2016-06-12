#!/usr/bin/python
# -*- coding:utf8 -*-

from ipin.rpc.etl.jd.jd_type.ttypes import JdJobRaw
from ipin.rpc.common.datatype.ttypes import DtString
from ipin.infrastructure.datatype.flag import DataFlag
from parse.format import content_format


job_desc_patterns = ["//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][1]/div | //div[@class='tab-cont-box']//div[@class='tab-inner-cont'][1]/div/p",
            "//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][1]//p",
            "//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][1]/div",
            "//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][1]",]


def parse(doc):
    """
   - jobPosition: 职位名称
   - jobSalary: 薪水
   - jobWorkLoc: 工作地区
   - jobDiploma: 学历要求
   - jobWorkAge: 工作经验
   - jobDescription: 工作描述
   - jobCate: 职能
   - jobWelfare: 工作福利
   - jobType: 工作性质
    """
    ret = JdJobRaw()
    ret.jobPosition = _get_job_position(doc)
    ret.jobDescription = _get_job_desc(doc)
    ret.jobWelfare = _get_job_welfare(doc)
    _set_job_others(ret, doc)
    return ret


def _get_job_welfare(doc):
    jobWelfare = DtString()
    spans = doc.xpath("//div[@class='welfare-tab-box']//span")
    tags =[]
    for span in spans:
        tags.append(span.text_content().strip())

    if len(tags) > 0:
        jobWelfare = DtString(",".join(tags), DataFlag(hasValue=True).toByte())
    return jobWelfare


def _get_job_position(doc):
    divs = doc.xpath("//div[@class='top-fixed-box']/div[@class='fixed-inner-box']/div")
    for div in divs:
        clzName = div.attrib.get("class","")
        if "inner-left" in clzName:
            els = div.xpath("h1")
            for el in els:
                return DtString(el.text_content().strip(), DataFlag(hasValue=True).toByte())
    return DtString()


def fetch_content_by_patterns(doc, patterns):

    for pat in patterns:
        els = doc.xpath(pat)
        r = ""
        for el in els:
            r += el.text_content()
            r += '\n'

        r = content_format(r)
        if len(r) < 10:
            continue
        return r
    return ""


def _get_job_desc(doc):
    desc = fetch_content_by_patterns(doc, job_desc_patterns)
    if desc:
        return DtString(desc, DataFlag(hasValue=True).toByte())
    return DtString()


def _set_job_others(ret,doc):
    divs = doc.xpath("//div")
    for div in divs:
        clzName = div.attrib.get("class","")
        if "terminalpage" in clzName:
            uls = div.xpath("div[@class='terminalpage-left']/ul")
            for ul in uls:
                clzName = ul.attrib.get("class","")
                if "terminal-ul" in clzName:
                    lis = ul.xpath("li")
                    for li in lis:
                        kels = li.xpath("span")
                        vels = li.xpath("strong")
                        if not kels or not vels:
                            continue
                        key = kels[0].text_content().strip()
                        value = vels[0].text_content().strip()
                        _set_job_others_detail(ret,key,value)


def _set_job_others_detail(ret,key,value):
    if isinstance(key, unicode):
        key = key.encode("utf8")
    if "职位月薪：" == key:
        ret.jobSalary = DtString(value, DataFlag(hasValue=True).toByte())
    if "工作地点：" == key:
        ret.jobWorkLoc = DtString(value, DataFlag(hasValue=True).toByte())
    if "工作性质：" == key:
        ret.jobType = DtString(value, DataFlag(hasValue=True).toByte())
    if "工作经验：" == key:
        ret.jobWorkAge = DtString(value, DataFlag(hasValue=True).toByte())
    if "最低学历："  == key:
        ret.jobDiploma = DtString(value, DataFlag(hasValue=True).toByte())
    if "招聘人数：" == key:
        pass
    if "职位类别：" == key:
        ret.jobCate = DtString(value, DataFlag(hasValue=True).toByte())
