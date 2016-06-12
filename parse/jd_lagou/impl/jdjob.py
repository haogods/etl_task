#!/usr/bin/python
# -*- coding:utf8 -*-

from ipin.rpc.etl.jd.jd_type.ttypes import JdJobRaw
from ipin.rpc.common.datatype.ttypes import DtString
from ipin.infrastructure.datatype.flag import DataFlag
from parse.format import content_format
from lxml import etree
import re

def parse(doc):
    """
   - jobPosition: 职位名称
   - jobSalary: 薪水
   - jobWorkLoc: 工作地区
   - jobDiploma: 学历要求
   - jobWorkAge: 工作经验
   - jobDescription: 工作描述
   - jobCate: 职能
   - jobType: 工作类型
   - jobWelfare: 工作福利
    """
    ret = JdJobRaw()
    ret.jobPosition = _get_job_position(doc)
    ret.jobDescription = _get_job_desc(doc)
    ret.jobCate = _get_job_cate(doc)
    ret.jobType = _get_job_type(doc)
    ret.jobWelfare = _get_job_welfare(doc)
    _set_job_others(ret, doc)
    return ret


def _get_job_welfare(doc):

    jobWelfare = DtString()
    lagou_welfares = doc.xpath("//dd[@class='job_request']")
    for el in lagou_welfares:
        text = el.text_content().strip()
        if isinstance(text, unicode):
            text = text.encode("utf-8")
        find = re.search(r'职位诱惑 :(.*)\n', text)
        if find:
            m = find.group(1)
            if isinstance(m, str):
                m = m.decode('utf-8')

            jobWelfare = DtString(m, DataFlag(hasValue=True).toByte())
    return jobWelfare


def _get_job_type(doc):
    jobType = DtString()
    els = doc.xpath("//dd[@class='job_request']//span")
    for span in els:
        tag = span.text_content()
        if isinstance(tag, unicode):
            tag = tag.encode('utf-8')

        if "兼职" in tag:
            jobType = DtString(u"兼职", DataFlag(hasValue=True).toByte())
            break
        elif "实习" in tag:
            jobType = DtString(u"实习", DataFlag(hasValue=True).toByte())
            break
        else:
            jobType = DtString(u"全职", DataFlag(hasValue=True).toByte())
            break
    return jobType


def _get_job_position(doc):
    els = doc.xpath("//div[@id='container']/div/div")
    for el in els:
        clzName = el.attrib.get("class","")
        if "content_l" in clzName and "content_l_jobdetail" in clzName:
            subEls = el.xpath("dl[@class='job_detail']/dt/h1/div")
            for subEl in subEls:
                position = subEl.tail.strip()

                ## position contains ...
                if re.match(".*\.$", position):
                    els = doc.xpath("//dl[@class='job_detail']/dt/h1/@title")
                    for el in els:
                        position = el
                        break
                return DtString(position, DataFlag(hasValue=True).toByte())

    return DtString()


def _set_job_others(ret,doc):
    els = doc.xpath("//div[@id='container']/div/div")
    for el in els:
        clzName = el.attrib.get("class","")
        if "content_l" in clzName and "content_l_jobdetail" in clzName:
            subEls = el.xpath("dl[@class='job_detail']/dd[@class='job_request']/span")
            if len(subEls) > 0:
                ret.jobSalary = DtString(subEls[0].text_content().strip(),DataFlag(hasValue=True).toByte())
            if len(subEls) > 1:
                ret.jobWorkLoc = DtString(subEls[1].text_content().strip(),DataFlag(hasValue=True).toByte())
            if len(subEls) > 2:
                ret.jobWorkAge = DtString(subEls[2].text_content().strip(),DataFlag(hasValue=True).toByte())
            if len(subEls) > 3:
                ret.jobDiploma =DtString(subEls[3].text_content().strip(),DataFlag(hasValue=True).toByte())


job_desc_patterns = ["//dl[@class='job_detail']/dd[@class='job_bt']/p | //dl[@class='job_detail']/dd[@class='job_bt']/h3 | "
                     "//dl[@class='job_detail']/dd[@class='job_bt']/ul/li"]

job_desc_pattern_other = "//dl[@class='job_detail']/dd[@class='job_bt']"


def _get_job_desc(doc):
    for pt in job_desc_patterns:
        ps = doc.xpath(pt)
        r = ""
        for p in ps:
            r += etree.tounicode(p, pretty_print=True)
        r = content_format(r)
        removed_special_token_content = re.sub(ur'岗位要求|岗位职责|工作描述|职位描述| ', '', r)
        if len(removed_special_token_content) < 10:
            continue
        return DtString(r, DataFlag(hasValue=True).toByte())

    els = doc.xpath(job_desc_pattern_other)
    r = ""

    for el in els:
      r += etree.tounicode(el, pretty_print=True)
      r = content_format(r)
      return DtString(r, DataFlag(hasValue=True).toByte())

    return DtString()


def _get_job_cate(doc):
    #TODO
    return DtString()
