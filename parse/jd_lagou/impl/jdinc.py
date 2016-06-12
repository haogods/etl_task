#!/usr/bin/python
# -*- coding:utf8 -*-

from ipin.rpc.etl.jd.jd_type.ttypes import JdIncRaw
from ipin.rpc.common.datatype.ttypes import DtString
from ipin.infrastructure.datatype.flag import DataFlag

def parse(doc):
    """
   - incName: 公司名称
   - incIndustry: 行业
   - incType: 公司类型
   - incScale: 公司规模
   - incIntro: 公司介绍
    """
    ret = JdIncRaw()
    ret.incName = _get_inc_name(doc)
    _set_inc_industry_and_scale(ret, doc)
    ret.incType = _get_inc_type(doc)
    ret.incIntro = _get_inc_intro(doc)
    ret.incUrl = _get_inc_url(doc)
    return ret


def _get_inc_name(doc):

    #　先取左边公司名
    els = doc.xpath("//dl[@class='job_company']/dt/a/@alt")
    if len(els) > 0:
        return DtString(els[0], DataFlag(hasValue=True).toByte())

    els = doc.xpath("//div[@id='container']/div/div")
    for el in els:
        clzName = el.attrib.get("class","")
        if "content_l" in clzName and "content_l_jobdetail" in clzName:
            subEls = el.xpath("dl[@class='job_detail']/dt/h1/div")
            for subEl in subEls:
                if subEl.text:
                    return DtString(subEl.text,DataFlag(hasValue=True).toByte())
    return DtString()


def _set_inc_industry_and_scale(ret, doc):
    els = doc.xpath("//div[@id='container']/div/div[@class='content_r']/dl[@class='job_company']/dd/ul")
    for el in els:
        clzName = el.attrib.get("class","")
        if "c_feature" in clzName and "reset" in clzName:
            subEls = el.xpath("li/span")
            for subEl in subEls:
                key = subEl.text_content().strip()
                if isinstance(key,unicode):
                    key = key.encode("utf8")
                if "领域" == key:
                    ret.incIndustry = DtString(subEl.tail,DataFlag(hasValue=True).toByte())
                elif "规模" == key:
                    ret.incScale = DtString(subEl.tail,DataFlag(hasValue=True).toByte())


def _get_inc_url(doc):
    inc_url = DtString()
    els = doc.xpath("//dl[@class='job_company']/dt/a/@href")

    if len(els) > 0:
        inc_url = DtString(els[0], DataFlag(hasValue=True).toByte())

    return inc_url



def _get_inc_type(doc):
    # inc_type = DtString()
    # inc_info = DtString()
    # els = doc.xpath("//dl[@class='job_company']/dt/a/@href")
    # if not els:
    #     return inc_type, inc_info



    #TODO
    return DtString()


def _get_inc_intro(doc):
    #TODO
    return DtString()
