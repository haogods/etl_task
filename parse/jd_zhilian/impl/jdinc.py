#!/usr/bin/python
# -*- coding:utf8 -*-

from ipin.rpc.etl.jd.jd_type.ttypes import JdIncRaw
from ipin.rpc.common.datatype.ttypes import DtString
from ipin.infrastructure.datatype.flag import DataFlag
from parse.format import content_format


company_patterns = ["//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][2]/div | "
                    "//div[@class='tab-cont-box']//div[@class='tab-inner-cont'][2]/p"]

def parse(doc):
    """
   - incName: 公司名称
   - incIndustry: 行业
   - incType: 公司类型
   - incScale: 公司规模
   - incIntro: 公司介绍
   - incUrl: 公司Url
    """
    ret = JdIncRaw()
    ret.incName, ret.incUrl = _get_inc_name_url(doc)
    ret.incIntro = _get_inc_intro(doc)
    _set_inc_others(ret, doc)
    return ret


def _get_inc_name_url(doc):
    els = doc.xpath("//div[@class='top-fixed-box']/div[@class='fixed-inner-box']/div/h2/a")
    for el in els:
        return DtString(el.text_content().strip(), DataFlag(hasValue=True).toByte()),\
               DtString(el.get("href").strip(), DataFlag(hasValue=True).toByte())

    return DtString(), DtString()


def _get_inc_intro(doc):

    for pat in company_patterns:
        els = doc.xpath(pat)
        r = ""
        for el in els:
            r += el.text_content()
            r += '\n'

        r = content_format(r)

        if len(r)>10:
            return DtString(r, DataFlag(hasValue=True).toByte())

    return DtString()


def _set_inc_others(ret,doc):
    uls = doc.xpath("//div[@class='company-box']/ul")
    for ul in uls:
        clzName = ul.attrib.get("class","")
        if "terminal-company" in clzName:
            lis = ul.xpath("li")
            for li in lis:
                kels = li.xpath("span")
                vels = li.xpath("strong")
                if not kels or not vels:
                    continue
                key = kels[0].text_content().strip()
                value = vels[0].text_content().strip()
                if isinstance(key, unicode):
                    key = key.encode("utf8")
                if "公司规模：" == key:
                    ret.incScale = DtString(value,DataFlag(hasValue=True).toByte())
                if "公司性质：" == key:
                    ret.incType = DtString(value,DataFlag(hasValue=True).toByte())
                if "公司行业：" == key:
                    ret.incIndustry = DtString(value,DataFlag(hasValue=True).toByte())
