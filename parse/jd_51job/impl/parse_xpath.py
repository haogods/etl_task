# coding=utf-8

from orm.jd_raw import JdRaw
from lxml import etree
from lxml import html
from parse.html_find import HtmlFind

def parse(doc):

    doc = doc.decode('utf-8')
    doc = html.fromstring(doc)
    ret = JdRaw()

    ret.incName = _get_inc_name(doc)
    ret.incIntro = _get_inc_intro(doc)
    ret.incUrl = _get_inc_url(doc)
    _set_inc_others(ret, doc)
    _set_job_others(ret, doc)

    return ret


def _set_job_others(ret, doc):
    els = doc.xpath('//div[@class="tCompany_basic_job"]/dl[@class="lineDl"]')
    for dl in els:
        dts = dl.xpath("dt")
        dds = dl.xpath("dd")

        for i in range(0, len(dts)):
            key = dts[i].text_content().strip()
            value = dds[i].text_content().strip()

            if isinstance(key, unicode):
                key = key.encode("utf8")

            if "发布日期：" == key:
                ret.pubDate = value
            if "工作地点：" == key:
                ret.jobWorkLoc = value
            if "招聘人数：" == key:
                ret.jobPersonNumber = value
            if "工作年限：" == key:
                ret.jobWorkAge = value
            if "学历要求：" == key:
                ret.jobDiploma = value
            if "薪资范围：" == key:
                ret.jobSalary = value
            if "薪酬福利：" == key:
                els = dds[i].xpath('span')
                job_welfare_list = []
                for el in els:
                    job_welfare_list.append(el.text_content().strip())

                ret.jobWelfare = ",".join(job_welfare_list)
            if "职能类别：" == key:
                job_cate_list = []
                els = dds[i].xpath('a')
                for el in els:
                    job_cate_list.append(el.text_content().strip())

                ret.jobCate = ",".join(job_cate_list)
            if "职位标签：" == key:
                job_tags_list = []
                els = dds[i].xpath('a')
                for el in els:
                    job_tags_list.append(el.text_content().strip())

                ret.jobTags = ",".join(job_tags_list)

    # job描述
    els = doc.xpath("//div[@class='tCompany_text']/ul")
    r = ""
    for el in els:
        r += etree.tounicode(el, pretty_print=True)

    r = HtmlFind.remove_tag(r)
    ret.jobDescription = r

    # job职位
    a = doc.xpath("//li[@class='tCompany_job_name']")
    if a:
        ret.jobPosition = a[0].text_content().strip()
    else:
        ret.jobPosition = ''



def _get_inc_url(doc):
    els = doc.xpath("//div[@class='tBorderTop_box job_page_company']/div")
    for el in els:
        clzName = el.attrib.get("class","")
        if "job_page_company_cade" in clzName:
             hrefs = el.xpath("ul/li/a/@href")
             for href in hrefs:
                 if "http" in href:
                     return href.strip()

    return ''


def _get_inc_name(doc):
    els = doc.xpath("//div[@class='tCompany_sidebar']/div[@class='tBorderTop_box job_page_company']/h2")
    for el in els:
        return el.text_content().strip()
    return ''


def _get_inc_intro(doc):
    els = doc.xpath("//div[@class='tCompany_text_gsjs']")
    for el in els:
        r = etree.tounicode(el, pretty_print=True)
        r = HtmlFind.remove_tag(r)
        return r
    return ''


def _set_inc_others(ret,doc):
    tables = doc.xpath("//div[@class='tBorderTop_box job_page_company']/dl[@class='lineDl']")
    for table in tables:

        key = table.xpath("dt")[0].text_content().strip()
        value = table.xpath("dd")[0].text_content().strip()
        if isinstance(key, unicode):
            key = key.encode("utf8")
        if "公司行业：" == key:
            ret.incIndustry = value
        if "公司性质：" == key:
            ret.incType = value
        if "公司规模：" == key:
            ret.incScale = value

import time
if __name__ == '__main__':
    with open('jd_51job3.html', 'rb') as f:
        doc = f.read()

    start = time.time()
    for i in range(0, 1000):
        parse(doc)

    print "consume: {}".format(time.time() - start)


