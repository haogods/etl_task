#coding=utf-8

from parse.base_parse import BaseParse
from orm.jd_raw import JdRaw
from parse.html_find import HtmlFind
from lxml import html
import time

'''
            "工作职位": 'jobPosition',
            "发布日期": 'pubDate',
            "工作地点": 'jobWorkLoc',
            "招聘人数": 'jobPersonNumber',
            "工作年限": 'jobWorkAge',
            "学历要求": 'jobDiploma',
            "薪资范围": 'jobSalary',
            "上班地址": 'jobWorkLocExt',
            "职能类别": 'jobCate',
            "职位标签": 'jobTags',
            "薪酬福利": 'jobWelfare',
            "工作描述": 'jobDescription',
            "公司名":   'incName',
            "公司行业": 'incIndustry',
            "公司性质": 'incType',
            "公司规模": 'incScale',
            "公司地址": 'incAddress',
            "公司介绍": 'incIntro',
'''
class Jd51jobParse(BaseParse):

    def __init__(self, doc):
        BaseParse.__init__(self, doc)
        self.result = JdRaw()

    def myprint(self):
        # print self.
        print self.result.to_json()

    def parse(self):

        top_info = self._doc.xpath("//div[@class='cn']")
        if not top_info:
            raise Exception("find top_info exception")
        position = top_info[0].xpath("h1/@title")[0]
        self.result.jobPosition = self.replace_pattern('\(.*?\)', '', position)
        self.result.jobWorkLoc = top_info[0].xpath("span[@class='lname']")[0].text_content()
        self.result.incName = top_info[0].xpath("p[@class='cname']/a/@title")[0]
        self.result.incUrl = top_info[0].xpath("p[@class='cname']/a/@href")[0]
        self.result.jobSalary = top_info[0].xpath("strong")[0].text_content()

        inc_info_part1 = top_info[0].xpath("p[@class='msg ltype']")
        if inc_info_part1:
            incType, incScale, incIndustry = inc_info_part1[0].text_content().split('|')
            self.result.incType = HtmlFind.remove_tag(incType, 1)
            self.result.incScale = HtmlFind.remove_tag(incScale, 1)
            self.result.incIndustry = HtmlFind.remove_tag(incIndustry, 1)

        ###########################################################################
        ###########################################################################
        mid_info = self._doc.xpath("//div[@class='jtag inbox']")
        if not mid_info:
            raise Exception("find mid_info exception")
        mid_elems = mid_info[0].xpath("div[@class='t1']/span[@class='sp4']")

        mid_field_list = ['jobWorkAge', 'jobPersonNumber']
        i = 0
        for mid_elem in mid_elems:
            if i >=2 :
                break

            setattr(self.result, mid_field_list[i], mid_elem.text_content())
            i += 1

        jobWelfare = ''
        mid_elems_2 = mid_info[0].xpath("p[@class='t2']/span")
        for el in mid_elems_2:
            jobWelfare += el.text_content() + ' '

        self.result.jobWelfare = jobWelfare

        jobDesc = self._doc.xpath("//div[@class='bmsg job_msg inbox']")
        if not jobDesc:
            raise Exception("get jobdesc exception")

        jobDescription = HtmlFind.remove_tag(html.tostring(jobDesc[0], encoding='utf-8'), 1)
        index = jobDescription.find('关键字：')
        if index != -1:
            jobDescription = jobDescription[:index]

        self.result.jobDescription = jobDescription

        index = jobDescription.find('职能类别：')
        if index != -1:
            jobCate = jobDescription[index:]

            self.result.jobCate = self.replace_pattern('职能类别：', '', jobCate)

        inc_intro = self._doc.xpath('//div[@class="tmsg inbox"]')
        if inc_intro:
            raw_content = html.tostring(inc_intro[0], encoding='utf-8')
            raw_content = HtmlFind.remove_tag(raw_content, 1)
            self.result.incIntro = raw_content


        self.myprint()



if __name__ == '__main__':
    with open("job.html", 'rb') as f:
        doc = f.read()

    start = time.time()
    for i in range(1000):
        find = Jd51jobParse(doc)
        find.parse()
    print "consume: {}".format(time.time() - start)




