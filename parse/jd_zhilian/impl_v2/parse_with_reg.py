# coding=utf-8

from parse.html_find import HtmlFind
from orm.jd_raw import JdRaw
import re


class JdZhilianHtmlFind(HtmlFind):
    def __init__(self, doc):
        super(JdZhilianHtmlFind, self).__init__(doc)
        self._init_keys_map()
        self._result = JdRaw()

    def _init_keys_map(self):
        self.keys_map = {

            "职位月薪": 'jobSalary',
            "工作地点": 'jobWorkLoc',
            "发布日期": 'pubDate',
            "工作性质": 'jobType',
            "工作经验": 'jobWorkAge',
            "最低学历": 'jobDiploma',
            "招聘人数": 'jobPersonNumber',
            "职位类别": 'jobCate',
            "公司规模": 'incScale',
            "公司性质": 'incType',
            "公司行业": 'incIndustry',
            "公司地址": 'incAddress',

        }

    def set_field(self, key, value):
        if isinstance(key, unicode):
            key = key.encode('utf-8')
        if key not in self.keys_map:
            return

        if key in ['工作地点', '职位类别', '发布日期', '公司行业']:
            # self._result[self.keys_map[key]] = self.remove_tag(value)
            setattr(self._result, self.keys_map[key], self.remove_tag(value))
            return

        setattr(self._result, self.keys_map[key], value)

    def find_fields(self):
        top_elems = self.findTag('div', 'fixed-inner-box')
        if not len(top_elems):
            raise Exception("find position exception..")

        # 工作职位
        position = self.findFromContent(r'<h1>(.*?)</h1>', top_elems[0], re.S)
        for e in position:
            setattr(self._result, "jobPosition", e)

        # 工作福利
        jobWelfares = []
        jobWelfare_elems = self.findFromContent(r'<span>(.*?)</span>', top_elems[0], re.S)
        for jobWelfare_elem in jobWelfare_elems:
            jobWelfares.append(jobWelfare_elem)
        setattr(self._result, "jobWelfare", ",".join(jobWelfares))

        # 工作部分信息
        job_mid_elems = self.findTag('ul', 'terminal-ul clearfix')
        if not len(job_mid_elems):
            raise Exception('find job mid elems exception..')

        spans = re.findall(r'<span>(.*?)：</span>', job_mid_elems[0], re.S)
        strongs = re.findall(r'<strong>(.*?)</strong>', job_mid_elems[0], re.S)

        for i in range(len(spans)):
            self.set_field(spans[i], strongs[i])

        # 工作描述， 公司描述
        button_elems = self.findTag('div', 'tab-inner-cont')
        if len(button_elems) < 2:
            raise Exception("parse job desc and incIntro exception")
        setattr(self._result, 'jobDescription', self.remove_tag(button_elems[0], 1))
        setattr(self._result, 'incIntro', self.remove_tag(button_elems[1], 1))

        # 公司额外信息
        company_elems = self.findTag('div', 'company-box')
        if not company_elems or len(company_elems) < 0:
            raise Exception("company fields parse exception...")
        spans = re.findall(r'<span>(.*?)：</span>', company_elems[0], re.S)
        strongs = re.findall(r'<strong>(.*?)</strong>', company_elems[0], re.S)
        for i in range(len(spans)):
            self.set_field(spans[i], strongs[i])

        company_name_info = re.findall(r'<p[^<>]*?company-name-t[^<>]*>.*?<a[^<>]*?href="(.*?)"[^<>]*?>(.*?)</a>', company_elems[0], re.S)
        if not company_name_info or len(company_name_info) < 1:
            raise Exception("parse company name, url exception...")
        setattr(self._result, 'incUrl',  company_name_info[0][0])
        setattr(self._result, 'incName', self.remove_tag(company_name_info[0][1]))

        return self._result

import time
if __name__ == '__main__':
    with open('jd_zhilian.html', 'rb') as f:
        doc = f.read()

    start = time.time()
    for i in range(0, 1000):
        zhilian_find = JdZhilianHtmlFind(doc)
        zhilian_find.find_fields()

    print time.time() - start


## 正则解析1000次 花费：1.08s
## xpath解析1000次 花费: 3.5s

## 3.5/1.08 = 3.24



