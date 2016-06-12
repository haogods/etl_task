# coding=utf-8

from parse.html_find import HtmlFind
from orm.jd_raw import JdRaw
from parse.format import content_format
import re



class JdLagouHtmlFind(HtmlFind):
    def __init__(self, doc):
        super(JdLagouHtmlFind, self).__init__(doc)
        self._result = JdRaw()

    def find_fields(self):
        job_request_part = self.findTag('dd', 'job_request')
        for e in job_request_part:
            spans = self.findFromContent(ur'.*<span[^>]*>(.*)</span>',e)
            ps = self.findFromContent(ur'<p[^>]*>([^<>]*?)</p>', e)

            if len(ps) == 1:
                ps_temp = self.findFromContent(r'职位诱惑 : (.*?)</p>', e, re.S)
                if ps_temp:
                    ps.insert(0, ps_temp[0])

            if not spans or len(spans) !=5 or not ps or len(ps) < 2:
                raise Exception("find_fields exception.. nimade")
            self._result.jobSalary = spans[0]
            self._result.jobWorkLoc = spans[1]
            self._result.jobWorkAge = spans[2]
            self._result.jobDiploma = spans[3]
            self._result.jobType = spans[4]
            self._result.jobWelfare = re.sub(r'职位诱惑 : ', '', ps[0])
            break

        # 工作描述
        job_desc = self.findTag('dd', 'job_bt')
        for e in job_desc:
            e = self.remove_tag(e, 1)
            self._result.jobDescription = e
            break

        # 工作职位 和 部门
        dt = self.findTag('dt', 'clearfix join_tc_icon')
        for m in dt:
            title = self.findFromContent(r'.*<h[^<>]* title="(.*?)"[^<>]*>.*?<div>([^<>]*?)</div>', m, re.S)
            if not title or len(title[0]) < 2:
                raise Exception('find_fields exception.... 2.. ')
            self._result.jobPosition = title[0][0]
            self._result.jobPositionExt = title[0][1]

        # 公司信息
        company = self.findTag('dl', 'job_company')
        for c in company:

            els = self.findFromContent(r'<a[^<>]*href="(.*?)"[^<>]*>'
                                       r'.*?<img[^<>]*alt="(.*?)"[^<>]*>'
                                       r'.*领域</span>([^<>]*)'
                                       r'.*规模</span>([^<>]*)', c, re.S)
            if els and len(els[0]) >= 4:
                self._result.incUrl = els[0][0]
                self._result.incName = els[0][1]
                self._result.incIndustry = content_format(els[0][2])
                self._result.incScale = content_format(els[0][3])

            span_stage = self.findFromContent(r'.*目前阶段</span>([^<>]*)', c, re.S)
            if span_stage and len(span_stage[0])>0:
                self._result.incStage = span_stage[0]

            span_incaddr = self.findFromContent(r'.*工作地址</h[^<>]*>.*?<div>([^<>]*)</div>', c, re.S)
            if span_incaddr and len(span_incaddr[0]) > 0:
                self._result.jobWorkLocExt = span_incaddr[0]

        return self._result

import time
if __name__ == '__main__':
    with open('jd_lagou.html', 'rb') as f:
        doc = f.read()

    start = time.time()
    for i in range(0,1000):
        lagoufind = JdLagouHtmlFind(doc)
        m = lagoufind.find_fields()
    print time.time() - start