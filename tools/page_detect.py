#coding=utf-8

from httpreq import BasicRequests
from lxml import html


class PageExpireDetect(object):

    def __init__(self):
        self.req = BasicRequests()
        self.req.load_proxy('../../conf/zhilian_proxy')

    def load_proxy(self, fn, index=-1, auto_change=True):
        self.req.load_proxy(fn, index, auto_change)

    def lagou_page_detect(self, url):

        resp = self.req.request_url(url)
        if isinstance(resp.text, unicode):
            resp.text = resp.text.encode("utf-8")
        htl = html.fromstring(resp.text)

        if htl.xpath("/div[@class='position_del']"):
            return False

        if "亲，你来晚了，该信息已经被删除鸟" in resp.text:
            return False

        return True

    def jd51job_page_detect(self, url):

        # 过期返回 False, 否则返回True
        resp = self.req.request_url(url)
        resp.encoding = 'gb2312'
        if u"很抱歉，你选择的职位目前已经暂停招聘" in resp.text:
            return False
        return True

    def zhilian_page_detect(self, url):

        resp = self.req.request_url(url)
        if "http://img01.zhaopin.cn/2014/seo/images/outmoded_01.png" in resp.text:
            return False

        htl = html.fromstring(resp.text)

        if htl.xpath("//div[@class='returnpage']"):
            return False

        srcs = htl.xpath("//div[@class='inner-right fr']/img/@src")
        for src in srcs:
            if "http://img01.zpin.net.cn/2014/rd2/img/company_gq.png" in src:
                return False

        return True

    def liepin_page_detect(self, url):
        con = self.req.request_url(url)
        if con is None or u'抱歉， 您访问的页面不存在或已删除' in con.text:
            return False
        if u'抱歉，您查看的职位已过期' in con.text:
            return False
        if u'该职位已结束' in con.text:
            return False

        return True

    def wealink_page_detect(self, url):
        resp = self.req.request_url(url)
        htl = html.fromstring(resp.text)


if __name__ == "__main__":

    c = PageExpireDetect()
    print c.lagou_page_detect("http://www.lagou.com/jobs/1854151.html")