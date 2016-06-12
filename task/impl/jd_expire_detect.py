#coding=utf-8

import sys
sys.path.append(sys.path[0]+"/../../")
from task.base import BaseTask
from store.page_store import PageStore
from tools import util
from tools.page_detect import PageExpireDetect
import time
import sys


class JdExpireDetect(BaseTask):
    def __init__(self, channel):
        self.channel = channel
        BaseTask.__init__(self, "{}_expire_detect".format(channel))
        self.page_store = PageStore(channel)
        self.process_items = []
        self.page_expire_detect = PageExpireDetect()

        self.test_mode = False

    def pre_check(self, item):
        now = int(time.time() * 1000)
        # 只检测3天之内，没标记为过期的
        if "updateTime" in item and item["updateTime"] < now - 3 * 24 * 3600:
            if "isExpired" not in item or item['isExpired'] == 0:
                self.process_items.append({"indexUrl": item["indexUrl"], "realUrl": item["realUrl"]})

    def _load_data(self):
        for item in self.page_store.get_all():
            self.pre_check(item)
            if len(self.process_items) and len(self.process_items) % 10000 == 0:
                print "load {} items".format(len(self.process_items))

        print "totally load {} items".format(len(self.process_items))

    def dispatcher(self, q):
        for item in self.process_items:
            q.put(item)

        q.put(None)
        self.wait_q()

    def check_expire_by_channel(self, job, channel):
        if "jd_lagou" == channel:
            return self.page_expire_detect.lagou_page_detect(job["realUrl"])
        if "jd_51job" == channel:
            return self.page_expire_detect.jd51job_page_detect(job["realUrl"])
        if "jd_zhilian" == channel:
            return self.page_expire_detect.zhilian_page_detect(job["realUrl"])
        if "jd_wealink" == channel:
            return self.page_expire_detect.wealink_page_detect(job["realUrl"])
        if 'jd_liepin' == channel:
            return self.page_expire_detect.liepin_page_detect(job['realUrl'])

    def run_job(self, job):
        if not isinstance(job, dict):
            return
        try:
            if not self.check_expire_by_channel(job, self.channel):
                if not self.test_mode:
                    self.page_store.set_expire({"indexUrl": job["indexUrl"]})
                print "set expired , indexUrl: {}".format(job["indexUrl"])
        except Exception as e:
            print e
            print "failed , indexUrl: {}".format(job["indexUrl"])

    def event_handler(self, evt, msg, **kwargs):
        if "START" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{} 过期检测".format(self.channel), msg)
            return

        if "DONE" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{} 过期检测".format(self.channel), msg)
            return

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: {} jd_lagou jd_51job jd_zhilian".format(sys.argv[0])

    for channel in sys.argv[1:]:
        # if "jd_lagou" in channel:
        #     continue
        ts = JdExpireDetect(channel)
        #if "jd_lagou" == channel:
        #    ts.page_expire_detect.load_proxy('../../conf/lagou_proxy')
       # elif "jd_51job" == channel:
       #     ts.page_expire_detect.load_proxy('../../conf/51job_proxy')
       # elif "jd_zhilian" == channel:
       #     ts.page_expire_detect.load_proxy('../../conf/zhilian_proxy')
        ts.run()
