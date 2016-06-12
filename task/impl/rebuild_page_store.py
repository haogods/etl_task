#coding=utf-8

###############################################################
########### 移除page_store中多余的page，保留最新的page ###########
###########          保证url唯一                     ###########

import sys
sys.path.append(sys.path[0]+"/../../")
from task.base import BaseTask
from store.page_store import PageStore
from store.cv_page_store import CVPageStore
from tools import util

class RemovePastPage(BaseTask):
    def __init__(self, channel):
        super(RemovePastPage, self).__init__("remove past page")
        self.channel = channel

        if "jd" in channel:
            self.page_store = PageStore(channel)
        elif "cv" in channel:
            self.page_store = CVPageStore(channel)
        else:
            raise Exception("unknown channel")

        self.unique_items = {}
        self.process_items = []

        self.test_mode = False

    def _load_data(self):
        for item in self.page_store.get_all():
            self.check(item)
            if len(self.process_items) and len(self.process_items) % 10000 == 0:
                print "load {} items".format(len(self.process_items))

        print "finish load {} items".format(len(self.process_items))

    def check(self, item):

        updateTime = item.get('updateTime')
        indexUrl = item.get('indexUrl')
        contentSign = item.get('contentSign')

        if indexUrl not in self.unique_items:
            self.unique_items[indexUrl] = {
                "updateTime" : updateTime,
                "contentSign" : contentSign
            }

        elif indexUrl in self.unique_items:
            if self.unique_items[indexUrl]["updateTime"] > updateTime:
                self.process_items.append({"indexUrl": indexUrl,"contentSign":contentSign})
            else:
                self.process_items.append({"indexUrl": indexUrl,"contentSign":self.unique_items[indexUrl]["contentSign"]})
                self.unique_items[indexUrl] = {
                    "updateTime" : updateTime,
                    "contentSign" : contentSign,
                }

    def dispatcher(self):
        for item in self.process_items:
            self._queue.put(item)

        self._queue.put(None)
        self.wait_q()

    def run_job(self, job):
        if not job or not isinstance(job, dict):
            print "none or not dict"
            return
        key = {"indexUrl": job["indexUrl"], "contentSign": job["contentSign"]}

        try:
            if not self.test_mode:
                self.page_store.remove_by_key(key)
            print "processed indexUrl: %s, contentSign: %s" % (job["indexUrl"], job["contentSign"])
        except Exception as e:
            print "failed process indexUrl: %s, contentSign: %s" % (job["indexUrl"], job["contentSign"])
            print e

    def event_handler(self, evt, msg, **kwargs):
        if "START" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{}_removePastPage 任务".format(self.channel), msg)
            return

        if "DONE" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{}_removePastPage 任务".format(self.channel), msg)
            return


if __name__ == '__main__':

    if len(sys.argv) < 2:
        print "Usage: {} jd_lagou jd_zhilian jd_51job jd_58job jd_wealink jd_liepin jd_linkedin".format(sys.argv[0])
    for channel in sys.argv[1:]:
        task = RemovePastPage(channel)
        task.run()

