#!/usr/bin/env python
# -*- coding:utf8 -*-


import sys
sys.path.append(sys.path[0]+"/../../")
print sys.path
import time
import pymongo
from task.base import BaseTask
mongo_url = "mongodb://2c_ipin:2c_ipin@db1,db2,db3/2c_ipin"


class tsk(BaseTask):
    def __init__(self):
        BaseTask.__init__(self, "fix_bug")
        self.process_item = []
        self.client = pymongo.MongoClient(mongo_url)
        self.test_mode = False

    def check_item(self, item):
        if "publishTime" in item and item["publishTime"] > time.time() * 1000:
            self.process_item.append({"jdId":item["jdId"]})
        #if "jobWorkLocId" in item and not isinstance(item["jobWorkLocId"], str):
        #    self.process_item.append({"jdId":item["jdId"], "jobWorkLocId": str(item["jobWorkLocId"])})
            # print {"jdId":item["jdId"], "jobWorkLocId": str(item["jobWorkLocId"])}

    def _load_data(self):
        i = 0
        for item in self.client["2c_ipin"]["jd_measure"].find():
            self.check_item(item)
            i+=1
            if i % 10000 == 0:
                print "load {} items".format(i)

        print "total load {} items".format(len(self.process_item))

    def dispatcher(self):
        for job in self.process_item:
            self._queue.put(job)

    def run_job(self, job):
        try:
            if self.test_mode:
                print job
                return
            #self.client["2c_ipin"]["jd_measure"].update({"jdId":job["jdId"]}, {"$set": {"jobWorkLocId": job["jobWorkLocId"]}})
            self.client["2c_ipin"]["jd_measure"].remove({"jdId":job["jdId"]})
            print "process jdId: {}".format(job["jdId"])
        except Exception as e:
            print e
            print "failed process jdId:{}".format(job["jdId"])


if __name__ == '__main__':
    t = tsk()
    t.run()
