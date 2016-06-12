# coding=utf-8

import sys
sys.path.append(sys.path[0]+"/../../")
from task.base import BaseTask
from tools import util
from store.jd_lagou import LgEtlStore
from store.jd_51job import Job51EtlStore
from store.jd_zhilian import ZLEtlStore
from store.toc_page import ToCMeasureStore
from client.thrift.client import Client as TClient
from ipin.rpc.common.datatype.ttypes import ServiceAccessToken
from orm.toC_measure_page import toCMeasurePageModel
from tools.filesave import FileSave
from bson import ObjectId
import pymongo
import traceback
import json
traceback.print_exc()

class TCImportTask(BaseTask):
    def __init__(self, thread_cnt):
        BaseTask.__init__(self, '2c_import', thread_cnt=thread_cnt)
        self.process_items = []
        self.fail_fn = BaseTask.PathConfig.toc_failids_file
        self.rs_fn = BaseTask.PathConfig.result_file
        self.lgstore = LgEtlStore()
        self.job51store = Job51EtlStore()
        self.zlstore = ZLEtlStore()
        self.tocstore = ToCMeasureStore()

        self.thrift_client = TClient('../../conf/thrift_conf.ini')
        self.inc_stats2_client = pymongo.MongoClient(self.lgstore.cmgClient.inc_stats2_mongo_url)
        self.zhineng_salary5_charts_client = pymongo.MongoClient(self.lgstore.cmgClient.zhineng_salary5_charts_mongo_url)

        self.edu_info_client = self.thrift_client.edu_info_client
        self.inc_info_clients = self.thrift_client.inc_idinfo_client

        self.toc_failids_fd = FileSave(BaseTask.PathConfig.toc_failids_file)

    def _load_data(self):
        with open(self.rs_fn) as f:
            for line in f:
                if not line:
                    continue
                index, expired = line.split('\t')
                self.process_items.append({"index":index, "expired": int(expired)})

    def dispatcher(self):
        for item in self.process_items:
            self._queue.put(item)

        self._queue.put(None)
        self.wait_q()

    def get_jd_raw(self, index):
        channel = index.split("://")[0]
        if channel == 'jd_lagou':
            return self.lgstore.get_raw(index)
        elif channel == 'jd_51job':
            return self.job51store.get_raw(index)
        elif channel == 'jd_zhilian':
            return self.zlstore.get_raw(index)

    def get_jd_measure(self, index):
        channel = index.split("://")[0]
        if channel == 'jd_lagou':
            return self.lgstore.get_measure(index)
        elif channel == 'jd_51job':
            return self.job51store.get_measure(index)
        elif channel == 'jd_zhilian':
            return self.zlstore.get_measure(index)

    def constructMobileJdUrl(self, jdId, jdFrom):
        positionId = jdId.split("://")[1]
        if "jd_lagou" == jdFrom:
            return "http://www.lagou.com/center/jobs_{}.html?m=1".format(positionId)
        if "jd_51job" == jdFrom:
            return "http://m.51job.com/search/jobdetail.php?jobid={}".format(positionId)
        if "jd_zhilian" == jdFrom:
            part1 = positionId[:9]
            part2 = positionId[9:]
            realPositionId = "cc{}j90{}000".format(part1, part2)
            return "http://m.zhaopin.com/jobs/{}".format(realPositionId)

    def get_major_ids(self, major):
        # major = u"计算机科学与技术"
        flag = 1
        try:
            bk_major = self.thrift_client.edu_info_client.findBkMajorId("", major)
        except Exception as e:
            bk_major = ""
        try:
            zk_major = self.thrift_client.edu_info_client.findZkMajorId("", major)
        except Exception as e:
            zk_major = ""

        ids = [bk_major, zk_major]

        if len(bk_major) <= 0 and len(zk_major) <= 0:
            flag = 2
        else:
            print "======================= ids: ", ids

        return ids, flag

    def get_salary_ratio(self, jobCate):
        r = -1
        if len(jobCate) <= 0:
            return r
        q = self.zhineng_salary5_charts_client["zhineng_stats_v2"]["zhineng_salary5_charts"].\
            find_one({"zhineng_id": jobCate})
        if q and "greater" in q:
            r = q.get("greater")
        return r

    def get_total_ind_rank(self, incSegmentId):
        tagList=[]
        total_ind_rank_postion = -1
        incId = self.get_inc_id(incSegmentId)

        if not incId:
            return "", tagList, total_ind_rank_postion

        q = self.inc_stats2_client["inc_stats_v6"]["inc"].find_one({"_id":ObjectId(incId)})
        if q:
            if "tag_list" in q:
                tagList = q.get("tag_list")

            if "rank_info" in q and "total_ind_rank_postion" in q["rank_info"]:
                total_ind_rank_postion = q["rank_info"]["total_ind_rank_postion"]

        return incId, tagList, total_ind_rank_postion

    def get_inc_id(self, incSegmentId):
        try:
            idinfo = self.inc_info_clients.queryIncId(ServiceAccessToken(), "", incSegmentId)
        except Exception as e:
            print e
            raise e
        return idinfo

    def rebuild(self, raw, measure):
        tocmeasure = toCMeasurePageModel()

        tocmeasure.jdId = raw["jdId"]
        tocmeasure.jdUrl = raw["jdUrl"]
        tocmeasure.mobileJdUrl = self.constructMobileJdUrl(raw["jdId"], raw["jdFrom"])
        tocmeasure.channel = raw["jdFrom"]
        tocmeasure.jd_content_hash = measure["jdSimHash"]
        tocmeasure.jd_measure_hash = measure["jdMd5"]
        tocmeasure.publishTime = measure["pubDateStamp"]
        tocmeasure.isExpired = 0

        tocmeasure.jobDiploma = raw["jobDiploma"]
        tocmeasure.jobDiplomaId = measure["jobDiplomaId"]
        tocmeasure.jobPosition = raw["jobPosition"]
        tocmeasure.jobWorkLoc = raw["jobWorkLoc"]
        tocmeasure.jobSalaryMin = measure["jobSalaryMin"]
        tocmeasure.jobSalaryMax = measure["jobSalaryMax"]
        tocmeasure.jobWorkLocId = str(measure["jobWorkLocId"])
        tocmeasure.jobWorkLoc = raw["jobWorkLoc"]
        tocmeasure.jobWorkAgeMin = measure["jobWorkAgeMin"]
        tocmeasure.jobWorkAgeMax = measure["jobWorkAgeMax"]
        tocmeasure.jobCate = measure["jobCate"]
        tocmeasure.jobType = raw["jobType"]
        tocmeasure.major = measure["jobMajor"]
        tocmeasure.jobDescription = raw["jobDescription"]

        # majorIds, majorIdsFlag
        if measure["jobMajor"] == u"专业不限":
            tocmeasure.majorIds = []
            tocmeasure.majorIdsFlag = 0 #专业不限
        elif measure["jobMajor"] == "":
            tocmeasure.majorIds = []
            tocmeasure.majorIdsFlag = -1 #专业没有
        else:
            tocmeasure.majorIds, tocmeasure.majorIdsFlag = self.get_major_ids(measure["jobMajor"])

        tocmeasure.incName = raw["incName"]
        tocmeasure.incIntro = raw["incIntro"]
        tocmeasure.incIndustry = raw["incIndustry"]
        tocmeasure.incIndustryId = measure["incIndustryId"]
        tocmeasure.incType = measure["incType"]
        tocmeasure.incScaleMin = measure["incScaleMin"]
        tocmeasure.incScaleMax = measure["incScaleMax"]

        tocmeasure.salaryRatio = self.get_salary_ratio(measure["jobCate"])
        tocmeasure.incId, tocmeasure.tagList, tocmeasure.total_ind_rank_postion = self.get_total_ind_rank(measure["incSegmentId"])

        return tocmeasure

    def run_job(self, item):

        index = item.get('index')
        expired = item.get('expired')
        try:

            if expired:
                self.tocstore.set_expired({"jdId": index})
                print "complete set expired,  indexUrl: {}".format(index)
                return

            raw = self.get_jd_raw(index)

            measure = self.get_jd_measure(index)

            result = self.rebuild(raw, measure)
            key = json.dumps({"jdId": raw["jdId"]})

            doc = util.remove_empty_key(json.loads(result.to_json()), ['isExpired'])
            self.tocstore.save_one(key, json.dumps(doc), True)

            print "complete copy indexUrl: {}".format(index)
        except Exception as e:
            self.toc_failids_fd.append_end_with(index)
            print "failed copy indexUrl: {}".format(index)
            traceback.print_exc()
            raise e

    def event_handler(self, evt, msg, **kwargs):
        if "START" == evt:
            util.send_email(["<jianghao@ipin.com>"], "2cimport 任务", msg)
            return

        if "DONE" == evt:
            util.send_email(["<jianghao@ipin.com>"], "2cimport 任务", msg)
            return


