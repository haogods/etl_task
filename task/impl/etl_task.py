# coding=utf-8

import sys
sys.path.append(sys.path[0]+"/../../")

from task.base import BaseTask
from store.jd_lagou import LgEtlStore
from store.jd_51job import Job51EtlStore
from store.jd_zhilian import ZLEtlStore
from store.page_store import PageStore
from tools.savebin import BinReader
from parse.jd_51job.impl.parse_with_reg import Jd51JobHtmlFind
from parse.jd_lagou.impl_v2.parse_with_reg import JdLagouHtmlFind
from parse.jd_zhilian.impl_v2.parse_with_reg import JdZhilianHtmlFind
from tools.format import construct_jd_raw
from client.thrift.client import Client as TClient
from orm.jd_measure import convertToJdMeasure
from tools.handler import get_jd_measure_hash, gen_sim_hash
from tools import util
from tools.filesave import FileSave
import time
import json


class ETLTask(BaseTask):
    def __init__(self, owner, queue_size, thread_cnt):
        BaseTask.__init__(self, owner, queue_size, thread_cnt)
        self.owner = owner
        self.process_item = {}
        self.expired_items = []
        self._raw_store = self._get_raw_store(owner)
        self._measure_store = self._get_measure_store(owner)
        self._page_store = PageStore(owner)
        self.page_store_db = 'admin'
        self.page_store_coll = "page_store_{}".format(owner)

        self.thrift_client = TClient('../../conf/thrift_conf.ini')
        self.jd_measure_client = self.thrift_client.jd_measure_server_client

        self.test_mode = False

        self.rs_file = FileSave(BaseTask.PathConfig.result_file)
        self.failfile = FileSave(BaseTask.PathConfig.etl_failids_file)

    def _get_raw_store(self, owner):
        if "jd_lagou" == owner:
            return LgEtlStore('raw')
        if "jd_51job" == owner:
            return Job51EtlStore('raw')
        if "jd_zhilian" == owner:
            return ZLEtlStore('raw')

        raise Exception(" unknown owner ")

    def _get_measure_store(self, owner):
        if "jd_lagou" == owner:
            return LgEtlStore('measure')
        if "jd_51job" == owner:
            return Job51EtlStore('measure')
        if "jd_zhilian" == owner:
            return ZLEtlStore('measure')

        raise Exception(" unknown owner ")

    def fill_data_with_flag(self, indexUrl, realUrl, contentSign, updateTime, filePath, flag):

        if indexUrl in self.process_item:
            if self.process_item[indexUrl]['updateTime'] < updateTime:
                self.process_item[indexUrl]['updateTime'] = updateTime
                self.process_item[indexUrl]['contentSign'] = contentSign
                self.process_item[indexUrl]['realUrl'] = realUrl
                self.process_item[indexUrl]['flag'] = flag
                self.process_item[indexUrl]['filePath'] = filePath

        else:
            self.process_item[indexUrl] = {
                'updateTime': updateTime,
                'contentSign': contentSign,
                'realUrl': realUrl,
                'filePath': filePath,
                'flag': flag,
            }

    def check(self, item):

        status = item.get('status', 0)
        isUpdated = item.get('isUpdated', 0)
        updateTime = item.get('updateTime')
        indexUrl = item.get('indexUrl')
        contentSign = item.get('contentSign')
        file_path = item.get('pageContentPath')
        jdUrl = item.get('realUrl')
        expired = item.get('isExpired', 0)

        if status == 0:
            self.fill_data_with_flag(indexUrl, jdUrl, contentSign, updateTime, file_path, 0)
        if status == 1 and isUpdated == 1:
            self.fill_data_with_flag(indexUrl, jdUrl, contentSign, updateTime, file_path, 1)
        if expired == 1:
            self.fill_data_with_flag(indexUrl, jdUrl, contentSign, updateTime,  file_path, 2)

    def _load_data(self):
        page_client = self._raw_store.cmgClient.page_store_mongo_client
        for item in page_client[self.page_store_db][self.page_store_coll].find():
            self.check(item)
            if len(self.process_item) and len(self.process_item) % 10000 == 0:
                print "load {} items".format(len(self.process_item))

        print "finish load {} items".format(len(self.process_item))
        print "finish load {} expired items".format(len(self.expired_items))
        # print "======================start dump ids to files================="
        # fd = FileSave('../data/results.txt')
        # for indexUrl in self.process_item:
        #     fd.append_end_with(indexUrl)
        #
        # print "=======================dump finish============================="

    def dispatcher(self, q):

        for item in self.process_item:
            q.put(item)

        for item in self.expired_items:
            q.put(item)

        q.put(None)
        self.wait_q()

    def getPageContent(self, filename):
        parts = filename.split("::")
        if len(parts) == 3:
            binReader = BinReader(parts[1])
            _, content = binReader.readone_at(int(parts[2]))
            if len(content) == 0:
                raise Exception("file name:{} , content error".format(filename))
            return content

        if len(parts) == 1:
            with open(filename) as f:
                content = f.read()
                if len(content) == 0:
                    raise Exception("file name:{} , content error".format(filename))
                return content

    def update_jd(self, item):
        jd_store_key = {'jdId': item}
        page_store_key = {'indexUrl': item, 'contentSign': self.process_item[item].get('contentSign')}
        updateTime = self.process_item[item].get("updateTime")
        strTime = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(updateTime/1000))

        jkey = json.dumps(jd_store_key)
        jdoc = json.dumps({"$set": {"pubDate": strTime}})
        jdoc_m = json.dumps({"$set": {"pubDateStamp": updateTime}})

        if self.test_mode:
            print " Update ===> jdkey: {} \n jdoc: {} \n".format(jkey, jdoc)
            return

        if not self._raw_store.save_one(jkey, jdoc, False):
            raise Exception("update jd_raw pubTime Exception")
        if not self._measure_store.save_one(jkey, jdoc_m, False):
            raise Exception("update jd_measure pubTime Exception")
        if not self._page_store.save_one(json.dumps(page_store_key), json.dumps({"$set": {"isUpdated": 0}}), False):
            raise Exception("set page store isUpdated status Exception")

    def check_is_student_job(self, jd_raw):
        if jd_raw.jobWorkAge in [u"在读学生", u"应届毕业生", u"无经验", u"无要求"]\
                or jd_raw.jobCate in [u"应届毕业生", u"储备干部", u"培训生", u"兼职", u"临时", u"实习生"]\
                or jd_raw.jobType in [u"兼职", u"实习"]:
            print "student job ===> ", jd_raw.jdId

            expired = 0
            if self.process_item[jd_raw.jdId]['flag'] == 2:
                expired = 1

            self.rs_file.append_end_with('\t'.join((jd_raw.jdId, str(expired))))
            return True
        return False

    def parse_measure_jd(self, item):
        try:
            jd_store_key = json.dumps({'jdId': item})
            page_store_key = json.dumps({'indexUrl': item, 'contentSign': self.process_item[item].get('contentSign')})
            fileName = self.process_item[item].get("filePath")
            pageContent = self.getPageContent(fileName)

            jd_raw = self.parse_by_owner(item, pageContent, self.owner)
            jd_measure = self.measure(jd_raw)

            if self.test_mode:
                print "Raw ===> {}".format(jd_raw.to_json())
                print "Measure ===> {}".format(jd_measure.to_json())
                return

            if not self._raw_store.save_one(jd_store_key, jd_raw.to_json(), True):
                raise Exception("set raw exception")

            if not self._measure_store.save_one(jd_store_key, jd_measure.to_json(), True):
                raise Exception("set measure exception")

            if not self._page_store.save_one(page_store_key, json.dumps({"$set": {"status": 1}}), False):
                raise Exception("set page store status exception")
            self.check_is_student_job(jd_raw)
        except Exception as e:
            self.failfile.append_end_with(item)
            raise e

    def parse_by_owner(self, jdId, pageContent, owner):
        if "jd_lagou" == owner:
            jdRaw = JdLagouHtmlFind(pageContent).find_fields()
        elif "jd_51job" == owner:
            jdRaw = Jd51JobHtmlFind(pageContent).find_fields()
        elif "jd_zhilian" == owner:
            jdRaw = JdZhilianHtmlFind(pageContent).find_fields()
        else:
            raise Exception("unknown owner")

        jdRaw.jdId = jdId
        jdRaw.jdFrom = self.owner
        jdRaw.jdUrl = self.process_item[jdId].get('realUrl')

        if not jdRaw.pubDate:
            stamp = self.process_item[jdId].get("updateTime")
            str_uptime = time.strftime("%Y-%m-%d %H:%m:%S", time.localtime(stamp/1000))
            jdRaw.pubDate = str_uptime

        return jdRaw

    def measure(self, jd_raw):
        raw_for_measure = construct_jd_raw(jd_raw)
        jd_measure = self.jd_measure_client.measureJd(raw_for_measure)
        jdMeasureObj = convertToJdMeasure(self.owner, jd_measure)

        self.set_md5_SimHash(jdMeasureObj, jd_raw)

        return jdMeasureObj

    def set_md5_SimHash(self, jd_measure, jd_raw):
        jd_measure.jdMd5 = self.get_jd_md5(jd_measure, jd_raw)
        jd_measure.jdSimHash = gen_sim_hash(jd_raw.jobDescription)

    def get_jd_md5(self, jd_measure, jd_raw):

        temp = dict({})
        temp["jdPosition"] = jd_raw.jobPosition
        temp["incName"] = jd_measure.incSegmentId
        temp["jdWorkLoc"] = jd_measure.jobWorkLocId

        return get_jd_measure_hash(temp)

    def event_handler(self, evt, msg, **kwargs):
        if "START" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{}_etl 任务".format(self.owner), msg)
            return

        if "DONE" == evt:
            util.send_email(["<jianghao@ipin.com>"], "{}_etl 任务".format(self.owner), msg)
            return

