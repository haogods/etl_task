#!/usr/bin/env python
# -*- coding:utf8 -*-

from cv_parser.api_cv_parser import CvParser
from tools import util
from orm.cv_raw import constructCvRawObj, convertToCvRaw
from orm.cv_measure import constructCvMeasureObj
from store.cv_hash_table import CVHashTable
from client.thrift.client import Client as ThriftClient
from task.cv_impl.common.task_common import CHANNEL_PRIORITY
from store.cv_raw_store import CVRawStore
import threading
import os
import time
import random
import requests
import json


class ETLRunner(object):
    def __init__(self, channel):
        self.channel = channel
        # jobList 哈希， 用于判断重复
        self.cv_hash_table = CVHashTable()
        self._duplication_count = 0

        self.parse_runner = ParseRunner(channel)
        self.measure_runner = MeasureRunner()
        self.store_runner = StoreRunner(channel)

        self.check_dupli_runner = CheckCvDupliRunner(channel)

        self.parsed_cnt = 0
        self.parsed_cnt_lock = threading.RLock()

    def run(self, job):

        indexUrl = job.get('indexUrl')
        pagecontent = job.get('pagecontent')
        ps = self.parse_runner.run(pagecontent)
        # 排除重复cv
        if self.check_dupli_runner.run(indexUrl, ps):
            return

        cvRaw_obj = constructCvRawObj(ps)
        self.add_other_fields(cvRaw_obj, self.channel, job)
        cvRaw = convertToCvRaw(cvRaw_obj)
        cvMeasure = self.measure_runner.run(cvRaw)

        # 转为mongoengine模型
        cvMeasured_obj = constructCvMeasureObj(cvMeasure)
        self.store_runner.run(indexUrl, cvRaw_obj, cvMeasured_obj)

    def add_other_fields(self, cvRaw_obj, channel, job):
        cvId = job.get('indexUrl')
        cvRaw_obj.cvId = cvId
        cvRaw_obj.cvFrom = channel
        cvRaw_obj.updateTime = job.get('updateTime')
        cvRaw_obj.contentSign = job.get('contentSign')

        for index, jobItem in enumerate(cvRaw_obj.jobList):
            cvRaw_obj.jobList[index].cvId = cvId


class ETLRunnerFromRaw(ETLRunner):
    def __init__(self, channel):

        ETLRunner.__init__(self, channel)

    def run(self, job):

        job['indexUrl'] = job.get('cvId')  # 只是为了和其它runner一致，使它有indexUrl
        assert ("cvId" in job)
        cvRaw_obj = constructCvRawObj(job)
        cvRaw_obj.cvId = job.get('cvId')
        cvRaw_obj.cvFrom = self.channel
        cvRaw_obj.updateTime = job.get('updateTime')

        cvRaw = convertToCvRaw(cvRaw_obj)
        cvMeasure = self.measure_runner.run(cvRaw)
        cvMeasured_obj = constructCvMeasureObj(cvMeasure)
        self.store_runner.run(cvRaw.cvId, None, cvMeasured_obj)


class CheckCvDupliRunner(object):
    def __init__(self, channel):
        self.channel = channel
        self.r_lock = threading.RLock()
        self._duplication_count = 0
        self.cv_hash_table = CVHashTable()

        self.cv_51job_raw_store = CVRawStore('cv_51job', 'raw')
        self.cv_51job_measure_store = CVRawStore('cv_51job','measure')

        self.cv_zhilian_raw_store = CVRawStore('cv_zhilian', 'raw')
        self.cv_zhilian_measure_store = CVRawStore('cv_zhilian', 'measure')

        self.cv_liepin_raw_store = CVRawStore('cv_liepin', 'raw')
        self.cv_liepin_measure_store = CVRawStore('cv_liepin', 'measure')

        self.dupli_file_save = open(os.path.join(os.path.dirname(__file__),
                                                 "../result/%s_duplicate_ids_%d" % (self.channel, os.getpid())), 'wb')

    def _inc_duplication_count(self, indexUrl):
        with self.r_lock:
            self._duplication_count += 1

    def remove_duplication(self, cvId_to_remove, channel, indexUrl):
        key = {'cvId': cvId_to_remove}

        if channel == 'cv_51job':
            self.cv_51job_raw_store.delete_one(key)
            self.cv_51job_measure_store.delete_one(key)
        elif channel == 'cv_zhilian':
            self.cv_zhilian_raw_store.delete_one(key)
            self.cv_zhilian_measure_store.delete_one(key)

        elif channel == 'cv_liepin':
            self.cv_liepin_raw_store.delete_one(key)
            self.cv_liepin_measure_store.delete_one(key)

        else:
            raise Exception('unknown channel')

        print >> self.dupli_file_save, "[D]cvId: %s is removed for duplicating with cvId: %s" % (cvId_to_remove, indexUrl)

    def run(self, indexUrl, ps):
        for s in ps.get('jobList', []):
            incName = s.get('incName', '')
            jobPosition = s.get('jobPosition', '')
            jobDesc = s.get('jobDesc', '')

            # 都不为空， 才会判重
            if not (incName and jobPosition and jobDesc):
                return False

            hash_value = util.md5([incName, jobPosition, jobDesc], remove_space=True)
            key = {'hash_value': hash_value}
            hash_doc = self.cv_hash_table.find_one(key)
            if hash_doc:
                # 统计重复数
                self._inc_duplication_count(indexUrl)

                # 如果此渠道优先级比较大， 替换掉存在hash表中的
                cvId_in_db = hash_doc.get('cvId')
                # 相同Id, 可能更新
                if cvId_in_db == indexUrl:
                    return False

                cv_channel_in_db = cvId_in_db.split('://')[0]
                if CHANNEL_PRIORITY.get(self.channel, 0) > CHANNEL_PRIORITY.get(cv_channel_in_db, 0):
                    hash_doc['cvId'] = indexUrl
                    self.cv_hash_table.save_one(key, hash_doc, True)
                    # remove 优先级低的， 保持解析数据没有重复
                    self.remove_duplication(cvId_in_db, cv_channel_in_db, indexUrl)
                    return False
                else:
                    self.remove_duplication(indexUrl, indexUrl.split('://')[0], cvId_in_db)

                    return True
            else:
                hash_doc = {'hash_value': hash_value, 'cvId': indexUrl}
                self.cv_hash_table.save_one(key, hash_doc, True)
                return False

        return False


class ParseRunner(object):
    def __init__(self, channel, http_service_addr=None):
        self.channel = channel
        self.http_service_addr = http_service_addr
        self.cv_parser = CvParser()

    def run(self, pagecontent):
        if not self.http_service_addr:
            return self.cv_parser.parser(pagecontent, cvFrom=self.channel)
        else:
            result = requests.post(self.http_service_addr, data={'channel': self.channel, 'pagecontent': pagecontent})
            return json.loads(result.text,encoding='utf-8')


class MeasureRunner(object):
    def __init__(self):
        self.thrift_client = ThriftClient(os.path.join(os.path.dirname(__file__), '../../../conf/thrift_conf.ini'))
        self.measure_client = self.thrift_client.cv_measure_server_client

    def run(self, raw):
        return self.measure_client.measureCv(raw)


class StoreRunner(object):
    def __init__(self, channel, is_bulk=False):
        self.channel = channel
        self.cv_raw_store = CVRawStore(self.channel, stage='raw')
        self.cv_measure_store = CVRawStore(self.channel, stage='measure')
        self.is_bulk = is_bulk

        if self.is_bulk:
            t = threading.Thread(target=self.do_bulk_upsert)
            t.start()

    def do_bulk_upsert(self):
        while True:
            time.sleep(random.randint(300, 3000))
            print "do upserting..."
            self.cv_raw_store.bulk_upsert(None, None)
            self.cv_measure_store.bulk_upsert(None, None)
            print "do upserting ok"

    def run(self, cv_id, cv_raw, cv_measure):
        key = {"cvId": cv_id}

        if not self.is_bulk:
            if cv_raw:
                self.cv_raw_store.save_one(key, cv_raw.to_json(), isUpsert=True)
            if cv_measure:
                self.cv_measure_store.save_one(key, cv_measure.to_json(), isUpsert=True)
        else:
            if cv_raw:
                self.cv_raw_store.bulk_upsert(key, cv_raw.to_mongo())
            if cv_measure:
                self.cv_measure_store.bulk_upsert(key, cv_measure.to_mongo())



