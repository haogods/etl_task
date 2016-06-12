#!/usr/bin/env python
# -*- coding:utf-8 -*-

import threading

from store.cv_page_store import CVPageStore
from cv_parser.api_cv_parser import CvParser
from tools.savebin import BinReader
from tools import util
from orm.cv_raw import constructCvRawObj, convertToCvRaw
from orm.cv_measure import constructCvMeasureObj
from store.cv_hash_table import CVHashTable
from client.thrift.client import Client as ThriftClient
from task.cv_impl.common.task_common import CHANNEL_PRIORITY
from store.cv_raw_store import CVRawStore
from tools.filesave import FileSave
import os


class ETLBase(object):
    def __init__(self, channel):
        self.channel = channel
        self.process_item = {}

        self.cv_page_store = CVPageStore(channel)
        self.cv_parser = CvParser()

        self.test_mode = False

        conf_path = os.path.join(os.path.dirname(__file__), '../../conf/thrift_conf.ini')
        self.thrift_client = ThriftClient(conf_path)

        # jobList 哈希， 用于判断重复
        self.cv_hash_table = CVHashTable()
        self._duplication_count = 0

        self.rlock = threading.RLock()

        self.cv_raw_store = CVRawStore(self.channel, stage='raw')
        self.cv_measure_store = CVRawStore(self.channel, stage='measure')
        self.measure_client = self.thrift_client.cv_measure_server_client

        self.cv_51job_raw_store = CVRawStore('cv_51job', 'raw')
        self.cv_51job_measure_store = CVRawStore('cv_51job','measure')

        self.cv_zhilian_raw_store = CVRawStore('cv_zhilian', 'raw')
        self.cv_zhilian_measure_store = CVRawStore('cv_zhilian', 'measure')

        self.cv_liepin_raw_store = CVRawStore('cv_liepin', 'raw')
        self.cv_liepin_measure_store = CVRawStore('cv_liepin', 'measure')

        self.fail_save = FileSave('%s_fail_ids.txt' % self.channel)
        self.parsed_cnt = 0
        self.parsed_cnt_lock = threading.RLock()

        # 二进制文件位置， 设置
        # local 表示在本地
        # remote 表示在远程
        self.bin_file_location = os.environ['BIN_FILE_LOCATION']

        if self.bin_file_location == 'remote':
            self.bin_read_client = self.thrift_client.bin_read_server_client

    def _inc_parsed_cnt(self):
        with self.parsed_cnt_lock:
            self.parsed_cnt += 1

    def _inc_duplication_count(self):
        with self.rlock:
            self._duplication_count += 1

    def fill_data_with_flag(self, indexUrl, realUrl, contentSign, updateTime, filePath, flag):

        self.process_item[indexUrl] = {
            'updateTime': updateTime,
            'contentSign': contentSign,
            'realUrl': realUrl,
            'filePath': filePath,
            'flag': flag,
        }

        if len(self.process_item)%10000 == 0:
            print "load items: %d" % len(self.process_item)

    def store(self, cvId, raw, measure, bulk=False):
        if self.test_mode:
            print "====================test mode cvId: %s================" % cvId
            print "raw: ", raw.to_json()
            return
        key = {"cvId": cvId}
        if not bulk:
            self.cv_raw_store.save_one(key, raw.to_json(), isUpsert=True)
            self.cv_measure_store.save_one(key, measure.to_json(), isUpsert=True)
        else:
            self.cv_raw_store.bulk_upsert(key, raw.to_mongo())
            self.cv_raw_store.bulk_upsert(key, measure.to_mongo())

    def measure(self, raw):
        return self.measure_client.measureCv(raw)

    def check_and_put(self, item):

        updateTime = item.get('updateTime')
        indexUrl = item.get('indexUrl')
        contentSign = item.get('contentSign')
        file_path = item.get('pageContentPath')
        realUrl = item.get('realUrl')

        self.fill_data_with_flag(indexUrl, realUrl, contentSign, updateTime, file_path, 0)

    def _load_data(self):
        for item in self.cv_page_store.get_all():
            self.check_and_put(item)

        print "============= totally load %d items ===============" % len(self.process_item)

    def dispatcher(self, q, from_which='db'):
        if from_which == 'db':
            self.dispatcher_from_db(q)
        elif from_which == 'file':
            self.dispatcher_from_file(q)
        else:
            raise Exception("unknown from_which")

    def dispatcher_from_db(self, q):
        i = 0
        total_cnt = len(self.process_item)
        for item in self.process_item:
            pagecontent = self.getPageContent(self.process_item[item].get("filePath"), self.bin_file_location)
            q.put({'indexUrl': item, 'pagecontent': pagecontent, 'updateTime':self.process_item[item]['updateTime'], 'contentSign':
                   self.process_item[item]['contentSign']})

            i += 1
            if i % 10000 == 0:
                print "processed %f%%" % (float(i*100/total_cnt))

        q.put(None)

    def dispatcher_from_file(self, q):
        with open('%s_need_fix_ids.txt', 'rb') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                doc = self.cv_page_store.get_one(line)
                pagecontent = self.getPageContent(doc.get('pageContentPath'), self.bin_file_location)
                q.put({'indexUrl': line, 'pagecontent': pagecontent, 'updateTime':doc['updateTime'], 'contentSign':
                   doc['contentSign']})

        q.put(None)

    def getPageContent(self, filename, from_where='local'):
        if from_where == 'local':
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
        elif from_where == 'remote':
            #TODO
            # 从远程获取bin文件内容
            content = self.bin_read_client.getHtml(filename)
            return content

        else:
            raise Exception("unknown from_where")

    def check_has_duplication(self, indexUrl, ps):
        for s in ps.get('jobList', []):
            incName = s.get('incName', '')
            jobPosition = s.get('jobPosition', '')
            jobDesc = s.get('jobDesc', '')

            # 都不为空， 才会判重
            if not (incName and jobPosition and jobDesc):
                return False

            hash_value = util.md5([incName, jobPosition, jobDesc], remove_space=True)
            key = {'hash_value': hash_value}
            hash_doc = self.cv_hash_table.get_one(key)
            if hash_doc:
                # 统计重复数
                self._inc_duplication_count()

                # 如果此渠道优先级比较大， 替换掉存在hash表中的
                cvId_in_db = hash_doc.get('cvId')
                # 相同Id, 可能更新
                if cvId_in_db == indexUrl:
                    return False

                cv_channel_in_db = cvId_in_db.split('://')[0]
                if CHANNEL_PRIORITY.get(self.channel, 0) > CHANNEL_PRIORITY.get(cv_channel_in_db, 0):
                    hash_doc['cvId'] = indexUrl
                    self.cv_hash_table.upsert_one(key, hash_doc)
                    # remove 优先级低的， 保持解析数据没有重复
                    self.remove_duplication(cvId_in_db, cv_channel_in_db)

                    return False
                return True
            else:
                hash_doc = {'hash_value': hash_value, 'cvId': indexUrl}
                self.cv_hash_table.upsert_one(key, hash_doc)

    def remove_duplication(self, cvId, channel):
        key = {'cvId': cvId}
        if channel == 'cv_51job':
            self.cv_51job_raw_store.remove_one(key)
            self.cv_51job_measure_store.remove_one(key)
        elif channel == 'cv_zhilian':
            self.cv_zhilian_raw_store.remove_one(key)
            self.cv_zhilian_measure_store.remove_one(key)

        elif channel == 'cv_liepin':
            self.cv_liepin_raw_store.remove_one(key)
            self.cv_liepin_measure_store.remove_one(key)

        else:
            raise Exception('unknown channel')

    def real_run(self, job):

        indexUrl = job.get('indexUrl')
        pagecontent = job.get('pagecontent')
        #print pagecontent
        ps = self.cv_parser.parser(htmlContent=pagecontent, cvFrom=self.channel)

        # 判断是否重复cv
        if self.check_has_duplication(indexUrl, ps):
            return

        cvRaw_obj = constructCvRawObj(ps)
        cvRaw_obj.cvId = job.get('indexUrl')
        cvRaw_obj.cvFrom = self.channel
        cvRaw_obj.updateTime = job.get('updateTime')
        cvRaw_obj.contentSign = job.get('contentSign')

        cvRaw = convertToCvRaw(cvRaw_obj)
        cvMeasure = self.measure(cvRaw)

        if not cvMeasure:
            print cvRaw

        cvMeasured_obj = constructCvMeasureObj(cvMeasure)

        self.store(indexUrl, cvRaw_obj, cvMeasured_obj, False)

    def event_handler(self, evt, msg, **kwargs):
        if evt == 'DONE':
            util.send_email(["<jianghao@ipin.com>"], "{} 样本对比".format(self.channel), msg + '\n duplicate cvs: %d' % self._duplication_count +
                            '\n parsed count: %d' % self.parsed_cnt)





