#!/usr/bin/env python
# -*- coding:utf8 -*-

from task.base import BaseTask
import pymongo
import json
import re
import threading
import time
import traceback
from tools.filesave import FileSave

class FixLoc(BaseTask):
    def __init__(self, thread_cnt):
        BaseTask.__init__(self, 'fix_loc',thread_cnt=thread_cnt)
        self.measure_mongo_client = pymongo.MongoClient('mongodb://localhost:28017/cv_51job_measure')
        self.raw_mongo_client = pymongo.MongoClient('mongodb://localhost:28017/cv_51job_raw')

        self.raw_db = 'cv_51job_raw'
        self.raw_coll = 'cv_raw'

        self.measure_db = 'cv_51job_measure'
        self.measure_coll = 'cv_measure'

        self.update_cnt = 0
        self.r_lock = threading.RLock()

        self.locIdMap={}

    def _inc_update_num(self):
        self.r_lock.acquire()
        self.update_cnt += 1
        self.r_lock.release()

    def _load_data(self):
        self.load_locId()

    def load_locId(self):
        with open('../../conf/locations.json', 'rb') as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                line = json.loads(line)

                line['n'] = re.sub(ur'市|省|自治区', '', line['n'])
                self.locIdMap.update({line['n']: line['o'] + '00000000'})

    def dispatcher(self):
        index = 0
        for measure_doc in self.measure_mongo_client[self.measure_db][self.measure_coll].find():

            raw_doc = self.raw_mongo_client[self.raw_db][self.raw_coll].find_one({'cvId': measure_doc['cvId']})
            if not raw_doc:
                continue
            m_doc = {'cvId': measure_doc['cvId'], 'baseInfo': measure_doc['baseInfo'], 'jobList': measure_doc['jobList']}
            r_doc = {'baseInfo': raw_doc['baseInfo'], 'jobList': raw_doc['jobList']}

            self._queue.put({'raw': r_doc, 'measure': m_doc})
            index += 1
            if index % 10000 == 0:
                print 'load %d items' % index

    def measure_loc(self, content):
        for name, code in self.locIdMap.items():
            if name in content:
                print "%s => %s" % (content, code)
                return code
        return ''

    def run_job(self, job):
        if not job:
            return

        cvId = job['measure']['cvId']

        try:
            measure_doc = job['measure']
            raw_doc = job['raw']

            if 'nowAddress' in measure_doc['baseInfo']:
                address = raw_doc['baseInfo']['nowAddress']
                measure_doc['baseInfo']['nowAddress'] = self.measure_loc(address)
                if measure_doc['baseInfo']['nowAddress']:
                    self._inc_update_num()

            for index, jobItem in enumerate(measure_doc['jobList']):
                if 'incLocationId' in jobItem:
                    measure_doc['jobList'][index]['incLocationId'] = self.measure_loc(raw_doc['jobList'][index]['incName'])

            self.measure_mongo_client[self.measure_db][self.measure_coll].update({'cvId': cvId}, {"$set": measure_doc})
            print "SUCESS COPY: %s" % cvId
        except Exception as e:
            traceback.print_exc()
            print "Fail copy:  %s" % cvId

    def end_operation(self, *args, **kwargs):
        time.sleep(10)
        print 'Succuss update %d' % self.update_cnt

class GetNotMeasureAddress(FixLoc):
    def __init__(self, thread_cnt):
        FixLoc.__init__(self, thread_cnt)

        self.empty_address_cnt = 0
        self.fail_measure_cnt = 0
        self._save_file = FileSave('not_measure_addresses.json')
        self.empty_lock = threading.RLock()
        self.fail_measure_lock = threading.RLock()

    def _inc_empty_address_cnt(self):
        with self.empty_lock:
            self.empty_address_cnt += 1

    def _inc_fail_measure_cnt(self):
        self.fail_measure_cnt += 1


    def run_job(self, job):
        if not job:
            return
        cvId = job['measure']['cvId']

        measure_doc = job['measure']
        raw_doc = job['raw']
        address = raw_doc['baseInfo']['nowAddress']

        if not address:
            self._inc_empty_address_cnt()

        try:
            if 'nowAddress' in measure_doc['baseInfo'] and address:
                measure_doc['baseInfo']['nowAddress'] = self.measure_loc(address)
                if measure_doc['baseInfo']['nowAddress']:
                    self._inc_update_num()
                else:
                    tmp = {'cvId': cvId, 'nowAddress_raw':address, 'nowAddress_measure': measure_doc['baseInfo']['nowAddress']}
                    self._save_file.append_end_with(tmp)

                    if address and not measure_doc['baseInfo']['nowAddress']:
                        self._inc_fail_measure_cnt()

            print "SUCESS COPY: %s" % cvId
        except Exception as e:
            traceback.print_exc()
            print "Fail copy:  %s" % cvId

    def end_operation(self, *args, **kwargs):
        self._save_file.append_end_with('empty address cnt: %s' % self.empty_address_cnt)
        self._save_file.append_end_with('fail measure cnt: %s' % self.fail_measure_cnt)
        self._save_file.append_end_with('measure success cnt: %s' % self.update_cnt)


class GetCVJobList(FixLoc):

    def __init__(self, thread_cnt):
        FixLoc.__init__(self,thread_cnt)
        self._save_file = FileSave('cv_jobLists.json')

    def _load_data(self):
        pass

    def run_job(self, job):
        if not job:
            return
        cvId = job['measure']['cvId']
        try:
            measure_doc = job['measure']
            raw_doc = job['raw']

            # if not measure_doc['jobList']:
            #     return

            for index, jobItem in enumerate(measure_doc['jobList']):
                # if jobItem['incLocationId'] == '440300000000':
                jobItem.update({'cvId': cvId})
                jobItem.update({"incName": raw_doc['jobList'][index].get('incName')})
                del jobItem['incDesc']
                self._save_file.append_end_with(jobItem)
                self._inc_update_num()

            print "SUCESS COPY: %s" % cvId
        except Exception as e:
            traceback.print_exc()
            print "Fail copy:  %s" % cvId


if __name__ == '__main__':
    # s = FixLoc(30)
    # s = GetCVJobList(30)
    s = GetNotMeasureAddress(40)
    s.run()