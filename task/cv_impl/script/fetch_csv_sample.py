#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
os.environ['BIN_FILE_LOCATION'] = 'remote'
from task.base import BaseTask
from task.cv_impl.dispatchers.bug_fix_dispatcher import RandomDispatcher
from task.cv_impl.common.csv_field_write import CsvWriteBase
from store.cv_raw_store import CVRawStore
import json
import sys


class FetchCsvSample(BaseTask, CsvWriteBase):

    def __init__(self, channel, thread_cnt, need_cnt):
        BaseTask.__init__(self, "fetch_csv_sample", thread_cnt=thread_cnt)
        self.channel = channel
        self.rand = RandomDispatcher(channel, self._queue, need_cnt)
        self.dispatcher = lambda q: self.rand.dispatcher()

        self.dir_path = self.get_save_dir_path()
        CsvWriteBase.__init__(self, self.dir_path, self.channel)

        self.cv_measure_store = CVRawStore(channel, 'measure')

    def _load_data(self):
        self.rand.load_data()

    def get_save_dir_path(self):
        dir = os.path.join(os.path.dirname(__file__), '%s_%s_result' % (self.channel, self._name))
        if not os.path.exists(dir):
            os.mkdir(dir)
        return dir

    def get_pagecontent(self, cvId):
        doc = self.rand.cv_page_store.get_one(cvId)
        filepath = doc['pageContentPath']
        return self.rand.getPageContent(filepath, 'remote')

    def save_html(self, cvId, pagecontent):
        path = os.path.join(self.dir_path, '%s_html_result' % self.channel)
        if not os.path.exists(path):
            os.mkdir(path)

        with open('%s/%s.html' % (path, cvId.split("://")[1]), 'wb') as f:
            f.write(pagecontent)

    def run_job(self, job):
        cvId = job.get('cvId')
        pagecontent = self.get_pagecontent(cvId)
        self.save_html(cvId, pagecontent)

        measure_data = self.cv_measure_store.get_one(cvId)

        self.save(job, measure_data)
        print "SUCCESS COPIED %s" % cvId

    def end_operation(self, *args, **kwargs):

        print "***********************************" * 2


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print "Usage: fetch_csv_sample.py channel need_cnt"
        exit(1)
    r = FetchCsvSample(sys.argv[1], 3, int(sys.argv[2]))
    r.run()





