#!/usr/bin/env python
# -*- coding:utf8 -*-


import os
os.environ['BIN_FILE_LOCATION'] = 'remote'
from task.base import BaseTask
from task.cv_impl.dispatchers.bug_fix_dispatcher import BugFixDispatcher
from task.cv_impl.common.csv_field_write import CsvWriteBase
import os
import sys


class GetsRaw(BaseTask, CsvWriteBase):
    def __init__(self, channel, thread_cnt=5):

        BaseTask.__init__(self, "gets_raw", thread_cnt=thread_cnt)

        self.rawdispatcher = BugFixDispatcher(channel, self._queue)
        self.dispatcher = lambda q: self.rawdispatcher.dispatcher()
        self.channel = channel

        self.dir_path = self.get_save_file_dir()

        CsvWriteBase.__init__(self, self.dir_path, self.channel)

    def get_save_file_dir(self):
        _dir = os.path.join(os.path.dirname(__file__), '%s_%s_result' % (self.channel, self._name))
        if not os.path.exists(_dir):
            os.mkdir(_dir)
        return _dir

    def is_need(self, doc):
        nowInc = doc.get('baseInfo').get('nowInc', '')
        if not nowInc:
            return False

        if isinstance(nowInc, str):
            nowInc = nowInc.decode('utf-8')
        if u'北大纵横' in nowInc or u'温氏集团' in nowInc or u'金诚信矿业管理' in nowInc:
            print doc.get('cvId'), ' ===> ', nowInc.encode('utf-8')
            return True
        if doc.get('jobList', []):
            for jobItem in doc.get('jobList'):
                incName = jobItem['incName']
                if isinstance(incName, str):
                    incName = incName.decode('utf-8')

                if u'北大纵横' in incName or u'温氏集团' in incName or u'金诚信矿业管理' in incName:
                    return True

        return False

    def run_job(self, job):
        if self.is_need(job):
            self.save(job, None)
            print 'Copied cvId: ', job.get('cvId')

if __name__ == '__main__':
    for channel in sys.argv[1:]:
        g = GetsRaw(channel, 20)
        g.run()




