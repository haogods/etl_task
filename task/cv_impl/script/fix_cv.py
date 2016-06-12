#!/usr/bin/env python
# -*- coding:utf8 -*-

import os
os.environ['BIN_FILE_LOCATION'] = 'remote'
from task.cv_impl.dispatchers.bug_fix_dispatcher import BugFixDispatcher
from task.cv_impl.runner.etl_runner_base import ETLRunner
from task.base import BaseTask
import re
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )


class FixCv(BaseTask):
    def __init__(self, channel, thread_cnt=5):
        BaseTask.__init__(self, "fixbug", thread_cnt=thread_cnt)
        self.chanel = channel
        self.bugfix = BugFixDispatcher(channel, self._queue)
        self.dispatcher = lambda q: self.bugfix.dispatcher()

        self.etl_runner = ETLRunner(channel)

    def run_job(self, job):
        if not isinstance(job, dict):
            return

        jobExpSalary = job.get('jobExp',{}).get('expSalary', '').encode('utf-8')

        if not jobExpSalary:
            return

        find = re.search(r'(\d+)', jobExpSalary)
        if find:
            return
        else:

            if '面议' in jobExpSalary:
                print job.get('cvId'), " ==> ", "面议, 略过"
                return

            page_index = self.get_page_index(job.get('cvId', ''))
            print job.get('cvId'), " ==> ", jobExpSalary
            self.etl_runner.run(page_index)
            print "SUCESS COPIED %s" % job.get('cvId')

    def get_page_index(self, cvId):

        doc = self.bugfix.cv_page_store.get_one(cvId)

        pagecontent = self.bugfix.getPageContent(doc['pageContentPath'],'remote')
        if not pagecontent:
            print "cvId : %s, pagecontent null" % cvId

        job = {
            'indexUrl': doc['indexUrl'],
            'pagecontent': pagecontent,
            'updateTime':doc['updateTime'],
            'contentSign':doc['contentSign'],
        }

        return job

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print "Usage: ./fix_cv.py channel"
        exit(1)
    r = FixCv(sys.argv[1])
    r.run()



