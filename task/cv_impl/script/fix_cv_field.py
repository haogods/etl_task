#!/usr/bin/env python
# -*- coding:utf8 -*-


import os
os.environ['BIN_FILE_LOCATION'] = 'remote'
from task.cv_impl.dispatchers.bug_fix_dispatcher import BugFixDispatcher
from task.cv_impl.runner.etl_runner_base import ETLRunner, ETLRunnerFromRaw
from task.base import BaseTask
import re
import traceback
import sys
reload(sys)
sys.setdefaultencoding( "utf-8" )

class FixCvField(BaseTask):
    def __init__(self, channel, field, field_pattern):
        BaseTask.__init__(self, "fix_cv_field")
        self.channel = channel
        self.field = field
        self.field_pattern = field_pattern

        self.bugfix = BugFixDispatcher(channel, self._queue)
        self.dispatcher = lambda q: self.bugfix.dispatcher()

        self.etl_runner = ETLRunnerFromRaw(channel)


    def run_job(self, job):

        try:
             if not isinstance(job, dict):
                return

             split_field = self.field.split('.')
             if len(split_field) == 2:
                 field_1 = getattr(job, split_field[0])
                 if isinstance(field_1, list):
                     for item in field_1:
                         field_2 = getattr(item, split_field[1])
                         assert isinstance(field_2, basestring)
                         if re.search(self.field_pattern, field_2):
                             # 匹配模式，重新量化
                             print "cvId: ", job.get('cvId'), " || ",
                             print self.field_pattern, " : ", field_2, " || ",
                             self.etl_runner.run(job)

        except Exception as e:
            traceback.print_exc()

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage: fix_cv_field.py channel field field_pattern"
        exit(1)

    channel, field, field_pattern = sys.argv[1:]
    bugfix = FixCvField(channel, field, field_pattern)
    bugfix.run()






