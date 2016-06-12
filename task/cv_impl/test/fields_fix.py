#!/usr/bin/env python
# -*- coding:utf-8 -*-


from task.base import BaseTask


class FieldsFix(BaseTask):
    class Config(object):
        NEED_FIX_FIELDS = ["baseInfo.updateTime", "baseInfo.nowWorkAge", "baseInfo.nowSalary",
                           "jobExp.expSalary", "jobList.incEmployee", "jobList.jobDuration", "jobList.jobSalary"]

    def __init__(self):
        BaseTask.__init__(self, "fields_fix")



    def dispatcher(self):
        pass

    def run_job(self, job):
        pass