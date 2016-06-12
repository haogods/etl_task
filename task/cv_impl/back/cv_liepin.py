#!/usr/bin/env python
# -*- coding:utf-8 -*-


import sys
sys.path.append(sys.path[0]+"/../../")
from task.cv_impl.back.etl_base import ETLBase


class CvLPETL(ETLBase):
    def __init__(self, thread_cnt=10):
        ETLBase.__init__(self, 'cv_liepin', thread_cnt=thread_cnt)
        self.test_mode = False


if __name__ == "__main__":

    etl = CvLPETL(20)
    etl.run()
