#!/usr/bin/env python
# -*- coding:utf-8 -*-


from task.cv_impl.back.etl_base import ETLBase


class CvZLETL(ETLBase):
    def __init__(self, thread_cnt=10):
        ETLBase.__init__(self, 'cv_zhilian', thread_cnt=thread_cnt)
        self.test_mode = False


if __name__ == "__main__":

    etl = CvZLETL(20)
    etl.run()
