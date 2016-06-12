#!/usr/bin/env python
# -*- coding:utf8 -*-

'''
  ETL 多进程处理
'''

import os
os.environ['BIN_FILE_LOCATION'] = 'local'

from tools import util, log_util
from multiprocessing import Pool, Manager
from Queue import Empty
import traceback
import sys
from task.cv_impl.back import cv_zhilian, cv_51job, cv_liepin
import etl_inc_base


channel_etl_handlers = {

    'cv_zhilian': cv_zhilian.CvZLETL,
    'cv_51job': cv_51job.Cv51ETL,
    'cv_liepin': cv_liepin.CvLPETL,
    'increase_etl': etl_inc_base.ETL_V2,
}


def run(q, channel, _type):
    handler = channel_etl_handlers.get(channel)()
    cnt = 0
    while 1:
        job = get_job(q, _type)
        if job is None:
            break
        cnt += 1
        try:
            job_runner(job, handler)
            util.processMsgPrint("SUCCESS COPIED %s, cnt: %d" % (job['indexUrl'], cnt))
        except Exception as e:
            traceback.print_exc()
            log_util.Logger.default_log("[process %d]: FAIL COPIED %s" % (os.getpid(), job['indexUrl']))


def get_job(q, _type):
    try:
        if _type == 'all':
            job = q.get(True)
        else:
            job = q.get(True, 20)
        return job
    except Empty:
        return None


def dispatcher(q, channel, _type):

    if _type == 'all':
        handler = channel_etl_handlers.get(channel)()
    else:
        handler = channel_etl_handlers.get('increase_etl')(channel)
    try:
        handler._load_data()
        handler.dispatcher(q)
    except Exception as e:
        traceback.print_exc()


def job_runner(job, handler):
    handler.real_run(job)


if __name__ == '__main__':

    process_cnt = 4
    channel = 'cv_zhilian'
    _type = "all"  # 全量
    if len(sys.argv) >= 3 and sys.argv[2].isdigit():
        channel = sys.argv[1]
        process_cnt = int(sys.argv[2])

    if "inc" in sys.argv:
        _type = 'inc' # 增量

    manager = Manager()
    q = manager.Queue()
    p = Pool()

    # dispacther()
    p.apply_async(dispatcher, args=(q, channel, _type))

    for i in range(process_cnt):
        p.apply_async(run, args=(q, channel, _type))

    p.close()
    p.join()

