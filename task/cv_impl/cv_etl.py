#!/usr/bin/env python
# -*- coding:utf8 -*-

'''
  ETL 多进程处理
'''

import os
os.environ['BIN_FILE_LOCATION'] = 'remote'

from task.cv_impl.dispatchers.etl_dipatcher import ETLDispatcher, IncEtlDispatcher, ETLDisFromRaw, ETLDisFromFile, ETLDisFromRawAfterCheck
from task.cv_impl.runner.etl_runner_base import ETLRunner, ETLRunnerFromRaw
from tools import util, log_util
from tools.filesave import FileSave
from multiprocessing import Pool, Manager
from Queue import Empty
from task.cv_impl.common.task_common import notify_emails
import traceback
import sys
import datetime
import time
from tools.util import send_email


def run(q, channel, _type):

    if _type in ['raw', 'check']:
        handler = ETLRunnerFromRaw(channel)
    else:
        handler = ETLRunner(channel)
    sucess_cnt = 0
    fail_cnt = 0

    fail_save_file_name = 'result/%s_fail_ids_%d.txt' % (channel, os.getpid())
    fail_save_file = FileSave(fail_save_file_name)

    result_file_name = 'result/%s_statistics_%d.txt' % (channel, os.getpid())
    result_file = FileSave(result_file_name)

    while 1:
        job = get_job(q, _type)
        if job is None:
            break
        try:
            handler.run(job)
            sucess_cnt += 1
            print >> result_file.fd, "%s" % (job['indexUrl'])
            if sucess_cnt % 1000 == 0:
                print "process %d, time: %s, success copied: %d, " \
                      "fail copied: %d, fail_save_file: %s, result_file: %s" % (os.getpid(),
                                                                                time.ctime(),
                                                                                sucess_cnt,
                                                                                fail_cnt,
                                                                                fail_save_file_name,
                                                                                result_file_name)
        except Exception as e:
            traceback.print_exc()
            fail_cnt += 1
            fail_save_file.append_end_with(job['indexUrl'])


def get_job(q, _type):
    try:
        if _type == 'all':
            job = q.get(True)
        else:
            job = q.get(True, 20)
        return job
    except Empty:
        return None


def dispatcher(q, channel, _type, filename=None):
    try:
        if _type == 'all':
            handler = ETLDispatcher(channel, q)
        elif _type == 'inc':
            handler = IncEtlDispatcher(channel, q)
        elif _type == 'raw':
            handler = ETLDisFromRaw(channel, q)
        elif _type == 'file':
            handler = ETLDisFromFile(channel, q, filename)

        elif _type == 'check':
            handler = ETLDisFromRawAfterCheck(channel, q)

        else:
            raise Exception('unknown dispather type: %s' % _type)

        handler.dispatcher()
    except Exception as e:
        traceback.print_exc()


def clean(channel):
    result_dir = "result/%s_result" % channel
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)
    now = time.localtime()
    now_str = "%04d%02d%02d" % (now.tm_year, now.tm_mon, now.tm_mday)
    fail_ids_file = "%s/%s_fail_ids_%s" % (result_dir, channel,now_str)
    cmd = "cat result/%s_fail_ids* | sort | uniq > %s; rm result/%s_fail_ids*" % (channel,  fail_ids_file, channel)
    print cmd
    os.system(cmd)
    fail_cnt = util.statistic_line(fail_ids_file)

    success_ids_file = "%s/%s_statistics_%s" % (result_dir, channel, now_str)
    cmd = "cat result/%s_statistics* | sort | uniq > %s; rm result/%s_statistics*" % (channel, success_ids_file, channel)
    print cmd
    os.system(cmd)
    success_cnt = util.statistic_line(success_ids_file)

    dupli_ids_file = "%s/%s_duplicate_ids_%s" % (result_dir, channel, now_str)
    cmd = "cat result/%s_duplicate_ids* | sort | uniq > %s; rm result/%s_duplicate_ids*" % (channel, dupli_ids_file, channel)
    os.system(cmd)

    dupli_cnt = util.statistic_line(dupli_ids_file)

    notify_title = "%s etl" % channel
    notify_msg = """

    prog: %s

    result：
            success_cnt: %d,
            fail_cnt: %d,
            dupli_cnt: %d

    """ % (' '.join(sys.argv), success_cnt, fail_cnt, dupli_cnt)

    send_email(notify_emails, notify_title, notify_msg)


if __name__ == '__main__':

    if len(sys.argv) < 4:
        print "Usage: cv_etl.py channel process_cnt [all|inc|check]"
        exit(1)

    channel = sys.argv[1]
    process_cnt = int(sys.argv[2])
    _type = sys.argv[3]

    file_name = None
    if "file" in sys.argv:
        if len(sys.argv) < 5:
            print "Usage: cv_etl.py channel process_cnt file file_name"
            exit(1)
        file_name = sys.argv[4]

    manager = Manager()
    q = manager.Queue(maxsize=300)
    p = Pool(processes=10)

    p.apply_async(dispatcher, args=(q, channel, _type, file_name))

    for i in range(process_cnt):
        p.apply_async(run, args=(q, channel, _type))

    p.close()
    p.join()

    # 清理工作
    clean(channel)


