#!/usr/bin/env python
# -*- coding:utf8 -*-

from tools import util
import sys
import os
import shutil

task_path = os.path.join(os.path.dirname(__file__), "../../../../task/cv_impl")
print task_path
sys.path.append(task_path)

import cv_sample_diff
import datetime


PREFIX = 'diff_result'


def save_sample_file(fname):
    if '51job' in fname:
        print os.getcwd()
        channel = 'cv_51job'
        target = os.path.join(os.path.dirname(__file__), '%s/cv_51job_sample' % PREFIX)
    elif 'zhilian' in fname:
        channel = 'cv_zhilian'
        target = os.path.join(os.path.dirname(__file__), '%s/cv_zhilian_sample' % PREFIX)

    elif 'liepin' in fname:
        channel = 'cv_liepin'
        target = os.path.join(os.path.dirname(__file__), '%s/cv_liepin_sample' % PREFIX)
    else:
        raise Exception('file name can not recognize')

    dest = '%s_sample' % channel
    util.untar(fname, dest)

    files = []
    find_files(dest, files)

    # 清理
    util.check_and_clear(target)

    for f in files:
        if os.path.split(f)[1].startswith('.'):
            continue
        shutil.move(f, target)

    # 清除 解压目录, 源文件
    os.system('rm -rf %s; rm -rf %s' % (dest, fname))


def save_parse_file(fname):
    if '51job' in fname:
        target = os.path.join(os.path.dirname(__file__), '%s/cv_51job_person_parsed' % PREFIX)
    elif 'zhilian' in fname:
        target = os.path.join(os.path.dirname(__file__), '%s/cv_zhilian_person_parsed' % PREFIX)
    elif 'liepin' in fname:
        target = os.path.join(os.path.dirname(__file__), '%s/cv_liepin_person_parsed' % PREFIX)
    else:
        raise Exception('file name can not recognize')

    util.check_and_clear(target)

    shutil.move(fname, target+'/parsed.csv')


def find_files(dir, rs):
    assert os.path.isdir(dir)
    assert isinstance(rs, list)

    files = os.listdir(dir)

    for file in files:

        filepath = '%s/%s' % (dir, file)
        if os.path.isdir(filepath):
            find_files(filepath, rs)
        else:
            rs.append(filepath)


def start_sample_diff(fname):

    if '51job' in fname:
        channel = 'cv_51job'
    elif 'zhilian' in fname:
        channel = 'cv_zhilian'
    elif 'liepin' in fname:
        channel = 'cv_liepin'
    else:
        raise Exception('unknown file name')

    diff = cv_sample_diff.CvPS(channel, prefix='app/controllers/diff_result')
    diff.run()

if __name__ == '__main__':
    start_sample_diff(sys.argv[1])