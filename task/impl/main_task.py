# coding=utf-8

from jd_lagou import LGETLTask
from jd_zhilian import ZLETLTask
from jd_51job import Jd51ETLTask
from tc_import import TCImportTask
from task.base import BaseTask

import sys
import os
import shutil
import datetime


def Usage(exe_file):
    print "{} [jd_lagou] [jd_51job] [jd_zhilian] [2c_import]".format(exe_file)
    print "at least choose one"


def end_operation():

    back_dir = os.getcwd()+"/../data/back_data"
    if not os.path.isdir(back_dir):
        os.mkdir(back_dir)

    now = datetime.datetime.now()
    des_fn = back_dir+'/result_%04d%02d%02d' % (now.year, now.month, now.day)

    shutil.move(BaseTask.PathConfig.result_file, des_fn)


if __name__ == '__main__':
    if len(sys.argv) < 2:
        Usage(sys.argv[0])
        exit(1)

    if 'jd_lagou' in sys.argv[1:]:
        lagouT = LGETLTask(10)
        lagouT.run()

    if 'jd_51job' in sys.argv[1:]:
        jd51jobT = Jd51ETLTask(10)
        jd51jobT.run()
    if 'jd_zhilian' in sys.argv[1:]:
        zlT = ZLETLTask(10)
        zlT.run()
    if '2c_import' in sys.argv[1:]:
        tCT = TCImportTask(10)
        tCT.run()

    end_operation()


