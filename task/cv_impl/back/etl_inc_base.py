#!/usr/bin/env python
# -*- coding:utf8 -*-

"""
    增量更新

    直接从bin文件读取需要更新的cv
"""

from etl_base import ETLBase
from tools import util, savebin

from orm.cv_raw import constructCvRawObj, convertToCvRaw
from orm.cv_measure import constructCvMeasureObj

import traceback
import os


class ETL_V2(ETLBase):
    def __init__(self, channel):
        ETLBase.__init__(self, channel)
        self.pos_file_save_dir = os.path.join(os.path.dirname(__file__), self.channel)
        self.pos_file_save_path = '%s/%s_last_pos.dat' % (self.pos_file_save_dir, self.channel)
        self.bin_file_path, self.last_read_pos = self.get_bin_file_and_last_pos()

        # 记录二进制文件修改时间
        self.bin_file_modify_time = int(os.stat(self.bin_file_path).st_mtime)

        if not self.bin_file_path:
            raise Exception("not need handle, the file not update")

        self.bin_reader = savebin.BinReader(self.bin_file_path, self.last_read_pos)

    def get_bin_file_and_last_pos(self):

        # 之前文件更新过， 则继续处理
        filename, modified_time, last_pos = self._get_file_and_pos_from_saved_file()
        if filename:
            stat = os.stat(filename)
            if int(stat.st_mtime) > modified_time:
                return filename, int(last_pos)

        # 之前的文件处理完了，处理这个月的 （文件是按月切分的）
        path_template = "/data/crawler/_files3_/%s/%d/%s_%04d%02d.bin"
        today = util.get_date_with_day_duration(0)
        bin_file = path_template % (self.channel, today.year, self.channel, today.year, today.month)
        # 如果相等，则表示此文件是最新的，但是今天没有更新
        if filename == bin_file:
            return None, None

        return bin_file, 0

    def _get_file_and_pos_from_saved_file(self):
        if not os.path.exists(self.pos_file_save_path):
            return None, None, None

        filename = ""
        modified_time = 0
        last_pos = 0
        ls = os.popen("tail -n 1 %s" % self.pos_file_save_path).read().strip()
        if ls:
            (filename, modified_time, last_pos) = ls.split(":")

        return filename, modified_time, last_pos

    def _load_data(self):
        pass

    def real_run(self, job):

        indexUrl = job.get('indexUrl')
        pagecontent = job.get('pagecontent')
        #print pagecontent
        ps = self.cv_parser.parser(htmlContent=pagecontent, cvFrom=self.channel)
        # 判断是否重复cv
        if self.check_has_duplication(indexUrl, ps):
            return

        cvRaw_obj = constructCvRawObj(ps)
        cvRaw_obj.cvId = job.get('indexUrl')
        cvRaw_obj.cvFrom = self.channel

        cvRaw = convertToCvRaw(cvRaw_obj)
        cvMeasure = self.measure(cvRaw)
        cvMeasured_obj = constructCvMeasureObj(cvMeasure)

        self.store(indexUrl, cvRaw_obj, cvMeasured_obj)

    def dispatcher(self, q, from_which='db'):
        curpos = 0
        try:
            while True:
                (a, pagecontent) = self.bin_reader.readone()
                if a is None:
                    break
                curpos = self.bin_reader.fd.tell()
                channel, cvid, _ = a.split(".")

                q.put({
                    "indexUrl": "%s://%s" % (channel, cvid),
                    "pagecontent": pagecontent,

                })

        except Exception as e:
            traceback.print_exc()
        finally:
            self._save_pos(curpos)

    def _save_pos(self, pos):
        if not os.path.exists(self.pos_file_save_dir):
            os.mkdir(self.pos_file_save_path)

        value = "%s:%d:%d" % (self.bin_file_path, self.bin_file_modify_time, pos)
        with open(self.pos_file_save_path, 'a+b') as f:
            f.write(value + os.linesep)

