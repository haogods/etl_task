#!/usr/bin/env python
# -*- coding:utf8 -*-


from task.base import BaseTask
from cv_parser.api_cv_parser import CvParser
from orm.cv_raw import MongoEngineCvRaw, constructCvRawObj, convertToCvRaw
from tools.log_util import Logger
from tools import util
import time
import os
import json
import csv
import copy
import codecs
import traceback
from tools.util import ExcelFileSave


class CvPS(BaseTask):
    def __init__(self, channel, prefix=None):
        BaseTask.__init__(self, channel, thread_cnt=2)
        self.channel = channel
        self.cv_parser = CvParser()
        self._cvs_data = {}

        #对比 临时存放文件
        self.diff_rs_dir = '%s_rs_diff' % self.channel if not prefix else '%s/%s_job_diff' % (prefix, self.channel)

        #机器解析
        self.mechine_rs_dir = '%s_mechine_parsed' % self.channel if not prefix else '%s/%s_mechine_parsed' % (prefix, self.channel)

        #人工解析
        self.person_rs_dir = '%s_person_parsed' % self.channel if not prefix else '%s/%s_person_parsed' % (prefix, self.channel)

        #结果
        self.result_file = '%s_result.txt' % self.channel if not prefix else '%s/%s_result.txt' % (prefix, self.channel)
        # util.check_and_clear(os.path.dirname(self.result_file))
        self.excel_save = ExcelFileSave(self.result_file)

        #样本
        self.sample_dir = '%s_sample' % self.channel if not prefix else '%s/%s_sample' % (prefix, self.channel)

    def start_operation(self, *args, **kwargs):
        # 清理准备工作
        print os.getcwd()
        for f in [self.diff_rs_dir, self.mechine_rs_dir]:
            if not os.path.isdir(f):
                os.system('mkdir -p %s' % f)
            else:
                os.system('rm -rf %s/*' % f)

    def dispatcher(self):
        fs = os.listdir(self.sample_dir)
        for file in fs:

            real_file = "%s/%s" % (self.sample_dir, file)
            if not os.path.isfile(real_file) or \
                os.path.splitext(real_file)[1] != '.html':
                continue

            fname = os.path.split(real_file)[1]
            cvId = os.path.splitext(fname)[0]
            self._queue.put({'fn': real_file, 'cvId': cvId})

    def run_job(self, job):

        if not job:
            return

        htmlfile = job.get('fn')
        cvId = job.get('cvId')

        with open(htmlfile, 'rb') as f:
            pagecontent = f.read()

        try:
            ps = self.cv_parser.parser(htmlContent=pagecontent, cvFrom=self.channel)
            cvRaw_obj = constructCvRawObj(ps)

            cvRaw_obj.cvId = "%s://%s" % (self.channel, cvId)
            cvRaw_obj.cvFrom = self.channel

            self._check_fields(cvRaw_obj.to_json(), cvId)
            # Logger.default_log("cvId: %s Ok" % cvId)
        except Exception as e:
            Logger.default_log("cvId: %s Fail" % cvId)
            traceback.print_exc()

    def _load_data(self):

        with open('%s/%s' % (self.person_rs_dir, 'parsed.csv'), 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter=',')
            for index, row in enumerate(reader):
                if index == 0:
                    continue
                cvId = row[0].split('.')[0]
                if '简历ID：' in cvId:
                    cvId = cvId.replace('简历ID：', '')
                parsed_data = row[1].replace('\n','')

                self._cvs_data.update({cvId: parsed_data})

    def _check_fields(self, cvRaw_obj, cvId):

        cvRaw_obj = json.loads(cvRaw_obj)
        person_parsed = self._cvs_data.get(cvId, '')
        if not person_parsed:
            return

        person_parsed = json.loads(person_parsed)

        print person_parsed

        diff_rs = copy.deepcopy(person_parsed)
        for key, value in person_parsed.items():

            if key == 'cvFrom':
                diff_rs['cvFrom'] = 0
                continue

            if key == 'baseInfo':
                for key1, value2 in value.items():
                    # 忽略cvId
                    if key1 == 'cvId':
                        diff_rs['baseInfo'][key1] = 0
                        continue

                    if value2 in [None,'None',''] and not cvRaw_obj['baseInfo'].get(key1, ''):
                        diff_rs['baseInfo'][key1] = 0
                        continue

                    if value2 != cvRaw_obj['baseInfo'].get(key1, ''):
                        diff_rs['baseInfo'][key1] = 1  # 解析不相同
                    else:
                        diff_rs['baseInfo'][key1] = 0  # 解析一样

            elif isinstance(value, (str, unicode)):
                diff_rs[key] = 0 if person_parsed[key] == cvRaw_obj.get(key, '') else 1

            elif key in ['languageList', 'proList', 'skillList', 'trainList', 'jobList', 'eduList', 'certList']:
                if not person_parsed[key]:
                    if not cvRaw_obj.get(key, ''):
                        diff_rs[key] = 0
                    else:
                        diff_rs[key] = 1
                    continue

                for index, languageItem in enumerate(person_parsed[key]):
                    # 为空
                    if not cvRaw_obj.get(key, ''):
                        diff_rs[key] = 1
                        break
                    elif cvRaw_obj.get(key, '') and len(cvRaw_obj.get(key)) < len(person_parsed[key]):
                        diff_rs[key] = 1
                        break
                    for key1, value1 in languageItem.items():

                        if cvRaw_obj[key][index].get(key1, '') != value1 and key1 != 'positionList':
                            diff_rs[key][index][key1] = 1 # 解析不同

                        elif key1 == 'positionList':
                            for positionIndex, positionItem in enumerate(value1):
                                if len(cvRaw_obj[key][index].get(key1, [])) < len(value1):
                                    diff_rs[key][index][key1] = 1
                                    continue

                                for key2, value2 in positionItem.items():
                                    diff_rs[key][index][key1][positionIndex][key2] = 0 if cvRaw_obj[key][index][key1][positionIndex].get(key2,'') == value2 else 1


                        else:
                            diff_rs[key][index][key1] = 0

            elif key in ['jobList']:
                if not person_parsed[key]:
                    if not cvRaw_obj.get(key, ''):
                        diff_rs[key] = 0
                    else:
                        diff_rs[key] = 1
                    continue





            elif key in ['others', 'privateInfo', 'jobExp']:
                if not person_parsed[key] or person_parsed in ['None', None]:
                    if not cvRaw_obj.get(key, ''):
                        diff_rs[key] = 0
                    else:
                        diff_rs[key] = 1

                for key1, value1 in person_parsed[key].items():
                    if value1 in ['None', None, '']:
                        if not cvRaw_obj[key].get(key1, ''):
                            diff_rs[key][key1] = 0
                        else:
                            diff_rs[key][key1] = 1

                        continue

                    if cvRaw_obj[key].get(key1) != value1:
                        diff_rs[key][key1] = 1 # 解析错误
                    else:
                        diff_rs[key][key1] = 0

        self._save_diff_rs(cvId, diff_rs)
        self._save_mechine_parsed(cvId, cvRaw_obj)
        self._save_people_parsed(cvId, person_parsed)
        # Logger.default_log('complete cvId: %s' % cvId)

        self._save_excel(cvId, person_parsed, cvRaw_obj, diff_rs)

    def _save_mechine_parsed(self, cvId, cvRaw_obj):
        with codecs.open("%s/%s.json" % (self.mechine_rs_dir, cvId), 'wb', encoding='utf-8') as f:
            f.write(json.dumps(cvRaw_obj, indent=4, ensure_ascii=False))

    def _save_people_parsed(self, cvId, person_parsed):
        with codecs.open("%s/%s.json" % (self.person_rs_dir, cvId), 'wb', encoding='utf-8') as f:
            f.write(json.dumps(person_parsed, indent=4, ensure_ascii=False))

    def _save_diff_rs(self, cvId, diff_rs):
        with open("%s/%s.json" % (self.diff_rs_dir, cvId), 'wb') as f:
            f.write(json.dumps(diff_rs, indent=4))

    def _save_excel(self, cvid, person_parsed, mechine_obj, diff_rs):
        self.excel_save.append([cvid])
        for key, value in diff_rs.items():
            if isinstance(value, (str, unicode, int)):
                self.excel_save.append(['', key, person_parsed[key], mechine_obj.get(key, ''), diff_rs[key]])

            if isinstance(value, dict):
                for key1, value2 in value.items():
                    if key1 == 'cvId':
                        continue

                    if isinstance(value2, (str, unicode, int)):
                        self.excel_save.append(['',"%s.%s"%(key, key1), person_parsed[key][key1], mechine_obj[key].get(key1,''), diff_rs[key][key1]])
                    else:
                        print type(value2)
                        raise Exception("%s.%s"%(key, key1))

            if isinstance(value, list):
                for index1, item1 in enumerate(value):
                    if isinstance(item1, dict):
                        for key2, value2 in item1.items():
                            if isinstance(value2, list):
                                for index2, item2 in enumerate(value2):
                                    if isinstance(item2, dict):
                                        for key3, value3 in item2.items():
                                            mechine_value = mechine_obj[key][index1].get(key2, '')
                                            if mechine_value and len(mechine_value) <= index2:
                                                mechine_value =0
                                            else:
                                                mechine_value = mechine_value[index2].get(key3, '')

                                            self.excel_save.append(['',"%s.%d.%s.%d.%s" % (key, index1, key2, index2, key3), person_parsed[key][index1][key2][index2][key3], mechine_value, diff_rs[key][index1][key2][index2][key3]])

                            else:
                                self.excel_save.append(['',"%s.%d.%s" % (key, index1, key2), person_parsed[key][index1][key2], mechine_obj[key][index1].get(key2, ''), diff_rs[key][index1][key2]])

                    else:
                        raise Exception("%s.%d" % (key, index1))

        self.excel_save.append([''])

    def event_handler(self, evt, msg, **kwargs):
        if evt == 'DONE':
            util.send_email(["<jianghao@ipin.com>"], "{} 样本对比".format(self.channel), msg)

            # 最终结果存放文件
            filedest = 'app/share/%s_result.txt' % self.channel

            if os.path.exists(filedest):
                now = util.get_date_with_day_duration()
                history_fn = os.path.join(os.path.dirname(filedest), '%s_%s.txt' % (self.channel, '%d%02d%02d%02d%02d' % (now.year, now.month, now.day, now.hour, now.minute)))

                # 将结果移动到历史文件
                os.system('mv %s %s' % (filedest, history_fn))

            # 将最后计算结果放到share 目录下， 提供下载
            os.system('mv %s %s' % (self.result_file, filedest))

