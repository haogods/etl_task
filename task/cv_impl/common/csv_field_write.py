#!/usr/bin/env python
# -*- coding:utf8 -*-

from tools.util import utf8str
from tools.filesave import CsvFile


def fetch_handler(key, raw_or_measure):
    if key == 'baseInfo':
        if raw_or_measure == 'raw':
            return cv_raw_baseInfo_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_baseInfo_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'jobExp':
        if raw_or_measure == 'raw':
            return cv_raw_jobExp_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_jobExp_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'eduList':
        if raw_or_measure == 'raw':
            return cv_raw_eduList_csv_write

        elif raw_or_measure == 'measure':
            return cv_measure_eduList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'jobList':
        if raw_or_measure == 'raw':
            return cv_raw_jobList_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_jobList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'proList':
        if raw_or_measure == 'raw':
            return cv_raw_proList_csv_write
        if raw_or_measure == 'measure':
            return cv_measure_proList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'trainList':
        if raw_or_measure == 'raw':
            return cv_raw_trainList_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_trainList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'languageList':
        if raw_or_measure == 'raw':
            return cv_raw_languageList_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_languageList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'certList':
        if raw_or_measure == 'raw':
            return cv_raw_certList_csv_write

        elif raw_or_measure == 'measure':
            return cv_measure_certList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'skillList':
        if raw_or_measure == 'raw':
            return cv_raw_skillList_csv_write
        elif raw_or_measure == 'measure':
            return cv_measure_skillList_csv_write

        raise Exception('raw or measure ? choose one')

    if key == 'privateInfo':
        return cv_privateInfo_csv_write

    if key == 'others':
        return cv_raw_others_csv_write


def check_csv_head(csv_file, header):
    if not csv_file.is_set_header:
        csv_file.set_header(header)


def csv_write_c(csv_file, header, value):
    check_csv_head(csv_file, header)

    row = []
    for k in header:
        v = value.get(k, '')
        row.append(utf8str(v))

    print row
    csv_file.append_row(row)


def cv_raw_baseInfo_csv_write(csv_file, cvId, value):

    header = ['cvId','age', 'gender', 'dob', 'height', 'marriage', 'nowAddress', 'nowDiploma',
              'nowHukou', 'nowInc', 'nowIndustry', 'nowPoliStatus', 'nowPosition', 'nowSalary',
              'nowWorkAge', 'nowZipCode', 'nowLocation', 'overSea', 'recentMajorName',
              'recentSchName', 'recentJobDuration', 'updateTime', 'nowAddressDetail',
              'country', 'baseSalary', 'bonus', 'subsidy', 'stock', 'credit', 'annualIncome']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


def cv_measure_baseInfo_csv_write(csv_file, cvId, value):

    header = ['cvId','age', 'gender', 'dob', 'height', 'marriage', 'nowAddress', 'nowDiploma',
              'nowHukouLocId', 'nowIncId', 'nowIndustryId', 'nowPoliStatus', 'nowPosition', 'nowSalaryMin',
              'nowSalaryMax', 'nowWorkAgeMin', 'nowWorkAgeMax', 'nowZipCode', 'nowLocationId', 'nowOverSea',
              'recentMajorId', 'recentSchId', 'recentJobDuration', 'updateTime', 'nowAddressDetail',
              'country', 'baseSalaryMin', 'baseSalaryMax', 'bonus', 'subsidy', 'stock', 'credit', 'annualIncome']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


def cv_raw_jobExp_csv_write(csv_file, cvId, value):

    header = ['cvId', 'dutyTime', 'expIndustrys', 'expJobCates', 'expJobTypes',
              'expLocations', 'expSalary', 'ignoreIncs', 'workStatus', 'expPositions','notes']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


def cv_measure_jobExp_csv_write(csv_file, cvId, value):

    header = ['cvId', 'dutyTime', 'expIndustryIdList', 'expJobCates', 'expJobTypeList',
              'expLocationIdList', 'expSalaryMin', 'expSalaryMax', 'ignoreIncs',
              'workStatus', 'expPositionList','notes']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


def cv_privateInfo_csv_write(csv_file, cvId, value):

    header = ['cvId', 'keywords', 'userName', 'phoneNumber',
              'email', 'qq', 'idNumber', 'wechatNum', 'homePage']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


def cv_raw_eduList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'eduStart', 'eduEnd', 'schName', 'majorName',
              'eduDesc', 'eduDiploma', 'eduTongZhao', 'eduType']

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_eduList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'eduStart', 'eduEnd', 'schId', 'majorId',
              'eduDesc', 'eduDiploma', 'eduTongZhao', 'eduType']

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_jobList_csv_write(csv_file, csv_jobPosition_file, cvId, items):

    header = ['cvId', 'itemId', 'incName', 'incDesc', 'incEmployee', 'incIndustrys',
              'incType', 'incLocation', 'jobStart', 'jobEnd', 'jobDuration', 'jobSalary', 'jobDepartment',
              'jobPosition', 'jobDesc', 'jobSubTeamSize', 'jobAchievement', 'reportTo',
              'underNum', 'witness', 'leaveReason', 'achieveDesc', 'directUnder', 'yearlyIncome']

    jobPosition_header = ['cvId', 'itemId', 'positionStart', 'positionEnd', 'jobPosition', 'location',
                          'department', 'report', 'subTimeSize', 'salary', 'jobDesc', 'jobAchievement']

    for item in items:
        item.update({'cvId': cvId})
        if item.get('positionList',[]):
            for position in item.get('positionList'):
                position.update({'cvId': cvId})
                csv_write_c(csv_jobPosition_file, jobPosition_header, position)

        csv_write_c(csv_file, header, item)


def cv_measure_jobList_csv_write(csv_file, csv_jobPosition_file, cvId, items):

    header = ['cvId', 'itemId', 'incSegmentId', 'incDesc', 'incEmployeeMin', 'incEmployeeMax', 'incIndustryIdList',
              'incType', 'incLocationId', 'jobStart', 'jobEnd', 'jobDuration', 'jobSalaryMin', 'jobSalaryMax', 'jobDepartment',
              'jobPosition', 'jobDesc', 'jobSubTeamSize', 'jobAchievement', 'reportTo',
              'underNum', 'witness', 'leaveReason', 'achieveDesc', 'directUnder', 'yearlyIncome']

    jobPosition_header = ['cvId', 'itemId', 'positionStart', 'positionEnd', 'jobPosition', 'location',
                          'department', 'report', 'subTimeSize', 'salaryMin', 'salaryMax', 'jobDesc', 'jobAchievement']

    for item in items:
        item.update({'cvId': cvId})
        if item.get('positionList',[]):
            for position in item.get('positionList'):
                position.update({'cvId': cvId})
                csv_write_c(csv_jobPosition_file, jobPosition_header, position)

        csv_write_c(csv_file, header, item)


def cv_raw_proList_csv_write(csv_file,  cvId, items):

    header = ['cvId', 'itemId', 'proStart', 'proEnd', 'proName', 'proDuty', 'proDesc',
              'softwareEnv', 'hardwareEnv', 'devTool', 'proAchievement', 'proIncName', 'proRole']

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_proList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'proStart', 'proEnd', 'proName', 'proDuty', 'proDesc',
              'softwareEnv', 'hardwareEnv', 'devTool', 'proAchievement', 'proIncName', 'proRole']

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_trainList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'trainStart', 'trainEnd', 'trainAgency', 'trainTitle', 'trainContent',
              'trainDesc', 'trainLoc', 'trainCert',]

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_trainList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'trainStart', 'trainEnd', 'trainAgency', 'trainTitle', 'trainContent',
              'trainDesc', 'trainLoc', 'trainCert',]

    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_languageList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'languageName', 'languageLevel']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_languageList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'languageName', 'languageLevel']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_certList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'certTime', 'certName', 'certLevel']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_certList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'certTime', 'certName', 'certLevel']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_skillList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'skillName', 'skillLevel', 'skillDuration']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_measure_skillList_csv_write(csv_file, cvId, items):

    header = ['cvId', 'itemId', 'skillName', 'skillLevel', 'skillDuration']
    for item in items:
        item.update({'cvId': cvId})
        csv_write_c(csv_file, header, item)


def cv_raw_others_csv_write(csv_file, cvId, value):

    header = ['cvId', "otherHobby","pubWork","socialActie","specialSkill","professionOrg","patent","longCareerGoal","religion",
                     "recommendLetter","schoolExp","otherInfo","gainHoner","selfIntro","stuPractice","attachment","cvName",
                     "jobIncNameFor","jobIncNameFor","workLoc", 'personIntro', 'glory', 'personResource']

    value.update({'cvId': cvId})
    csv_write_c(csv_file, header, value)


class CsvWriteBase(object):

    def __init__(self, dirpath, channel):
        self.is_file_init = False
        self.dir_path = dirpath
        self.channel = channel

        self.positionList_raw = CsvFile('%s/positionList_raw.csv' % self.dir_path)
        self.positionList_measure = CsvFile('%s/positionList_raw.csv' % self.dir_path)

    def init_file(self, job):
        for key, value in job.items():
            if not isinstance(value, (dict, list)):
                continue
            exec ("self.%s_raw = CsvFile('%s/%s_raw.csv')" % (key, self.dir_path, key))
            exec ("self.%s_measure = CsvFile('%s/%s_measure.csv')" % (key, self.dir_path, key))

        self.is_file_init = True

    def save(self, raw, measure):
        if not self.is_file_init:
            self.init_file(raw)

        if raw:
            for key, value in raw.items():
                self.write_content_raw(raw.get('cvId'), key, value)

        if measure:
            for key, value in measure.items():
                self.write_content_measure(measure.get('cvId'), key, value)

    def write_content_measure(self, cvId, key, value):
        if not isinstance(value, (dict, list)):
            return

        measure_handler = fetch_handler(key, 'measure')

        if key == 'jobList':
            measure_handler(self.jobList_measure, self.positionList_measure, cvId, value)
            return

        measure_file = getattr(self, "%s_measure" % key)
        measure_handler(measure_file, cvId, value)

    def write_content_raw(self, cvId, key, value):

        if not isinstance(value, (dict, list)):
            return

        raw_handler = fetch_handler(key, 'raw')

        if key == 'jobList':
            raw_handler(self.jobList_raw, self.positionList_raw, cvId, value)
            return

        raw_file = getattr(self, "%s_raw" % key)
        raw_handler(raw_file, cvId, value)


if __name__ == '__main__':
    csv = CsvFile('ttt.csv')
    csv_write_c(csv, ['a', 'b'], {'a': 1, 'b':2})
    csv_write_c(csv, ['a', 'b'], {'a': 3, 'b':4})
