#coding=utf8

import sys
sys.path.append(sys.path[0]+'/../../../')
print sys.path
from cv_parser.api_cv_parser import CvParser

cvparse = CvParser()

def test_51cv_parse():
    with open('cv_51job.html') as f:
        content = f.read()

    rs = cvparse.parser(content, cvFrom='cv_51job')
    return rs




if __name__ == '__main__':
    rs = test_51cv_parse()
    # from orm.cv_raw import MongoEngineCvRaw
    #
    # # ps = {"baseInfo": {
    # #     'age': '1'
    # # }}
    #
    # s = MongoEngineCvRaw()
    # m = s.constructCvRaw(rs)
    # print m.baseInfo.to_mongo()