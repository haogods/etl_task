#!/usr/bin/env python
# -*- coding:utf8 -*-

from tools.savebin import BinReader
import pymongo
import traceback

class GetHtmlPage(object):
    JdClient = pymongo.MongoClient('mongodb://hadoop2/jd_crawler')
    CvClient = pymongo.MongoClient('mongodb://hadoop2/cv_crawler')

    @staticmethod
    def get_jd_html_page(jdid):

        jd_db = "jd_crawler"
        try:
            coll = "page_store_%s" % jdid.split('://')[0]
        except Exception as e:
            traceback.print_exc()
            return None

        doc = GetHtmlPage.JdClient[jd_db][coll].find_one({'indexUrl':jdid})
        if not doc:
            print "jdid: %s not exists" % jdid
            return None

        return BinReader.getPageContent(doc['pageContentPath'])


    @staticmethod
    def get_cv_html_page(jdid):
        cv_db = 'cv_crawler'

        try:
            coll = "page_store_%s" % jdid.split('://')[0]
        except Exception as e:
            traceback.print_exc()
            return None

        doc = GetHtmlPage.CvClient[cv_db][coll].find_one({'indexUrl':jdid})
        if not doc:
            print "cvid: %s not exists" % jdid
            return None

        return BinReader.getPageContent(doc['pageContentPath'])



