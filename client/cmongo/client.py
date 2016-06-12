# coding=utf-8

from ConfigParser import ConfigParser
from cmongo import newCMongo
import pymongo
import os

class Client(object):
    def __init__(self, fn):
        p = ConfigParser()
        p.read(fn)

        self._page_store_mongodb_url = p.get("page_store", "mongo_db_url")
        self._cv_page_store_mongodb_url = p.get("cv_page_store", "mongo_db_url")

        self._jd_51job_raw_store_mongo_url = p.get("jd_51job_parse_measure", "jd_raw_store_mongo_url")
        self._jd_51job_measure_store_mongo_url = p.get("jd_51job_parse_measure", "jd_measure_store_mongo_url")

        self._cv_51job_raw_store_mongo_url = p.get("cv_51job_parse_measure", "cv_raw_store_mongo_url")
        self._cv_51job_measure_store_mongo_url = p.get("cv_51job_parse_measure", "cv_measure_store_mongo_url")

        self._jd_zhilian_raw_store_mongo_url = p.get("jd_zhilian_parse_measure", "jd_raw_store_mongo_url")
        self._jd_zhilian_measure_store_mongo_url = p.get("jd_zhilian_parse_measure", "jd_measure_store_mongo_url")

        self._cv_zhilian_raw_store_mongo_url = p.get("cv_zhilian_parse_measure", "cv_raw_store_mongo_url")
        self._cv_zhilian_measure_store_mongo_url = p.get("cv_zhilian_parse_measure", "cv_measure_store_mongo_url")

        self._cv_liepin_raw_store_mongo_url = p.get('cv_liepin_parse_measure', 'cv_raw_store_mongo_url')
        self._cv_liepin_measure_store_mongo_url = p.get('cv_liepin_parse_measure', 'cv_measure_store_mongo_url')

        self._cv_hash_table = p.get('cv_hash_table', 'mongo_db_url')

        self._jd_lagou_raw_store_mongo_url = p.get("jd_lagou_parse_measure", "jd_raw_store_mongo_url")
        self._jd_lagou_measure_store_mongo_url = p.get("jd_lagou_parse_measure", "jd_measure_store_mongo_url")

        self._zhineng_salary5_charts_mongo_url = p.get("zhineng", "zhineng_salary5_charts_mongodb_url")
        self._inc_stats2_mongo_url = p.get("zhineng", "inc_stats2_mongo_url")

        self._2c_ipin_mongo_url = p.get("2c_ipin", "mongo_db_url")


    @property
    def toc_ipin_mongo_url(self):
        return self._2c_ipin_mongo_url

    @property
    def zhineng_salary5_charts_mongo_url(self):
        return self._zhineng_salary5_charts_mongo_url

    @property
    def inc_stats2_mongo_url(self):
        return self._inc_stats2_mongo_url

    @property
    def page_store_mongo_client(self):
        return pymongo.MongoClient(self._page_store_mongodb_url)

    @property
    def cv_page_store_mongo_client(self):
        return pymongo.MongoClient(self._cv_page_store_mongodb_url)

    @property
    def cv_hash_table_client(self):
        return pymongo.MongoClient(self._cv_hash_table)

    @property
    def page_store_mongodb_url(self):
        return self._page_store_mongodb_url

    @property
    def jd_51job_raw_store_mongo_url(self):
        return self._jd_51job_raw_store_mongo_url

    @property
    def jd_51job_measure_store_mongo_url(self):
        return self._jd_zhilian_measure_store_mongo_url

    @property
    def cv_51job_raw_store_mongo_url(self):
        return self._cv_51job_raw_store_mongo_url

    @property
    def cv_51job_measure_store_mongo_url(self):
        return self._cv_51job_measure_store_mongo_url



    @property
    def jd_zhilian_raw_store_mongo_url(self):
        return self._jd_zhilian_raw_store_mongo_url

    @property
    def jd_zhilian_measure_store_mongo_url(self):
        return self._jd_zhilian_measure_store_mongo_url

    @property
    def cv_zhilian_raw_store_mongo_url(self):
        return self._cv_zhilian_raw_store_mongo_url

    @property
    def cv_zhilian_measure_store_mongo_url(self):
        return self._cv_zhilian_measure_store_mongo_url

    @property
    def cv_liepin_raw_store_mongo_url(self):
        return self._cv_liepin_raw_store_mongo_url

    @property
    def cv_liepin_measure_store_mongo_url(self):
        return self._cv_liepin_measure_store_mongo_url


    @property
    def jd_lagou_raw_store_mongo_url(self):
        return self._jd_lagou_raw_store_mongo_url

    @property
    def jd_lagou_measure_store_mongo_url(self):
        return self._jd_lagou_measure_store_mongo_url

    def jd_51job_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_51job_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def jd_51job_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_51job_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_51job_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_51job_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_51job_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_51job_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def jd_page_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._page_store_mongodb_url, db_name, coll_name)
        return cmg

    def cv_page_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_page_store_mongodb_url, db_name, coll_name)
        return cmg

    def jd_zhilian_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_zhilian_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def jd_zhilian_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_zhilian_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_zhilian_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_zhilian_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_zhilian_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_zhilian_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_liepin_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_liepin_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def cv_liepin_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_liepin_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def jd_lagou_raw_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_lagou_raw_store_mongo_url, db_name, coll_name)
        return cmg

    def jd_lagou_measure_store_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._jd_lagou_measure_store_mongo_url, db_name, coll_name)
        return cmg

    def toC_ipin_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._2c_ipin_mongo_url, db_name, coll_name)
        return cmg

    def cv_hash_table_cmongo_client(self, db_name, coll_name):
        cmg = newCMongo(self._cv_hash_table, db_name, coll_name)
        return cmg

