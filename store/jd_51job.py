#coding=utf-8

from base import CBaseStore
import pymongo

class Job51EtlStore(CBaseStore):
    def __init__(self, stage='raw'):
        CBaseStore.__init__(self)
        self._stage = stage
        self.raw_db_name = "jd_51job_raw"
        self.measure_db_name = "jd_51job_measure"
        self.raw_coll_name = "jd_raw"
        self.measure_coll_name = "jd_measure"

        self.pymongo_raw_client = pymongo.MongoClient(self.cmgClient.jd_51job_raw_store_mongo_url)
        self.pymongo_measure_client = pymongo.MongoClient(self.cmgClient.jd_51job_measure_store_mongo_url)

    def create_client(self, **kwargs):
        if self._stage == 'raw':
            return self.cmgClient.jd_51job_raw_store_cmongo_client(self.raw_db_name, self.raw_coll_name)
        if self._stage == 'measure':
            return self.cmgClient.jd_51job_measure_store_cmongo_client(self.measure_db_name, self.measure_coll_name)


    def get_raw(self, jdId):
        key = {"jdId": jdId}
        return self.pymongo_raw_client[self.raw_db_name][self.raw_coll_name].find_one(key)

    def get_measure(self, jdId):
        key = {"jdId": jdId}
        return self.pymongo_measure_client[self.measure_db_name][self.measure_coll_name].find_one(key)

    def set_raw_expired(self, key):
        return self.pymongo_raw_client[self.raw_db_name][self.raw_coll_name].update(key, {"$set": {"isExpired": 1}})

    def set_measure_expired(self, key):
        return self.pymongo_measure_client[self.measure_db_name][self.measure_coll_name].update(key, {"$set": {"isExpired": 1}})
