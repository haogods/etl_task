#coding=utf-8

from base import CBaseStore
import pymongo

class ToCMeasureStore(CBaseStore):
    def __init__(self):
        CBaseStore.__init__(self)
        self.db = "2c_ipin"
        self.coll = "jd_measure"
        self.py_c = pymongo.MongoClient(self.cmgClient.toc_ipin_mongo_url)

    def create_client(self, **kwargs):
        return self.cmgClient.toC_ipin_cmongo_client(self.db, self.coll)

    def set_expired(self, key):
        return self.py_c[self.db][self.coll].update(key, {"$set": {"isExpired": 1}})