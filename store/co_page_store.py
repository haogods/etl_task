#coding=utf-8

from base import CBaseStore
import pymongo

import sys
sys.path.append(sys.path[0]+"/../../")


class CoPageStore(CBaseStore):
    def __init__(self, owner):
        CBaseStore.__init__(self)
        self.owner = owner
        self.db = "co_crawler"
        self.coll = 'page_store_{}'.format(self.owner)
        self.py_c = self.cmgClient.page_store_mongo_client[self.db][self.coll]

    def create_client(self, **kwargs):
        return self.cmgClient.jd_page_store_cmongo_client(self.db, self.coll)

    def get_all(self):
        return self.py_c.find()

    def remove_by_key(self, key):
        return self.py_c.remove(key)

    def set_expire(self, key):
        return self.py_c.update(key, {"$set": {"isExpired": 1, "isUpdated": 1}})

    def count_all(self):
        return self.py_c.count()



