#!/usr/bin/env python
# -*- coding:utf8 -*-


from base import CBaseStore
from tools.log_util import Logger
from pprint import pprint
import time

import sys
# sys.path.append(sys.path[0]+"/../../")

class CVPageStore(CBaseStore):
    def __init__(self, owner):
        CBaseStore.__init__(self)
        self.owner = owner
        self.db = "cv_crawler"
        self.coll = 'page_store_{}'.format(self.owner)
        self.py_c = self.cmgClient.cv_page_store_mongo_client[self.db][self.coll]

        self._bulk = self.py_c.initialize_unordered_bulk_op()
        self._bulk_size = 0


    def create_client(self, **kwargs):
        return self.cmgClient.cv_page_store_cmongo_client(self.db, self.coll)

    def get_all(self):
        return self.py_c.find()

    def get_one(self, cvId):
        key = {"indexUrl": cvId}
        return self.py_c.find_one(key)

    def remove_by_key(self, key):
        return self.py_c.remove(key)

    def set_status(self, cvId):
        key = {"indexUrl": cvId}
        return self.py_c.update(key, {"$set": {"status": 1}})

    def set_updateTime(self, cvId, updateTime):
        key = {'indexUrl': cvId}
        return self.py_c.update(key, {'$set': {"updateTime": updateTime}})

    def bulk_set_status(self, key):
        # key为空，立即执行execute
        if not key:
            if self._bulk_size:
                with self._lock:
                    self._bulk_execute()
            return

        self._bulk.find(key).upsert().update({"$set": {"status": 1}})

        with self._lock:
            self._bulk_size += 1
            if self._bulk_size % self._max_bulk_size == 0:
                self._bulk_execute()

    def _bulk_execute(self):
        Logger.default_log("start bulk execute, db: %s, collection: %s" % (self.db, self.coll))
        result = self._bulk.execute()
        self._unset_bulk()
        # pprint(result)
        # time.sleep(1)

    def _unset_bulk(self):
        self._bulk = self.py_c.initialize_unordered_bulk_op()
        self._bulk_size = 0

    def count_all(self):
        return self.py_c.count()








