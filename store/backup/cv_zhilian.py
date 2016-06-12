#coding=utf-8

from store.base import CBaseStore
import pymongo
from tools.log_util import Logger
from pprint import pprint
import time


class CvZLEtlStore(CBaseStore):
    def __init__(self, stage='raw', coll='cv_raw'):
        CBaseStore.__init__(self)
        self._stage = stage
        self._coll = coll

        if stage == 'raw':
            self.db_name = "cv_zhilian_raw"
            self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_zhilian_raw_store_mongo_url)
        elif stage == 'measure':
            self.db_name = "cv_zhilian_measure"
            self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_zhilian_measure_store_mongo_url)

        self._bulk = self.pymongo_client[self.db_name][self._coll].initialize_unordered_bulk_op()
        self._bulk_size = 0

    def create_client(self, **kwargs):
        if self._stage == 'raw':
            return self.cmgClient.cv_zhilian_raw_store_cmongo_client(self.db_name, self._coll)
        if self._stage == 'measure':
            return self.cmgClient.cv_zhilian_measure_store_cmongo_client(self.db_name, self._coll)

    def get_one(self, cvId):
        key = {"cvId": cvId}
        return self.pymongo_client[self.db_name][self._coll].find_one(key)

    def get_all(self):
        return self.pymongo_client[self.db_name][self._coll].find()

    def bulk_upsert(self, key, doc):
        if not key or not doc:
            if self._bulk_size:
                with self._lock:
                    self._bulk_execute()
            return

        self._bulk.find(key).upsert().update({"$set": doc})
        with self._lock:
            self._bulk_size += 1
            if self._bulk_size % self._max_bulk_size == 0:
                self._bulk_execute()

    def _bulk_execute(self):
        Logger.default_log("start bulk execute, db: %s, collection: %s" % (self.db_name, self._coll))
        result = self._bulk.execute()
        self._unset_bulk()
        # pprint(result)
        #
        # time.sleep(1)

    def _unset_bulk(self):
        self._bulk = self.pymongo_client[self.db_name][self._coll].initialize_unordered_bulk_op()
        self._bulk_size = 0

