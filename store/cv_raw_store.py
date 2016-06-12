#!/usr/bin/env python
# -*- coding:utf8 -*-

from base import CBaseStore
from tools.log_util import Logger
import pymongo


class CVRawStore(CBaseStore):
    def __init__(self, channel, stage='raw'):
        self.channel = channel
        CBaseStore.__init__(self)

        self.stage = stage
        self.db_name = '%s_%s' % (self.channel, self.stage)
        self.coll_name = 'cv_%s' % self.stage

    def _init_pymongo_client(self):
        if self.channel == 'cv_51job':
            if self.stage == 'raw':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_51job_raw_store_mongo_url)
            elif self.stage == 'measure':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_51job_measure_store_mongo_url)

        elif self.channel == 'cv_zhilian':
            if self.stage == 'raw':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_zhilian_raw_store_mongo_url)
            elif self.stage == 'measure':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_zhilian_measure_store_mongo_url)
        elif self.channel == 'cv_liepin':
            if self.stage == 'raw':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_liepin_raw_store_mongo_url)
            elif self.stage == 'measure':
                self.pymongo_client = pymongo.MongoClient(self.cmgClient.cv_liepin_measure_store_mongo_url)

        self._bulk = self.pymongo_client[self.db_name][self.coll_name].initialize_unordered_bulk_op()
        self._bulk_size = 0

    def __getattr__(self, item):

        # 用到才初始化
        if 'pymongo_client' == item:

            if 'pymongo_client' not in self.__dict__:
                self._init_pymongo_client()
                return self.pymongo_client

            else:
                return self.pymongo_client

    def create_client(self, **kwargs):
        if self.stage == 'raw':
            if self.channel == 'cv_51job':
                return self.cmgClient.cv_51job_raw_store_cmongo_client(self.db_name, self.coll_name)
            elif self.channel == 'cv_zhilian':
                return self.cmgClient.cv_zhilian_raw_store_cmongo_client(self.db_name, self.coll_name)
            elif self.channel == 'cv_liepin':
                return self.cmgClient.cv_liepin_raw_store_cmongo_client(self.db_name, self.coll_name)

        if self.stage == 'measure':
            if self.channel == 'cv_51job':
                return self.cmgClient.cv_51job_measure_store_cmongo_client(self.db_name, self.coll_name)

            elif self.channel == 'cv_zhilian':
                return self.cmgClient.cv_zhilian_measure_store_cmongo_client(self.db_name, self.coll_name)

            elif self.channel == 'cv_liepin':
                return self.cmgClient.cv_liepin_measure_store_cmongo_client(self.db_name, self.coll_name)

    def get_one(self, cvId):
        key = {"cvId": cvId}
        return self.pymongo_client[self.db_name][self.coll_name].find_one(key)

    def get_all(self):
        return self.pymongo_client[self.db_name][self.coll_name].find()

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
        Logger.default_log("start bulk execute, db: %s, collection: %s" % (self.db_name, self.coll_name))
        result = self._bulk.execute()
        Logger.default_log("result : %s" % result)
        self._unset_bulk()

    def _unset_bulk(self):
        self._bulk = self.pymongo_client[self.db_name][self.coll_name].initialize_unordered_bulk_op()
        self._bulk_size = 0

    def remove_one(self, key):
        return self.pymongo_client[self.db_name][self.coll_name].remove(key, multi=False)

    def count_all(self):
        return self.pymongo_client[self.db_name][self.coll_name].count()