#!/usr/bin/env python
# -*- coding:utf8 -*-


from base import CBaseStore


class CVHashTable(CBaseStore):
    def __init__(self):
        CBaseStore.__init__(self)
        self.db_name = 'cv_hash'
        self.coll_name = 'cv_hash'
        self.pymongo_client = self.cmgClient.cv_hash_table_client[self.db_name][self.coll_name]

    def get_one(self, key):
         return self.pymongo_client.find_one(key)

    def get_many(self, key):
        return self.pymongo_client.find(key)

    def upsert_one(self, key, doc):
        return self.pymongo_client.update(key, doc, upsert=True)

    def upsert_many(self, key, doc):
        return self.pymongo_client.update(key, doc, upsert=True, multi=True)

    def create_client(self, **kwargs):
        return self.cmgClient.cv_hash_table_cmongo_client(self.db_name, self.coll_name)
