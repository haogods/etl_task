#coding=utf-8


from client.cmongo.client import Client as cmgClient
import threading
import json
import os


class CBaseStore(object):
    def __init__(self):
        conf_path=os.path.join(os.path.dirname(__file__), '../conf/mongo_conf.ini')
        self.cmgClient = cmgClient(conf_path)
        self._tls = threading.local()
        self._lock = threading.RLock()
        self._max_bulk_size = 1000

    def get_clent(self):
        mgc = getattr(self._tls, 'mgc', None)
        if not mgc:
            mgc = self.create_client()
            setattr(self._tls, 'mgc', mgc)
        return mgc

    def create_client(self, **kwargs):
        raise Exception("Virtual function called")

    def save_one(self, obj_key, doc, isUpsert, wait=False):

        if isinstance(obj_key, dict):
            obj_key = json.dumps(obj_key)

        if isinstance(doc, dict):
            doc = json.dumps(doc, ensure_ascii=False)

        client = self.get_clent()
        if wait:
            return client.update_one(obj_key, doc, isUpsert)
        else:
            return client.update_one_no_wait(obj_key, doc, isUpsert)

    def save_many(self, obj_key, doc, isUpsert, wait=False):

        if isinstance(obj_key, dict):
            obj_key = json.dumps(obj_key)

        if isinstance(doc, dict):
            doc = json.dumps(doc, ensure_ascii=False)

        client = self.get_clent()
        if wait:
            return client.update_many(obj_key, doc, isUpsert)
        else:
            return client.update_many_no_wait(obj_key, doc, isUpsert)

    def find_one(self, obj_key):
        if isinstance(obj_key, dict):
            obj_key = json.dumps(obj_key)

        client = self.get_clent()

        return client.find_one(obj_key)

    def delete_one(self, obj_key):

        if isinstance(obj_key, dict):
            obj_key = json.dumps(obj_key)

        client = self.get_clent()
        return client.delete_one(obj_key)




