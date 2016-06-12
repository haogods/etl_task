#coding=utf-8
import ctypes
from ctypes import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')
import os
import json



class newCMongo(object):
    def __init__(self, mongodb_url, db_name, coll):
        self._so = ctypes.CDLL(os.path.join(os.path.dirname(__file__), '../../c_codes/mongo/so/libmongo.so'))
        # set return type
        self.set_return_type()
        self._ptr = self._so.new_CMongo()
        if not self._init_mongo(mongodb_url, db_name, coll):
            raise Exception("init mongo fail")

    def set_return_type(self):
        self._so.new_CMongo.restype = c_size_t
        self._so.save_str.restype = c_char_p
        self._so.init.restype = c_bool
        self._so.update_one.restype = c_bool
        self._so.update_one_no_wait.restype = c_bool
        self._so.update_many.restype = c_bool
        self._so.update_many_no_wait.restype = c_bool

        self._so.find_one.restype = c_char_p
        self._so.delete_one.restype = c_bool

    def find_one(self, key):
        if isinstance(key, dict):
            key = json.dumps(key)

        r = self._so.find_one(self._ptr, c_char_p(key))
        if r:
            return json.loads(r)
        else:
            return None

    def delete_one(self, key):
        if isinstance(key, dict):
            key = json.dumps(key)

        r = self._so.delete_one(self._ptr, c_char_p(key))
        return r

    def save_str(self, s):
        r = self._so.save_str(c_size_t(self._ptr), s)
        return r

    def _init_mongo(self, mongodb_url, dbname, coll):
        return self._so.init(c_size_t(self._ptr), c_char_p(mongodb_url), c_char_p(dbname), c_char_p(coll))

    def update_one(self, key, doc, upsert):
        return self._so.update_one(c_size_t(self._ptr), c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_one_no_wait(self, key, doc, upsert):
        return self._so.update_one_no_wait(c_size_t(self._ptr), c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_many(self, key, doc, upsert):
        return self._so.update_many(c_size_t(self._ptr), c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_many_no_wait(self, key, doc, upsert):
        return self._so.update_many_no_wait(c_size_t(self._ptr), c_char_p(key), c_char_p(doc), c_bool(upsert))

    def __del__(self):
        self._so.delete_CMongo(c_size_t(self._ptr))