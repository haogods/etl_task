#coding=utf-8
import ctypes
import json
from ctypes import *
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

def calltest(a, b, c):
    so = ctypes.CDLL('./c_codes/libhello.so')
    so.test(str(a), int(b), long(c))

#so = ctypes.CDLL('./libmongo.so')
#client = so.CMongo()


class SayHello(object):
    def __init__(self):
        self._so = ctypes.CDLL('/home/godlike/PycharmProjects/jd_parse_task/c_codes/hello/so/libhello.so')
        self._ptr = self._so.new_SayHello()
    def hello(self, msg):
        self._so.SayHello_hello(self._ptr, str(msg))
    def __del__(self):
        self._so.delete_SayHello(self._ptr)

class newCMongo(object):
    def __init__(self):
        self._so = ctypes.CDLL('/home/godlike/PycharmProjects/jd_parse_task/c_codes/mongo/so/libmongo.so')
        # set return type
        self.set_return_type()
        self._ptr = self._so.new_CMongo()

    def set_return_type(self):
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
        r = self._so.save_str(self._ptr, s)
        return r

    def init_mongo(self, mongodb_url, dbname, coll):
        return self._so.init(self._ptr, c_char_p(mongodb_url), c_char_p(dbname), c_char_p(coll))

    def update_one(self, key, doc, upsert):
        return self._so.update_one(self._ptr, c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_one_no_wait(self, key, doc, upsert):
        return self._so.update_one_no_wait(self._ptr, c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_many(self, key, doc, upsert):
        return self._so.update_many(self._ptr, c_char_p(key), c_char_p(doc), c_bool(upsert))

    def update_many_no_wait(self, key, doc, upsert):
        return self._so.update_many_no_wait(self._ptr, c_char_p(key), c_char_p(doc), c_bool(upsert))

    def __del__(self):
        self._so.delete_CMongo(self._ptr)


# mg = newCMongo()
#
# print mg.init_mongo("mongodb://localhost:28017/cv_51job_raw", 'cv_51job_raw', 'cv_raw')
#
# key = json.dumps({'cvId': '11'})
# doc = json.dumps({'$set':{'test':1, 'mmm':123, "sssssss":123213, 'ff':33, 'qq':444}})
# mg.update_one(key, doc , True)
#
# print "before === "
# print mg.find_one(key)
# print "after ===="
# mg.delete_one(key)
# print mg.find_one(key)

