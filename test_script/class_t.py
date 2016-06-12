#!/usr/bin/env python
# -*- coding:utf8 -*-

import threading

class A(object):
    count = 0
    r_lock = threading.RLock()

    def __init__(self):
        with self.r_lock:
            print "inc one"
            A.count += 1

            self.m = 0


    def __del__(self):

        with self.r_lock:
            print "delete one "
            A.count -= 1

    @staticmethod
    def howMany():
        return A.count


a = A()
print A.count

del a


# print dir(a)
# print a.__dict__
# print vars(a)




