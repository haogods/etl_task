#!/usr/bin/env python
# -*- coding:utf8 -*-


log_when = __import__('log_when')



class A(object):

    def __init__(self):
        self.a = 1

    def _init_b(self):
        self.b = 2

    def __getattr__(self, item):
        if item == 'b':
            self._init_b()
            return self.b


my_max = getattr(log_when, 'my_max')
print my_max([1,2,3])




