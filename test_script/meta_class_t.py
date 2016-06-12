#!/usr/bin/env python
# -*- coding:utf8 -*-

from time import ctime
from warnings import warn

class ReqStrSugRepr(type):
    def __init__(cls, name, bases, attrd):
        super(ReqStrSugRepr, cls).__init__(name, bases, attrd)

        if '__str__' not in attrd:
            raise TypeError("Class requires ovverriding of __str__()")

        if '__repr__' not in attrd:
            warn('Class suggests overriding of __repr__()\n', stacklevel=3)


class MetaC(type):
    def __init__(self, name, bases, attrd):
        super(MetaC, self).__init__(name, bases, attrd)
        print "*** created class %r at : %s" % (name, ctime())



class Foo(object):

    __metaclass__ = ReqStrSugRepr
    def __init__(self):
        print "** Instantiated class %r at: %s" % (self.__class__.__name__, ctime())


    def __str__(self):
        return "<class %r>" % self.__class__.__name__


    def __call__(self, *args, **kwargs):
        for i in args:
            print i

        for k, v in kwargs.items():
            print "%s=%s" % (k, v)

    __repr__ = __str__


f = open('test.txt')
exec f




