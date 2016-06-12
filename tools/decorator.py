#!/usr/bin/env python
# encoding: utf-8

import time
from log_util import Logger
import traceback
#import Extest


def timefunc(func):
    def _wrapper(*args, **kwargs):
        start = time.time()
        res = func(*args, **kwargs)
        Logger.default_log("function: %s, cost: %f" % (func.__name__, time.time()-start))
        return res
    return _wrapper


def timemethod(func):
    def _wrapper(*args, **kwargs):
        start = time.time()
        self = args[0]
        res = func(self, *args[1:], **kwargs)
        Logger.default_log("cls: %s, method: %s, cost: %f " % (self.__class__,
                                                               func.__name__, time.time()-start))
        return res
    return _wrapper


def handler_exception(ismethod=False):
    if ismethod:
        def _wrapper(func):
            def __wrapper(*args, **kwargs):
                try:
                    res = func(args[0], args[1:], **kwargs)
                    return res
                except Exception as e:
                    traceback.print_exc()
            return __wrapper
        return _wrapper

    else:
        def _wrapper(func):
            def __wrapper(*args, **kwargs):
                try:
                    res = func(*args, **kwargs)
                    return res
                except Exception as e:
                    traceback.print_exc()

            return __wrapper
        return _wrapper





@timefunc
def count(n):
    while n:
        n -= 1

@handler_exception(ismethod=False)
def p():
    s = "1" + 2

class M(object):
    @timemethod
    def countdown(self, n):
        while n:
            n -= 1

# @timefunc
# def c_count(n):
#     Extest.countdown(n)

if __name__ == '__main__':
    # s = M()
    # s.countdown(100000)
    # c_count(100000)
    p()
