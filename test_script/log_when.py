#!/usr/bin/env python
# -*- coding:utf8 -*-

from time import time

def logged(when):
    def log(f, *args, **kwargs):
        print """Called:
        function: %s
        args: %r
        kargs: %r

                """ % (f, args, kwargs)

    def pre_log(f):
        def wrapper(*args, **kwargs):
            log(f, *args, **kwargs)
            return f(*args, **kwargs)

        return wrapper

    def post_log(f):
        def wrapper(*args, **kwargs):
            now = time()
            try:
                return f(*args, **kwargs)
            finally:
                log(f, *args, **kwargs)
                print "time delta: %s" % (time() - now)

        return wrapper


    try:
        return {"pre": pre_log,
                "post": post_log}[when]
    except KeyError, e:
        raise ValueError(e), 'must be "pre" or "post"'


@logged('post')
def hello():
    print "hello"


x = 10
def foo():
    y = 5
    bar = lambda y=y:x+y
    print bar(y)
    y = 8
    print bar(y)


def my_max(x):
    if not isinstance(x, list):
        x = list(x)
    return reduce(lambda x,y: x if x>y else y, x)


print my_max([1,2,35,5])