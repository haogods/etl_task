#!/usr/bin/env python
# encoding: utf-8

from ipin.rpc.common.ttypes import IllegalArgumentException

def check(html):
    if not html:
        print "this resume is not ok"
        raise IllegalArgumentException(u'简历不合法')
