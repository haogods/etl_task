#!/usr/bin/env python
# -*- coding:utf8 -*-

from lxml import html
from ipin.rpc.etl.jd.jd_type.ttypes import JdRaw

from impl import jdinc
from impl import jdjob


def parse(content):
    doc = html.fromstring(content)
    jdRaw = JdRaw(jdFrom="jd_lagou")
    jdRaw.jdInc = jdinc.parse(doc)
    jdRaw.jdJob = jdjob.parse(doc)
    return jdRaw

import time
if __name__ == "__main__":
    with open("impl/jd_lagou.html") as f:
        content = f.read()

    start = time.time()
    for i in range(0, 1000):
        jdRaw = parse(content)

    print time.time() - start



#############
# 正则解析1000次 花费：1.13秒
# xpath 解析1000次 花费： 5.27妙

# 5.27/1.13 = 4.66