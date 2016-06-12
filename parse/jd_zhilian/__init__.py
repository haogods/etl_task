#!/usr/bin/env python
# -*- coding:utf8 -*-

from lxml import html
from ipin.rpc.etl.jd.jd_type.ttypes import JdRaw
from ipin.rpc.common.datatype.ttypes import DtString
from ipin.infrastructure.datatype.flag import DataFlag

# from impl.checker import check
from impl import jdinc
from impl import jdjob


def parse(content):
    # check(content)
    doc = html.fromstring(content)
    jdRaw = JdRaw(jdFrom="jd_zhilian")
    jdRaw.jdUrl = ""
    jdRaw.jdInc = jdinc.parse(doc)
    jdRaw.jdJob = jdjob.parse(doc)
    pub_date = doc.xpath("//ul[@class='terminal-ul clearfix']/li[3]/strong")
    if pub_date:
        jdRaw.pubDate = DtString(pub_date[0].text_content(), DataFlag(hasValue=True).toByte())
    return jdRaw


import time
if __name__ == '__main__':
    with open('jd_zhilian.html', 'rb') as f:
        doc = f.read()

    start = time.time()
    for i in range(0, 1000):
        parse(doc)

    print time.time() - start





