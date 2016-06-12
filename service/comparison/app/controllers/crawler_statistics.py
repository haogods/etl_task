#!/usr/bin/env python
# -*- coding:utf8 -*-

from store.page_store import PageStore
from store.cv_page_store import CVPageStore
from store.cv_raw_store import CVRawStore
from store.co_page_store import CoPageStore


def get_count(jd_or_cv, channel):

    if jd_or_cv == 'jd':
        store = PageStore(channel)
    elif jd_or_cv == 'cv':
        store = CVPageStore(channel)
    elif jd_or_cv == 'co':
        store = CoPageStore(channel)

    else:
        raise Exception('unknown jd_or_cv type: %s' % jd_or_cv)

    return store.count_all()


def get_measure_count(channel):
    store = CVRawStore(channel, stage='measure')
    return store.count_all()