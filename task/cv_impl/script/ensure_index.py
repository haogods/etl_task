#!/usr/bin/env python
# -*- coding:utf8 -*-


import sys
import pymongo

if __name__ == '__main__':
    if len(sys.argv) < 4:
        print "Usage ./ensure_index.py cv_channel stage key"
        exit(1)

    cv_channel = sys.argv[1]
    stage = sys.argv[2]
    key = sys.argv[3]

    if cv_channel == 'cv_zhilian':
        client = pymongo.MongoClient('mongodb://hadoop2:28017/cv_zhilian_raw')
    elif cv_channel == 'cv_liepin':
        client = pymongo.MongoClient('mongodb://hadoop2:28017/cv_liepin_raw')

    if stage == 'raw':
        client['%s_raw' % cv_channel]['cv_raw'].ensure_index(key,unique=True)

    elif stage == 'measure':
        client['%s_measure' % cv_channel]['cv_measure'].ensure_index(key,unique=True)


    print 'complete'