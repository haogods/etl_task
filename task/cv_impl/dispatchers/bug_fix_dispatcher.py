#!/usr/bin/env python
# -*- coding:utf8 -*-


from task.cv_impl.dispatchers.etl_dipatcher import ETLDispatcherBase
from store.cv_raw_store import CVRawStore
from tools.log_util import Logger
import random


class BugFixDispatcher(ETLDispatcherBase):
    def __init__(self, channel, q):
        ETLDispatcherBase.__init__(self, channel, q)
        self.cv_raw_store = CVRawStore(self.channel, stage='raw')
        self.cv_measure_store = CVRawStore(self.channel, stage='measure')

    def real_dispatcher(self, from_where):

        i = 0
        total_cnt = self.cv_raw_store.count_all()

        for doc in self.cv_raw_store.get_all():
            del doc['_id']
            self.queue.put(doc)
            i += 1
            if i % 10000 == 0:
                Logger.printProgress(i, total_cnt)


class RandomDispatcher(BugFixDispatcher):
    def __init__(self, channel,q, dispatcher_count):
        BugFixDispatcher.__init__(self, channel, q)
        self.dispatcher_cnt = dispatcher_count
        self.already_dispatched_cnt = 0

        self.loaded_items = []

    def load_data(self):
        index = 0
        for doc in self.cv_raw_store.get_all():
            self.loaded_items.append(doc['cvId'])
            index += 1
            if index and index % 10000 == 0:
                print "load %d items" % index

        print "totally loaded %d items" % len(self.loaded_items)

    def real_dispatcher(self, from_where):

        to_dispatch_items = random.sample(self.loaded_items, self.dispatcher_cnt)

        for item in to_dispatch_items:
            doc = self.cv_raw_store.get_one(item)
            del doc['_id']
            self.queue.put(doc)
            self.already_dispatched_cnt += 1

            Logger.default_log("current dispatched cnt: %d, total need cnt: %d" % (self.already_dispatched_cnt, self.dispatcher_cnt))










