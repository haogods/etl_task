#!/usr/bin/env python
# -*- coding:utf8 -*-

import threading
import sys
import os
import ctypes
import StringIO
import re
import struct
import gzip
import time


class Runtime(object):
    namelock = threading.RLock()
    namemap = {}

    @staticmethod
    def get_thread_name(tid):
        with Runtime.namelock:
            return Runtime.namemap.get(tid, '')

    @staticmethod
    def set_thread_name(tid, name):
        with Runtime.namelock:
            Runtime.namemap[tid] = name

    @staticmethod
    def current_thread():
        return threading.current_thread().ident

    @staticmethod
    def set_current_thread_name(name):
        return Runtime.set_thread_name(Runtime.current_thread(), name)

Runtime.set_current_thread_name('main')