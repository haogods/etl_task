#!/usr/bin/env python
# -*- coding:utf8 -*-

from tools.filesave import CsvFile
import time

def test_write(file, row):
    file.append_row(row)

class T(object):

    def __init__(self):
        exec('self.a = 1')
        self.csv_file = CsvFile('tt.csv')

    def __del__(self):
        print "ok"



if __name__ == '__main__':
    s = T()
    while 1:
        row = ['a', 'b']
        test_write(s.csv_file, row)
        time.sleep(3)

    print "111"