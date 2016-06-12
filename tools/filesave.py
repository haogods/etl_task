# coding=utf-8

import threading
import json
import codecs
import csv
import sys
import os
reload(sys)
sys.setdefaultencoding('utf-8')

class FileSave(object):
    def __init__(self, fn, mod='a+b'):
        self.fn = fn
        self._fd = codecs.open(self.fn, mod, encoding='UTF-8')
        self._locker = threading.Lock()

    def __del__(self):
        self._fd.close()

    def append_end_with(self, c):

        if isinstance(c, dict):
            c = json.dumps(c, ensure_ascii=False)

        with self._locker:
            self._fd.write(c + os.linesep)
            self._fd.flush()

    @property
    def fd(self):
        return self._fd

class CsvFile(object):
    def __init__(self, fn):
        self.fn = fn
        self.csv_file = codecs.open(fn, 'wb', 'utf-8')
        self.writer = csv.writer(self.csv_file)
        self._lock = threading.RLock()

        self.is_set_header = False

    def __del__(self):
        with self._lock:
            self.csv_file.close()

    def set_header(self, header):
        if not isinstance(header, list):
            raise Exception('need list')

        with self._lock:
            self.writer.writerow(header)
            self.is_set_header = True

    def append_row(self, row):
        with self._lock:
            self.writer.writerow(row)

    def append_rows(self, rows):
        with self._lock:
            self.writer.writerow(rows)

if __name__ == '__main__':
    # csvfile = CsvFile('tt.csv')
    # csvfile.set_header(['cvId', 'cv'])
    # name = json.dumps({"name":"jainghao", "country":"中国"}, ensure_ascii=False, indent=2)
    # print name
    # csvfile.append_row(['cvId://123455', name])

    s = FileSave('test.txt')
    print >> s.fd, "ok"

