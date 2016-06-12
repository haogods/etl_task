#!/usr/bin/env python
import StringIO
import gzip
import os
import struct
import threading


class FileSaver:
    def __init__(self, fn):
        self.fd = open(fn, 'a+b')
        self.lock = threading.Lock()

    def __del__(self):
        self.fd.close()

    def append(self, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(value, (float, int, long)):
            value = str(value)
        with self.lock:
            self.fd.write(value+os.linesep)
            self.fd.flush()


class BinSaver:
    @staticmethod
    def compress_item(name, value):
        if isinstance(value, unicode):
            value = value.encode('utf-8')
        if isinstance(name, unicode):
            name = name.encode('utf-8')
        fo = StringIO.StringIO()
        f = gzip.GzipFile(fileobj=fo, mode='wb')
        f.write(value)
        f.close()
        r = fo.getvalue()
        fo.close()
        return struct.pack("I", len(name)) + name + struct.pack("I", len(r)) + r

    def __init__(self, fn):
        self._fd = open(fn, 'a+b')
        self._locker = threading.Lock()
        self._fn = fn

    def __del__(self):
        self._fd.close()

    def append(self, name, value):
        a = BinSaver.compress_item(name, value)
        pos = 0
        with self._locker:
            pos = self._fd.tell()
            self._fd.write(a)
            self._fd.flush()
        return pos

    def filename(self):
        return self._fn


class BinReader:
    def __init__(self, fn, offset=0):
        self._fsz = float(os.path.getsize(fn))
        self._nread = 0
        self.fd = open(fn, 'rb')
        self.fd.seek(offset)
        self.lock = threading.Lock()
    def __del__(self):
        self.fd.close()
    def _readone_i(self):
        sz0 = self.fd.read(4)
        if len(sz0) == 0:
            return (None,None)
        if len(sz0) != 4:
            raise IOError('invalid file')
        (sz,) = struct.unpack("I", sz0)
        fn = self.fd.read(sz)
        if len(fn) != sz:
            raise IOError('invalid file')
        self._nread += sz+4

        sz0 = self.fd.read(4)
        if len(sz0) != 4:
            raise IOError('invalid file')
        (sz,) = struct.unpack("I", sz0)
        gzconn = self.fd.read(sz)
        if len(gzconn) != sz:
            raise IOError('invalid file')
        self._nread += sz+4

        fin = StringIO.StringIO(gzconn)
        with gzip.GzipFile(fileobj=fin, mode='rb') as f:
            conn = f.read()
        fin.close()
        return (fn, conn)

    def progress(self):
        if self._fsz == 0.0:
            return 1.0
        return float(self._nread) / self._fsz

    def readone(self):
        with self.lock:
            return self._readone_i()

    def readone_at(self, pos):
        with self.lock:
            self.fd.seek(pos)
            return self._readone_i()


    @staticmethod
    def getPageContent(filename):
        parts = filename.split("::")
        if len(parts) == 3:
            binReader = BinReader(parts[1])
            s, content = binReader.readone_at(int(parts[2]))
            print s
            #print "==>", content
            if len(content) == 0:
                raise Exception("file name:{} , content error".format(filename))
            return content

        if len(parts) == 1:
            with open(filename) as f:
                content = f.read()
                if len(content) == 0:
                    raise Exception("file name:{} , content error".format(filename))
                return content


if __name__ == "__main__":
    t = BinReader('/data/crawler/_files_/jd_zhilian.14296.bin')
    (a, b) = t.readone_at(997353438)
    print a, b
   # while True:
    #    (a,b) = t.readone()
     #   if a is None:
     #       break
     #   print a, b
