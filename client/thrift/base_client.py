#!/usr/bin/python
# -*- encoding:utf8 -*-

from collections import deque
import threading
from thrift.transport.TSocket import TSocket
from thrift.transport.TTransport import TFramedTransport
from thrift.protocol.TBinaryProtocol import TBinaryProtocol
from ipin.rpc.common.ttypes import NamedError


class BaseClient(object):
    def __init__(self, host_list, client_clz, max_conn=100, time_out=10 * 1000):
        self._host_index = 0
        self._total_conn = 0
        self._host_list = host_list
        self._max_conn = max_conn
        self._client_clz = client_clz
        self._time_out = time_out
        self._pool = deque()
        self._lock = threading.Lock()

    def _get_conn(self):
        if len(self._pool):
            return self._pool.popleft()
        self._lock.acquire(True)
        try:
            if self._total_conn >= self._max_conn:
                return None
            conn = self._create_conn()
            self._total_conn += 1
            return conn
        except Exception:
            return None
        finally:
            self._lock.release()

    def _create_conn(self):
        self._host_index += 1
        self._host_index %= len(self._host_list)
        host = self._host_list[self._host_index]
        parts = host.split(':')
        host = parts[0]
        port = int(parts[1])

        conn = TSocket(host, port)
        conn.setTimeout(self._time_out)
        conn.open()
        return conn

    def _realease_conn(self, conn, to_close):
        if not conn:
            return
        if not to_close:
            self._pool.append(conn)
            return
        self._lock.acquire(True)
        try:
            conn.close()
            self._total_conn -= 1
        finally:
            self._lock.release()

    def call(self, name, *args):
        conn = None
        to_close = False
        try:
            conn = self._get_conn()
            trans = TFramedTransport(conn)
            protocal = TBinaryProtocol(trans)
            client = self._client_clz(protocal)
            func = getattr(client, name)
            return func(*args)
        except NameError as e:
            raise e
        except Exception as e:
            to_close = True
        finally:
            if conn:
                self._realease_conn(conn, to_close)

    def __getattr__(self, name):
        def func(*args):
            return self.call(name, *args)
        return func


