#!/usr/bin/python
# -*- encoding:utf8 -*-

from base_client import BaseClient
import threading


class ClientFactory(object):

    def __init__(self, client_config):
        self._config = client_config
        self._client_map = {}
        self._lock = threading.Lock()

    def _get_client(self, config_item, clazz):
        name = config_item.getName()
        try:
            self._lock.acquire()
            client = self._client_map.get(name)
            if client is not None:
                return client
            client = BaseClient(config_item.addr_list, clazz, config_item.pool_size, config_item.time_out)
            self._client_map[name] = client
            return client
        finally:
            self._lock.release()

    def get_jd_measure_server_client(self):
        from ipin.rpc.etl.jd.measure import JdMeasureService
        return self._get_client(self._config.jd_measure_server_config, JdMeasureService.Client)

    def get_edu_info_client(self):
        from ipin.rpc.edu.edu_info import EduIdInfoService
        return self._get_client(self._config.edu_info_config, EduIdInfoService.Client)

    def get_inc_idinfo_gen(self):
        from ipin.rpc.job.incinfo import IncInfoService
        return self._get_client(self._config.inc_idinfo_config, IncInfoService.Client)

    def get_jd_parse_server_client(self):
        from ipin.rpc.etl.jd.analyze import JdAnalyzeService
        return self._get_client(self._config.inc_idinfo_config, JdAnalyzeService.Client)

    def get_jd_store_server_client(self):
        from ipin.rpc.etl.jd.store import JdStoreService
        return self._get_client(self._config.jd_store_server_config, JdStoreService.Client)

    def get_cv_measure_server_client(self):
        from ipin.rpc.etl.cv.measure import CvMeasureService
        return self._get_client(self._config.cv_measure_server_config, CvMeasureService.Client)

    def get_bin_read_server_client(self):
        from ipin.rpc.binf import BinfService
        return self._get_client(self._config.bin_read_server_config, BinfService.Client)