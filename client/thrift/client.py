#!/usr/bin/python
# -*- encoding:utf8 -*-

from client_factory import ClientFactory
from client_config import ClientConfig
from ConfigParser import ConfigParser


class Client(object):
    def __init__(self, fn):
        p = ConfigParser()
        p.read(fn)

        jd_measure_addrs = p.get("jd_measure", "measure_addrs")
        edu_info_addrs = p.get("edu_info", "server_addrs")
        inc_idinfo_addrs = p.get("inc_info", "server_addrs")
        jd_parse_addrs = p.get("jd_parse", "parse_addrs")
        # jd_store_addrs = p.get("jd_store", "store_addrs")

        cv_measure_addrs = p.get("cv_measure", "measure_addrs")
        bin_read_addrs = p.get('bin_read', 'server_addrs')

        if jd_measure_addrs is None:
            raise Exception("need config: jd_measure_addrs")
        if edu_info_addrs is None:
            raise Exception("need config : edu_info_addrs")
        if inc_idinfo_addrs is None:
            raise Exception("need config: inc_idinfo_addrs")
        if jd_measure_addrs is None:
            raise Exception("need config: jd_parse_addrs")
        # if jd_store_addrs is None:
        #     raise Exception("need config: jd_store_addrs")
        if cv_measure_addrs is None:
            raise Exception("need config: cv_measure_addrs")

        if not bin_read_addrs:
            raise Exception('need config: bin_read')

        config = ClientConfig()
        config.jd_measure_server_config.addr_list = jd_measure_addrs.split(",")
        config.edu_info_config.addr_list = edu_info_addrs.split(",")
        config.inc_idinfo_config.addr_list = inc_idinfo_addrs.split(",")
        config.jd_parse_server_config.addr_list = jd_parse_addrs.split(",")
        # config.jd_store_server_config.addr_list = jd_store_addrs.split(",")
        config.cv_measure_server_config.addr_list = cv_measure_addrs.split(",")
        config.bin_read_server_config.addr_list = bin_read_addrs.split(",")

        factory = ClientFactory(config)
        self._jd_measure_server_client = factory.get_jd_measure_server_client()
        self._edu_info_client = factory.get_edu_info_client()
        self._inc_idinfo_client = factory.get_inc_idinfo_gen()
        self._jd_parse_server_client = factory.get_jd_parse_server_client()
        # self._jd_store_server_client = factory.get_jd_store_server_client()
        self._cv_measure_server_client = factory.get_cv_measure_server_client()
        self._bin_read_server_client = factory.get_bin_read_server_client()



    @property
    def bin_read_server_client(self):
        return self._bin_read_server_client

    @property
    def jd_measure_server_client(self):
        return self._jd_measure_server_client

    @property
    def edu_info_client(self):
        return self._edu_info_client

    @property
    def inc_idinfo_client(self):
        return self._inc_idinfo_client

    @property
    def jd_parse_server_client(self):
        return self._jd_parse_server_client

    # @property
    # def jd_store_server_client(self):
    #     return self._jd_store_server_client

    @property
    def cv_measure_server_client(self):
        return self._cv_measure_server_client

