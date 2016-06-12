#!/usr/bin/python
# -*- encoding:utf8 -*-


class ClientConfigItem(object):
    def __init__(self, name):
        self._name = name
        self.addr_list = []
        self.pool_size = 100
        self.time_out = 10 * 1000

    def getName(self):
        return self._name


class ClientConfigField(object):
    def __init__(self, name):
        self._name = name

    def __get__(self, obj, owner):
        item = getattr(obj, self._name, None)
        if None == item:
            item = ClientConfigItem(self._name)
            setattr(obj, self._name, item)
        return item


class ClientConfig(object):

    jd_measure_server_config = ClientConfigField("jd_measure_server")
    jd_parse_server_config = ClientConfigField("jd_parse_server")
    jd_store_server_config = ClientConfigField("jd_store_server")
    edu_info_config = ClientConfigField("edu_info")
    inc_idinfo_config = ClientConfigField("inc_info")
    bin_read_server_config = ClientConfigField('bin_read')

    cv_measure_server_config = ClientConfigField("cv_measure_server")


if __name__ == "__main__":
    c = ClientConfig()
    item = c.jd_measure_server_config
    print "finish"