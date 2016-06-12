# coding=utf-8

import hashlib
from simple_hash import SimpleSimHash
from cut_words import cut_with_stop_words


def check(jd_measure):
    if "jdPosition" not in jd_measure or \
            "jdWorkLoc" not in jd_measure or \
            "incName" not in jd_measure:

        raise Exception("need jdPosition, jdWorkLoc, incName")


def get_jd_measure_hash(jd_measure):
    """

    1、使用JD量化条件生成hash字符串的接口。

    :param jd_measure:
    :return:
    """
    check(jd_measure)

    pos = jd_measure["jdPosition"]
    workLoc = jd_measure["jdWorkLoc"]
    incName = jd_measure["incName"]

    if not pos or not workLoc or not incName:
        return ''

    if isinstance(pos, int):
        pos = str(pos)
    if isinstance(workLoc, int):
        workLoc = str(workLoc)
    if isinstance(incName, int):
        incName = str(incName)

    key = pos + "_" + workLoc + "_" + incName
    hs = hashlib.md5()
    hs.update(key.encode('utf-8'))
    return hs.hexdigest()


def gen_sim_hash(jdDesc):
    """
    使用jd文本内容生成 simhash 字符串的接口。
    :param jd:
    :return:
    """

    tokens = cut_with_stop_words(jdDesc)
    s = SimpleSimHash(tokens)
    return str(s.hash)


def get_hamming_distance(hash_one, hash_two):
    """
    hash字符串距离计算函数接口

    :param hash_one:
    :param hash_two:
    :return:
    """

    x = (hash_one ^ hash_two) & ((1 << 128) - 1)
    total = 0
    while x:
        total += 1
        x &= x - 1
    return total

if __name__ == '__main__':
    jd_measure = {"jdPosition":2, "jdWorkLoc":3, "incName":4}
    jd1 = dict({"jobDesc":"今天天气不错"})
    jd2 = dict({"jobDesc":"今天天气不错草"})
    print get_jd_measure_hash(jd_measure)
    hash_one = gen_sim_hash(jd1.get("jobDesc"))
    hash_two = gen_sim_hash(jd2.get("jobDesc"))

    print get_hamming_distance(int(hash_one), int(hash_two))
