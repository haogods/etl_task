#!/usr/bin/python
# coding=utf8

import hashlib


class SimpleSimHash(object):
    """
    1. 使用jd文本内容生成 simhash 字符串的接口:

        s = SimpleSimHash(tokens)
        s.hash : simhash字符串

    2. hash字符串距离计算函数接口:

        s1 = SimpleSimHash(tokens1)
        s2 = SimpleSimHash(tokens2)
        s1.hamming_distance(s2)

    """

    def __init__(self, tokens=[], hash_bits=128, hash_func=None):

        self.hash_bits = hash_bits
        self.tokens = tokens
        if hash_func is None:
            def _hash_func(x):
                return int(hashlib.md5(x).hexdigest(), 16)
            self.hash_func = _hash_func

        else:
            self.hash_func = hash_func

        self.hash = self.build_from_tokens(tokens)

    def build_from_tokens(self, tokens):

        v = [0]*self.hash_bits
        for t in [self.hash_func(token.encode('utf-8')) for token in tokens]:

            for i in range(self.hash_bits):
                bit_mask = 1 << i
                if t & bit_mask:
                    v[i] += 1
                else:
                    v[i] -= 1

        fingerprint = 0
        for i in range(self.hash_bits):
            if v[i] >= 0:
                fingerprint += 1 << i

        return fingerprint

    def hamming_distance(self, other):
        x = (self.hash ^ other.hash) & ((1 << self.hash_bits) - 1)
        total = 0
        while x:
            total += 1
            x &= x - 1
        return total

