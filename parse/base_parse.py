#coding=utf-8

from lxml import html
import re

class BaseParse(object):
    def __init__(self, doc):
        self._doc = html.fromstring(doc)

    def parse(self):
        raise NotImplementedError("need implement")

    def perform(self):
        return self.parse()

    def replace_pattern(self, pattern, replace, content):
        return re.sub(pattern, replace, content, re.S)