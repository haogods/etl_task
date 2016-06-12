#coding=utf-8

import re

class HtmlFind(object):
    def __init__(self, doc):
        self._doc = doc

    def findTag(self, tag, attr=None, text_pattern=None):

        if not attr and not text_pattern:
            pat = ur'<{}>(.*?)</{}>'.format(tag, tag)
        elif not attr and text_pattern:
            pat = ur'<{}[^>]*?>{}</{}>'.format(tag, text_pattern, tag)
        elif attr and not text_pattern:
            pat = ur'<{}[^>]*{}[^>]*>(.*?)</{}>'.format(tag, attr, tag)
        elif attr and text_pattern:
            pat = ur'<{}[^>]*{}[^>]*>{}</{}>'.format(tag, attr, text_pattern, tag)

        els = re.findall(pat, self._doc, re.S)
        return els

    def findElemByPattern(self, pat):
        finds = re.findall(pat, self._doc, re.S)
        return finds

    def searchElemByPattern(self, pat):
        searches = re.search(pat, self._doc, re.S)
        if not searches:
            return []
        return searches.groups()

    def findFromContent(self, pat, content, flag=None):
        if flag:
            finds = re.findall(pat, content, flag)
        else:
            finds = re.findall(pat, content)
        return finds

    @staticmethod
    def remove_tag(s, fmt=False):
        if fmt:
            r = re.sub(r'<br>|<p>|<BR>','\n', s)
            r = re.sub(r'(<[^>]*>)|&nbsp;|\?xml:namespace|&#13;','',r)
            r = re.sub(r'[\t\r ]+', ' ', r)
            r = re.sub(r'\s+\n+\s+', '\n', r)
            r = re.sub(r'^[\t\n\r ]+|[\t\n\r ]+$', '', r)
        else:
             r = re.sub(r'(<[^>]*>)|&nbsp;|\?xml:namespace|&#13;','',s)
             r = re.sub(r'^[\t\n\r ]+|[\t\n\r ]+$', '', r)
        return r





