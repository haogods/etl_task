# coding=utf-8

import re

def content_format(r):
    r = re.sub('<.*?>', "\n", r)   #去掉tag
    r = re.sub(r'[ ]+', " ", r)   #去掉多个空格
    r = re.sub(r'\?xml:namespace|&#13;', '', r)
    r = re.sub(r"[\n\t\r]+ *", '\n', r)  #多个换行 替换为2个
    r = re.sub(r'^[\t\n\r ]+|[\t\n\r ]+$', '', r)
    return r
