#!/usr/bin/env python
# -*- coding:utf8 -*-


import re

reg_1 = r'(\Bthe\b)'

find = re.search(reg_1, 'there is a dog, the god, thethe')
print find.group(1)

find = re.findall(r'(.*?foo)','foofoomanfoo')
print find

