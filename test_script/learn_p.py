#!/usr/bin/env python
# -*- coding:utf8 -*-

import subprocess
import commands
import os
import popen2
import sys

# exec 和 eval 区别

## eval 用来计算 字符串中有效的python表达式
## exec 用来执行存储在字符串或文件中的python语句

exec('print("test")')
s = eval('1+2')
print s


# input 和 raw_input

# a = input("your put :")
# b = raw_input("1234")
#
# print "input ==> ", type(a)
# print "raw_inout ==> ", type(b)


# subprocess => call
# subprocess.call(['ls','-a'])

# commands getoutput
s = commands.getoutput('ls -a')
print type(s)


# os.popen

s = os.popen('cat test.txt')
for line in s:
    print line

s.close()


# sys.exitfunc()

prev_exit_func = getattr(sys, 'exitfunc', None)
def my_exit_func(old_exit=prev_exit_func):
    if old_exit and callable(old_exit):
        old_exit()

    print "python is exiting....."

sys.exitfunc = my_exit_func
