#!/usr/bin/env python
# -*- coding:utf8 -*-

class MyF(object):
    def __init__(self, fname):
        self._file = open(fname, 'wb')

    def __del__(self):
        self.close()

    # 授权给file对象
    def __getattr__(self, item):
        return getattr(self._file, item)

    def write(self, line):
        self._file.write(line.upper())

class HideX(object):
    def __init__(self, x):
        self.x = x

    @property
    def x(self):
        def fget(self):
            return self.__x

        def fset(self,x):
            assert isinstance(x, int), 'x must be an interger'
            self.__x = x

        print locals()
        return locals()



if __name__ == '__main__':
    # c = MyF('test.txt')
    # c.write('today is good day')
    h = HideX(4)
    print h.x
    h.x = 10
    print h.x




# __getattr__ 仅当属性不能再实例的__dict__中，它所在的类，祖先类中找到时，才被调用
# __getattribute__() 当属性被访问时，它一直被调用，而不局限于不能找到的情况，

# 非明确从__getattribute__() 调用,或__getattribute__()引发了 AttributeError 异常,否则__getattr__不会被调用