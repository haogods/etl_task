#!/usr/bin/env python
# -*- coding:utf8 -*-

import numpy as np
import cv2

# print np.empty([2,3],dtype=int)
# print np.eye(2,3,dtype=int, k=0)
# print np.identity(3, dtype=int)
#
# print np.ones((5,))
# print np.ones_like([[1,2,3],[4,5,6]])

# a = np.arange(15).reshape(3,5)
# print a
# print a.shape
# print a.ndim
# print a.dtype.name
# print a.itemsize
# print a.size
#
# print type(a)
# print np.arange(0,2,0.2)

# x =  np.linspace(0,2*np.pi,100)
# print np.sin(x)

a = np.ones((2,3), dtype=int)
b = np.random.random((2,3))
a *= 3
print a

print b

b += a
print b

