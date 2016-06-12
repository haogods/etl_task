#!/usr/bin/env python
# -*- coding:utf8 -*-


from distutils.core import setup, Extension

MOD = "Extend1"

setup(name=MOD, ext_modules=[Extension(MOD, sources=['extend1.c'])])

