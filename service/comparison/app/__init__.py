#!/usr/bin/env python
# -*- coding:utf8 -*-

from flask import Flask

app = Flask(__name__)
app.config.from_object('app.config')
import views


