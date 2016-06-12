#!/usr/bin/env python
# encoding: utf-8

import datetime
import sys


class ColorConfig(object):
    class ForegroundColor(object):
        BLACK = 30
        RED = 31
        GREEN = 32
        YELLOW = 33
        BLUE = 34
        WINE_RED = 35
        CYAN_BLUE = 36 #青蓝
        WHITE = 37

    class BackgroundColor(object):
        BLACK = 40
        RED = 41
        GREEN = 42
        YELLOW = 43
        BLUE = 44
        WINE_RED = 45
        CYAN_BLUE = 46 #青蓝
        WHITE = 47

    class ShowStyle(object):

        DEFAULT = 0
        HIGHLITE = 1
        UNDER_LINE = 4
        FLICKER = 5 #闪烁
        INVERSE_WHITE = 7 #反白显示
        UNVISABLE = 8 #不可见


class Logger(object):
    def __init__(self, show_style=ColorConfig.ShowStyle.HIGHLITE,
            foreground_color=ColorConfig.ForegroundColor.BLUE,
            background_color=ColorConfig.BackgroundColor.WHITE):

        self._show_style = show_style
        self._foreground_color = foreground_color
        self._background_color = background_color

    @staticmethod
    def _unset():
        print "\033[0m"

    def log(self,msg):
        print "\033[%d;%d;%dm" % (self._show_style,
                self._foreground_color,
                self._background_color)

        now_time = datetime.datetime.now()
        msg ="[%s]\t%s\n" % (now_time, msg)

        sys.stdout.write(msg)
        self._unset()


    @staticmethod
    def default_log(msg):
        print "\033[%d;%d;%dm" % (ColorConfig.ShowStyle.HIGHLITE,
                ColorConfig.ForegroundColor.BLUE,
                ColorConfig.BackgroundColor.YELLOW)

        now_time = datetime.datetime.now()
        msg ="[%s]\t%s\n" % (now_time, msg)

        sys.stdout.write(msg)
        Logger._unset()

    @staticmethod
    def printProgress(current_index, total_num):

        Logger.default_log("processed %f%%" % (float(current_index*100)/total_num))

if __name__ == '__main__':
    lg = Logger(show_style=ColorConfig.ShowStyle.HIGHLITE, foreground_color=ColorConfig.ForegroundColor.RED,
                background_color=ColorConfig.BackgroundColor.BLACK)
    lg.log("hello world")
    print "444"
    lg.log("woca")
    print "====="
    Logger.default_log("mmmmmmmmmmm")