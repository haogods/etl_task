#!/usr/bin/env python
# -*- coding:utf8 -*-

from time import sleep,  ctime
import thread

def loop0():
    print "start loop 0 at: ", ctime()
    sleep(4)
    print "loop 0 done at: ", ctime()


def loop1():
    print "start loop 1 at: ", ctime()
    sleep(2)
    print "loop 1 done at: ", ctime()


def loop(nloop, nsec, lock):
    print "start loop", nloop, 'at: ', ctime()
    sleep(nsec)
    print "loop", nloop, "done at: ", ctime()
    lock.release()

def main():
    print "start at : ", ctime()
    loop0()
    loop1()
    print "end at : ", ctime()

def main1():
    print "start at: ", ctime()
    thread.start_new_thread(loop0, ())
    thread.start_new_thread(loop1, ())
    sleep(6)
    print "all done at :", ctime()


def main2():
    print "start at: ", ctime()
    locks = []
    loops = [4,2]
    nloops = range(len(loops))
    for i in nloops:
        lock = thread.allocate_lock()
        lock.acquire()
        locks.append(lock)

    for i in nloops:
        thread.start_new_thread(loop, (i, loops[i], locks[i]))

    for i in nloops:
        while locks[i].locked(): pass


    print "all done at :", ctime()



if __name__ == '__main__':
    main2()

