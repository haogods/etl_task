#!/usr/bin/env python
# -*- coding:utf8 -*-


output = '<int %r id=%#0x valv=%d>'

w = x = y = z = 1

print "global w: %#0x" % id(w)
print "global x: %#0x" % id(x)
print "global y: %#0x" % id(y)
print "global z: %#0x" % id(z)

def f1():
    x = y = z = 2
    def f2():
        y = z = 3
        def f3():
            z = 4

            print output % ('w', id(w), w)
            print output % ('x', id(x), x)
            print output % ('y', id(y), y)
            print output % ('z', id(z), z)

            clo = f3.func_closure
            if clo:
                print "f3 closure: ", [str(c) for c in clo]
            else:
                print "f3 no closure"

        f3()

        clo = f2.func_closure
        if clo:
            print "f2 closure: ", [str(c) for c in clo]
        else:
            print "f2 no closure"
    f2()

    clo = f1.func_closure
    if clo:
        print "f1 closure: ", [str[c] for c in clo]
    else:
        print "f1 no closure"




f1()

