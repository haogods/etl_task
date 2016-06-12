#!/usr/bin/env python
# -*- coding:utf8 -*-

import re

class EncryptBase(object):

    @staticmethod
    def char_to_num(m):
        assert len(m) == 1 and isinstance(m, basestring)

        if '0' <= m <= '9':
            return ord(m) - ord('0')

        if 'A' <= m <= 'Z':
            return ord(m) - ord('A') + 10

        if 'a' <= m <= 'z':
            return ord(m) - ord('a') + 10


    @staticmethod
    def num_to_char(n):
        if 0 <= n <= 9:
            return str(n)
        if 10 <= n <= 36:
            return chr(n-10+ord('a'))

        return '?'

    @staticmethod
    def int10_to_int36(n):

        if isinstance(n, basestring):
            n = int(n)

        loop = '0123456789abcdefghijklmnopqrstuvwxyz'

        a = []
        while n != 0:
            a.append(loop[n % 36])
            n = n / 36
        a.reverse()

        return ''.join(a)


    @staticmethod
    def a_to_num(a):

        vv = 0

        for ch in a:
            if 'A' <= ch <= 'Z':
                x = ord(ch) - ord('A') + 1
            elif 'a' <= ch <= 'z':
                x = ord(ch) - ord('a') + 1
            else:
                raise Exception("invalid char %s for a_to_num" % ch)

            vv = vv * 27 + x

        return vv


    @staticmethod
    def a_from_num(num):
        _os = ''
        while 1:
            vv1, r = divmod(num, 27)
            r = int(r)
            if r == 0:
                break
            _os = ''.join([chr(r-1+ord('a')),_os])
            num = vv1


        return _os


    @staticmethod
    def a_to_num_cs(a):
        vv = 1
        for ch in a:
            if 'A' <= ch <= 'Z':
                x = ord(ch) - ord('A')
            elif 'a' <= ch <= 'z':
                x = ord(ch) - ord('a') + 26

            else:
                raise Exception('invalid char %s for a_to_num_cs' % ch)

            vv = vv * (26*2) + x

        return vv

    @staticmethod
    def a_from_num_cs(num):
        gmp1 = 1
        _os = ''

        while num > gmp1:
            vv1, r = divmod(num, 52)
            r = int(r)
            if r < 26:
                _os = ''.join([chr(r+ord('A')), _os])
            else:
                _os = ''.join([chr(r-26+ord('a')), _os])

            num = vv1

        return _os

    @staticmethod
    def pack_num(s, m):
        vv = 0
        for ch in s:
            no = EncryptBase.char_to_num(ch) + 1
            vv = vv * m + no

        return EncryptBase.int10_to_int36(vv)

    @staticmethod
    def unpack_num(s, m):
        _os = ''
        vv = int(s, 36)
        while vv > 0:
            vv1, r = divmod(vv, m)
            r = int(r)
            _os1 = '-' if r == 0 else EncryptBase.num_to_char(r-1)
            _os = ''.join([_os1, _os])
            vv = vv1

        return _os


    @staticmethod
    def pack_list(a):
        _os = ''
        for v in a:
            stmp = bin(v)[2:]
            str_len = len(stmp)
            prefix = "000000000" + bin(str_len)[2:]
            prefix = prefix[len(prefix)-8:]
            _os = ''.join([_os, prefix, stmp])

        _os = ''.join(['1', _os])

        r = EncryptBase.int10_to_int36(int(_os, 2))
        return r

    @staticmethod
    def unpack_list(a):

        rv = []

        _os = bin(int(a, 36))[2:]
        _os = _os[1:]

        while 1:
            _len = len(_os)
            if _len >= 8:
                prefix = _os[0:8]
            else:
                break

            prelen = int(prefix, 2)
            if _len >= 8+prelen:
                num = _os[8:8+prelen]
            else:
                break

            _os = _os[8+prelen:]

            rv.append(int(num, 2))

        if _os:
            print "warning : unpack %r failed." % a

        return rv


class CEncryptID(EncryptBase):


    @staticmethod
    def get_channel(f):
        if f == 'cv_51job':
            return 1
        if f == 'cv_zhilian':
            return 2
        if f == 'cv_liepin':
            return 3
        if f == 'cv_chinahr':
            return 4
        if f == 'cv_58job':
            return 5

        return None

    @staticmethod
    def encrypt(value):
        if '://' not in value:
            return  None
        n = value.index('://')
        f = value[:n]
        cid = value[n+3:]
        ch = CEncryptID.get_channel(f)

        if ch == 1:
            return CEncryptID.num_to_char(ch) + CEncryptID.pack_num(cid, 11)

        if ch == 2:
            find = re.search("!^([a-z]+)([0-9]+)([a-z]+)([0-9]+)$!i", cid)
            if find:
                pl = [CEncryptID.a_to_num(find.group(1)), int(find.group(2)), CEncryptID.a_to_num(find.group(3)), int(find.group(4))]
                return '2' + CEncryptID.pack_list(pl)

        if ch == 3:
            find = re.search("([0-9]+)([a-z]+)([0-9]+)", cid)
            if find:
                pl = [int(find.group(1)), CEncryptID.a_to_num_cs(find.group(2)), int(find.group(3))]
                return "3" + CEncryptID.pack_list(pl)

        raise Exception('failed to encrypt %s' % value)






    @staticmethod
    def decrypt(xid):
        tp = CEncryptID.char_to_num(xid[0])
        if tp == 1:
            r_p1 = CEncryptID.unpack_num(xid[1:], 11)
            return 'cv_51job://%s' % r_p1

        if tp == 2:
            lst = CEncryptID.unpack_list(xid[1:])
            cvid = CEncryptID.a_from_num(lst[0]) + str(lst[1]) + CEncryptID.a_from_num(lst[2]) + str(lst[3])

            return "cv_zhilian://%s" % cvid.upper()

        if tp == 3:
            lst = CEncryptID.unpack_list(xid[1:])
            cvid = str(lst[0]) + CEncryptID.a_from_num_cs(lst[1]) + str(lst[2])
            return "cv_liepin://%s" % cvid

        raise Exception('failed to decrypt %s' % xid)


if __name__ == '__main__':
    print CEncryptID.decrypt('25lgl9mlccyeu3542snplx4')
    # print CEncryptID.encrypt('cv_51job://26503724')