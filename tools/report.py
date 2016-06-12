#coding=utf-8

import socket
import sys


def usage(script_name):
    print "{} -l (to see log)".format(script_name.split('/')[-1])


def show_log(host='', port=9527):

    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        port = int(port)
    except ValueError:
        port = socket.getservbyport(port, 'udp')

    s.bind((host, port))
    # s.connect((host, port))
    while 1:
        # print "Enter data to transmit: "
        # data = sys.stdin.readline().strip()
        # s.sendall(data)
        # print "Looking for replies; press Ctrl-C or Ctrl-Break to stop."
        buf = s.recv(2048)
        if not len(buf):
            break
        print "Server replies: "
        sys.stdout.write(buf)
        print "\n"

if __name__ == "__main__":
    if len(sys.argv)>1 and sys.argv[1] == 'l':
        show_log()
    else:
        usage(sys.argv[0])