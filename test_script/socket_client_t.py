#!/usr/bin/env python
# -*- coding:utf8 -*-

from twisted.internet import protocol, reactor
import socket

HOST = '127.0.0.1'

PORT = 21567
BUFFSIZE = 1024
ADDR = (HOST, PORT)


def tcpClient():
    tcpCliSock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tcpCliSock.connect(ADDR)

    while 1:
        data = raw_input("> ")
        if not data:
            break

        tcpCliSock.send(data)
        data = tcpCliSock.recv(BUFFSIZE)
        if not data:
            break

        print data

    tcpCliSock.close()

def udpClient():
    udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    while 1:
        data = raw_input("> ")
        udpSocket.sendto(data, ADDR)
        data, addr = udpSocket.recvfrom(BUFFSIZE)
        if not data:
            break

        print data

    udpSocket.close()

class TSClntProtocol(protocol.Protocol):
    def sendData(self):
        data = raw_input("> ")
        if data:
            print "... sending %s..." % data

            self.transport.write(data)
        else:
            self.transport.loseConnection()

    def connectionMade(self):
        self.sendData()

    def dataReceived(self, data):
        print data
        self.sendData()


class TSClntFactory(protocol.ClientFactory):
    protocol = TSClntProtocol
    clientConnectionLost = clientConnectionFailed = lambda self, connector, reason: reactor.stop()

def tcpClient_two():
    reactor.connectTCP(HOST, PORT, TSClntFactory())
    reactor.run()



def api_test():
    addr = "192.168.1.1"

    bin = socket.inet_aton(addr)
    print socket.inet_ntoa(bin)

    bin1 = socket.inet_pton(socket.AF_INET, addr)
    print socket.inet_ntop(socket.AF_INET, bin1)





if __name__ == '__main__':
    # udpClient()
    # tcpClient()
    # api_test()
    tcpClient_two()

