#!/usr/bin/env python
# -*- coding:utf8 -*-

from SocketServer import (
     TCPServer as TCP,
    StreamRequestHandler as SRH
)

from twisted.internet import protocol, reactor


import socket
from time import ctime

HOST = ''
PORT = 21567
ADDR = (HOST, PORT)
BUFFSIZE = 1024



def tcpServer():
    tcpSocket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    tcpSocket.bind(ADDR)
    tcpSocket.listen(5)

    while 1:
        print "waiting for connection..."
        tcpCliSocket, addr = tcpSocket.accept()
        print "... connected from: ", addr

        while 1:
            data = tcpCliSocket.recv(BUFFSIZE)
            if not data:
                break

            tcpCliSocket.send("[%s] %s" % (ctime(), data))
        tcpCliSocket.close()
    tcpSocket.close()

def udpServer():
    udpSocket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udpSocket.bind(ADDR)

    while 1:
        print "waiting for msg...."
        data, addr = udpSocket.recvfrom(BUFFSIZE)
        udpSocket.sendto('[%s] %s' % (ctime(), data), addr)
        print "receive from and returned to :", addr

    udpSocket.close()



class MyRequestHandler(SRH):
    def handle(self):
        print "... connected from : ", self.client_address

        self.wfile.write('[ %s ] %s' % (ctime(), self.rfile.readline()))

class TSServProtocol(protocol.Protocol):
    def connectionMade(self):
        clnt = self.clnt = self.transport.getPeer().host
        print "...connected from :", clnt

    def dataReceived(self, data):
        print "received data: %s" % data
        self.transport.write('[%s] %s' % (ctime(), data))

def tcpServer_three():
    factory = protocol.Factory()
    factory.protocol = TSServProtocol
    print "...waiting for connection"
    reactor.listenTCP(PORT, factory)
    reactor.run()

def tcpServer_two():
    tcpServ = TCP(ADDR, MyRequestHandler)
    print "waiting for connection..."
    tcpServ.serve_forever()




if __name__ == '__main__':
    # udpServer()
    # tcpServer()
    tcpServer_three()