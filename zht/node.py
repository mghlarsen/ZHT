from cmd import Cmd
from gevent_zeromq import zmq
from gevent.pool import Pool

import argparse

from table import Table
from peer import Peer

class Node(object):
    def __init__(self, identity, repAddr, pubAddr, ctx=None, poolSize=200):
        self._greenletPool = Pool(poolSize)
        self._id = identity
        self._repAddr = repAddr
        self._pubAddr = pubAddr
        self._ctx = ctx or zmq.Context.instance()
        self._rep = self._ctx.socket(zmq.XREP)
        self._rep.setsockopt(zmq.IDENTITY, self._id + ":REP")
        self._rep.bind(repAddr)
        self._pub = self._ctx.socket(zmq.PUB)
        self._pub.setsockopt(zmq.IDENTITY, self._id + ":PUB")
        self._pub.bind(pubAddr)
        self._sub = self._ctx.socket(zmq.SUB)
        self._req = self._ctx.socket(zmq.XREQ)
        self._peers = dict()
        self._table = Table()
        self._controlSock = self._ctx.socket(zmq.PULL)
        self._controlSock.bind('inproc://zhtnode-control')

    def spawn(self, f, *args, **kwargs):
        self._greenletPool.spawn(f, *args, **kwargs)

    def _subConnect(self, addr):
        self._sub.connect(addr)

    def _reqConnect(self, addr):
        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.IDENTITY, "%s:REQ:%s" % (self._id, addr))
        sock.connect(addr)
        return sock

    def start(self):
        self.spawn(self._handleRep)
        self.spawn(self._handleControl)

    def connect(self, addr):
        requestSock = self._reqConnect(addr)
        requestSock.send_multipart(["PEER", self._id, self._repAddr, self._pubAddr])
        reply = requestSock.recv_multipart()
        self._peers[reply[1]] = Peer(self, reply[1], addr, reply[2], requestSock)
        self._subConnect(reply[2])

    def _handleControl(self):
        while True:
            m = self._controlSock.recv_multipart()
            if m[0] == 'EOF':
                self._greenletPool.kill()
                return
            elif m[0] == 'CONNECT':
                for addr in m[1:]:
                    self.spawn(self.connect, addr)
            else:
                print m

    def _handleRep(self):
        while True:
            m = self._rep.recv_multipart()
            self.spawn(self._handleRepMessage, m)

    def _handleRepMessage(self, m):
        print m
        i = 0
        while m[i] != "":
            i += 1
        envelope = m[0:i+1]
        msg = m[i+1:]
        reply = None
        if msg[0] == "PEER":
            peerInfo = (msg[1], msg[2], msg[3])
            print "Recieved PEER request: identity:%s  REP:%s  PUB:%s" % peerInfo
            reply = envelope + ["PEER", self._id, self._pubAddr]
            self._peers[peerInfo[0]] = Peer(self, peerInfo[0], peerInfo[1], peerInfo[2], self._reqConnect(peerInfo[1]))
            self._subConnect(peerInfo[2])
        elif msg[0] == "PEERS":
            print "Recieved PEERS request"
            reply = envelope + ["PEERS", str(len(self._peers))]
            for ident in self._peers.keys():
                reply += [ident, self._peers[ident]._reqAddr]
        elif msg[0] == "PARTITIONS":
            print "Recieved PARTITIONS request"
            partitions = self._table.ownedPartitions()
            reply = envelope + ["PARTITIONS", str(len(partitions))]
            for part in partitions:
                reply.append(part)
        else:
            reply = envelope + ["ECHO"] + msg
        self._rep.send_multipart(reply)

