from gevent_zeromq import zmq
from gevent.pool import Pool
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
        self._controlSock = self._ctx.socket(zmq.REP)
        self._controlSock.bind('ipc://.zhtnode-control-' + identity)

    def spawn(self, f, *args, **kwargs):
        return self._greenletPool.spawn(f, *args, **kwargs)

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
                self._controlSock.send('OK')
                return
            elif m[0] == 'CONNECT':
                self._greenletPool.map(self.connect, m[1:])
                self._controlSock.send('OK')
            elif m[0] == 'GET':
                r = []
                for key in m[1:]:
                    try:
                        r.append(self._table[key])
                    except KeyError:
                        r.append('KeyError')
                self._controlSock.send_multipart(r)
            elif m[0] == 'PUT':
                self._table[m[1]] = m[2]
                self._controlSock.send_multipart(['OK', m[1], m[2]])
            else:
                self._controlSock.send(['ERR', 'UNKNOWN COMMAND'] + m)

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
        elif msg[0] == "BUCKETS":
            print "Recieved BUCKETS request"
            partitions = self._table.ownedBuckets()
            reply = envelope + ["BUCKETS", str(len(partitions))]
            for part in partitions:
                reply.append(part)
        else:
            reply = envelope + ["ECHO"] + msg
        self._rep.send_multipart(reply)

