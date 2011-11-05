"""
Node implementation. A node is the fundamental unit of the collective ZHT system.
"""
from gevent_zeromq import zmq
from gevent.pool import Pool
from table import Table
from peer import Peer
import json

class Node(object):
    """
    Construct a new :class:`Node`.

    :param identity: The identity string of this Node.
    :param repAddr: The ZMQ address to bind the REP socket to.
    :param pubAddr: The ZMQ address to bind the PUB socket to.
    :param ctx: The ZMQ Context object to operate from.
    :param poolSize: The size of the greenlet pool this Node will operate from.

    """
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
        self._sub.setsockopt(zmq.SUBSCRIBE, "")
        self._req = self._ctx.socket(zmq.XREQ)
        self._peers = dict()
        self._table = Table()
        self._controlSock = self._ctx.socket(zmq.REP)
        self._controlSock.bind('ipc://.zhtnode-control-' + identity)

    def spawn(self, f, *args, **kwargs):
        """
        Spawn a new greenlet in this Node's pool

        :param f: A callable that will be called from the new greenlet.
        :param args: Argument list to call f with.
        :param kwargs: Keyword arguments to call f with.

        """
        return self._greenletPool.spawn(f, *args, **kwargs)

    def _subConnect(self, addr):
        """
        Connect this Node's SUB socket to an address.

        :param addr: The ZMQ address of the PUB socket to connect to.

        """
        self._sub.connect(addr)

    def _reqConnect(self, addr):
        """
        Return a new REQ socket connected to the given address.

        :param addr" The ZMQ address of the REP socket to connect to.

        """
        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.IDENTITY, "%s:REQ:%s" % (self._id, addr))
        sock.connect(addr)
        return sock

    def start(self):
        """
        Start Node operations.

        This method spawns off new greenlets to handle various control messages.
        """
        self.spawn(self._handleRep)
        self.spawn(self._handleSub)
        self.spawn(self._handleControl)

    def connect(self, addr):
        """
        Connect to a Peer.

        :param addr: The ZMQ address of the Peer's REP socket.

        This will create a new peer and add it to this Node's peer table. A synchronize operation will
        happen in a spawned greenlet.
        """
        requestSock = self._reqConnect(addr)
        requestSock.send_multipart(["PEER", self._id, self._repAddr, self._pubAddr])
        reply = requestSock.recv_multipart()
        self._peers[reply[1]] = Peer(self, reply[1], addr, reply[2], requestSock)
        self._subConnect(reply[2])

    def _handleControl(self):
        """
        Handle commands given over the control socket.
        """
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
                self._pubUpdate(m[1])
            else:
                self._controlSock.send(['ERR', 'UNKNOWN COMMAND'] + m)

    def _handleRep(self):
        """
        Handle requests recieved over the REP socket.

        Each request is handled in a spawned greenlet.
        """
        while True:
            m = self._rep.recv_multipart()
            self.spawn(self._handleRepMessage, m)

    def _handleRepMessage(self, m):
        """
        Handle an individual request recieved over the REP socket.

        :param m: The request to handle.
        """
        print m
        i = 0
        while m[i] != "":
            i += 1
        envelope = m[:i+1]
        msg = m[i+1:]
        reply = None
        if msg[0] == "PEER":
            peerInfo = (msg[1], msg[2], msg[3])
            print "Recieved PEER request: identity:%s  REP:%s  PUB:%s" % peerInfo
            reply = envelope + ["PEER", self._id, self._pubAddr]
            self._subConnect(peerInfo[2])
            self._peers[peerInfo[0]] = Peer(self, peerInfo[0], peerInfo[1], peerInfo[2], self._reqConnect(peerInfo[1]))
        elif msg[0] == "PEERS":
            print "Recieved PEERS request"
            reply = envelope
            reply.append("PEERS")
            reply.append(json.dumps(dict(((ident, peer._repAddr) for ident, peer in self._peers.items()))))
        elif msg[0] == "BUCKETS":
            print "Recieved BUCKETS request"
            reply = envelope + ["BUCKETS", json.dumps(self._table.ownedBuckets())]
        elif msg[0] == "KEYS":
            print "Recieved KEYS request for bucket '%s'" % (msg[1],)
            reply = envelope + ["KEYS", msg[1], json.dumps(self._table.getKeySet(msg[1], includeTimestamp=True))]
        elif msg[0] == "GET":
            print "Recieved GET request for key '%s'" % (msg[1],)
            try:
                entry = self._table.getValue(msg[1])
                reply = envelope + ["GET", msg[1], entry._value, repr(entry._timestamp)]
            except KeyError:
                reply = ["ERROR", "KeyError", "GET", msg[1]]
        else:
            reply = envelope + ["ECHO"] + msg
        print "REPLY: %s" % (reply,)
        self._rep.send_multipart(reply)

    def _pubUpdate(self, key):
        """
        Send an update message over the PUB socket for the given key.

        :param key: The key to give an update for.
        """
        entry = self._table.getValue(key)
        self._pub.send_multipart(["UPDATE|" + entry._hash, key, entry._value, repr(entry._timestamp)])

    def _handleSub(self):
        """
        Handle messages recieved over the SUB socket.

        Each message is handled in a spawned greenlet.
        """
        while True:
            m = self._sub.recv_multipart()
            self.spawn(self._handleSubMessage, m)

    def _handleSubMessage(self, m):
        """
        Handle an individual message recieved over the SUB socket.

        :param m: The message to handle.
        """
        print "SUB: Recieved %s" % (m,)
        if m[0][:7] == 'UPDATE|':
            print "UPDATE key:%s value:%s timestamp:%s" % (m[1], m[2], m[3])
            if self._table.putValue(m[1], m[2], float(m[3])):
                self._pubUpdate(m[1])

