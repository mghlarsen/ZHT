# 
# Copyright 2011 Michael Larsen <mike.gh.larsen@gmail.com>
#
"""
Node implementation. A node is the fundamental unit of the collective ZHT system.
"""
from gevent_zeromq import zmq
from gevent.pool import Pool
from gevent import sleep
from table import Table
from peer import Peer
import json
import logging
from zht.table import hex_hash
log = logging.getLogger('zht.node')
pubLog = log.getChild('pub')
subLog = log.getChild('sub')
connLog = log.getChild('connect')
repLog = log.getChild('rep')

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
        self.__peersConnected = set()
        self._sub = self._ctx.socket(zmq.SUB)
        self._sub.setsockopt(zmq.SUBSCRIBE, "")
        self.__subConnected = set()
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
        connLog.debug("_subConnect('%s')", addr)
        self.__subConnected.add(addr)
        self._sub.connect(addr)

    def _reqConnect(self, addr):
        """
        Return a new REQ socket connected to the given address.

        :param addr" The ZMQ address of the REP socket to connect to.

        """
        sock = self._ctx.socket(zmq.REQ)
        sock.setsockopt(zmq.IDENTITY, str("%s:REQ:%s" % (self._id, addr)))
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
        self.spawn(self._heartbeat)

    def connect(self, addr):
        """
        Connect to a Peer.

        :param addr: The ZMQ address of the Peer's REP socket.

        This will create a new peer and add it to this Node's peer table. A synchronize operation will
        happen in a spawned greenlet.
        """
        connLog.debug("start connect:'%s' peersConnected:'%s'", addr, self.__peersConnected)
        if addr in self.__peersConnected:
            connLog.debug("duplicate")
            return
        else:
            self.__peersConnected.add(addr)
        requestSock = self._reqConnect(addr)
        requestSock.send_multipart(["PEER", self._id, self._repAddr, self._pubAddr])
        reply = requestSock.recv_multipart()
        if reply[1] != self._id and not reply[1] in self._peers:
            self._peers[reply[1]] = Peer(self, reply[1], addr, reply[2], requestSock)
            self._subConnect(reply[2])
            self._pubPeer(reply[1], addr)

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
                        if self._table.owns(key):
                            r.append('KeyError')
                        else:
                            r.append(self._rget(key))
                self._controlSock.send_multipart(r)
            elif m[0] == 'RGET':
                r = []
                for key in m[1:]:
                    r.append(self._rget(key))
                self._controlSock.send_multipart(r)
            elif m[0] == 'PUT':
                self._table[m[1]] = m[2]
                self._controlSock.send_multipart(['OK', m[1], m[2]])
                self._pubUpdate(m[1])
            elif m[0] == 'PEERS':
                self._controlSock.send_multipart(['PEERS'] + list(self._peers.keys()))
            else:
                self._controlSock.send(['ERR', 'UNKNOWN COMMAND'] + m)

    def _rget(self, key):
        h = hex_hash(key)
        for pName in self._peers.keys():
            peer = self._peers[pName]
            for b in peer._ownedBuckets:
                if h.startswith(b):
                    return peer._makeRequest(["GET", str(key)])[2] 

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
        log.debug(str(m))
        i = 0
        while m[i] != "":
            i += 1
        envelope = m[:i+1]
        msg = m[i+1:]
        reply = None
        if msg[0] == "PEER":
            peerInfo = (msg[1], msg[2], msg[3])
            repLog.debug("Recieved PEER request: identity:%s  REP:%s  PUB:%s", *peerInfo)
            reply = envelope + ["PEER", self._id, self._pubAddr]
            if not peerInfo[0] in self._peers.keys():
                self._subConnect(peerInfo[2])
                self._peers[peerInfo[0]] = Peer(self, peerInfo[0], peerInfo[1], peerInfo[2], self._reqConnect(peerInfo[1]))
                self._pubPeer(peerInfo[0], peerInfo[1])
        elif msg[0] == "PEERS":
            repLog.debug("Recieved PEERS request")
            reply = envelope
            reply.append("PEERS")
            reply.append(json.dumps(dict(((ident, peer._repAddr) for ident, peer in self._peers.items()))))
        elif msg[0] == "BUCKETS":
            repLog.debug("Recieved BUCKETS request")
            reply = envelope + ["BUCKETS", json.dumps(self._table.ownedBuckets())]
        elif msg[0] == "KEYS":
            repLog.debug("Recieved KEYS request for bucket '%s'" % (msg[1],))
            reply = envelope + ["KEYS", msg[1], json.dumps(self._table.getKeySet(msg[1], includeTimestamp=True))]
        elif msg[0] == "GET":
            repLog.debug("Recieved GET request for key '%s'", msg[1])
            try:
                entry = self._table.getValue(msg[1])
                reply = envelope + ["GET", msg[1], entry._value, repr(entry._timestamp)]
            except KeyError:
                reply = ["ERROR", "KeyError", "GET", msg[1]]
        else:
            reply = envelope + ["ECHO"] + msg
        repLog.debug("REPLY: %s", reply)
        self._rep.send_multipart(reply)

    def _pubUpdate(self, key):
        """
        Send an update message over the PUB socket for the given key.

        :param key: The key to give an update for.
        """
        entry = self._table.getValue(key)
        self._pub.send_multipart(["UPDATE|" + entry._hash, key, entry._value, repr(entry._timestamp)])

    def _pubPeer(self, id, addr):
        self._pub.send_multipart(["PEER", str(id), str(addr)])

    def _handleSub(self):
        """
        Handle messages recieved over the SUB socket.

        Each message is handled in a spawned greenlet.
        """
        while True:
            m = self._sub.recv_multipart()
            self.spawn(self._handleSubMessage, m)

    def _heartbeat(self):
        while True:
            self._pub.send_multipart(['HEARTBEAT', self._id])
            sleep(30)

    def _handleSubMessage(self, m):
        """
        Handle an individual message recieved over the SUB socket.

        :param m: The message to handle.
        """
        subLog.debug("SUB: Recieved %s", m)
        if m[0][:7] == 'UPDATE|':
            subLog.debug("UPDATE key:%s value:%s timestamp:%s", m[1], m[2], m[3])
            if self._table.putValue(m[1], m[2], float(m[3])):
                self._pubUpdate(m[1])
        elif m[0] == 'HEARTBEAT':
            id = m[1]
            subLog.debug("HEARTBEAT: id:'%s'", id)
        elif m[0] == 'PEER':
            id = m[1]
            addr = m[2]
            subLog.debug("PEER: id:'%s', addr:'%s'", id, addr)
            if not id in self._peers.keys() and id != self._id:
                self.connect(addr)

