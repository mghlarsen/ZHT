"""
Peers are the outside entities that each Node communicates with.
"""
import json
import logging
log = logging.getLogger('zht.peer')

class Peer(object):
    """
    Construct a new Peer instance.

    The synchronization process will also happen, but will take place in a spawned greenlet.

    :param node: The local Node that owns this Peer object.
    :param identity: The identity string of the remote Peer.
    :param repAddr: The ZMQ address of the remote Peer's REP socket.
    :param pubAddr: The ZMQ address of the remote Peer's PUB socket.
    :param sock: A ZMQ REQ socket connected to the remote Peer's REP socket.
     
    """
    def __init__(self, node, identity, repAddr, pubAddr, sock):
        self._node = node
        self._id = identity
        self._repAddr = repAddr
        self._pubAddr = pubAddr
        self._sock = sock
        self._partitions = set()
        self.__initialized = False
        self._node.spawn(self._initState)

    def _initState(self):
        """
        Initialize the internal state of this Peer object.

        Any Bucket synchronization that needs to happen will occur during this initialization process. 
        """
        reply = self._makeRequest(["PEERS"])
        if reply[0] == "PEERS":
            peerDict = json.loads(reply[1])
            for id, addr in peerDict.items():
                log.debug("Peer %s: ID:%s repAddr:%s", self._id, id, addr)
        reply = self._makeRequest(["BUCKETS"])
        self._ownedBuckets = set(json.loads(reply[1]))
        for prefix in self._ownedBuckets:
            if prefix in self._node._table.ownedBuckets():
                keysReply = self._makeRequest(["KEYS", str(prefix)])
                log.debug(str(keysReply))
                keysDict = json.loads(keysReply[2])
                for key, timestamp in keysDict.items():
                    try:
                        entry = self._node._table.getValue(str(key))
                        if entry._timestamp < float(timestamp):
                            getReply = self._makeRequest(["GET", str(key)])
                            if entry.putValue(msg[2], float(msg[3])):
                                self._node._pubUpdate(str(key))
                    except KeyError:
                        getReply = self._makeRequest(["GET", str(key)])
                        if self._node._table.putValue(str(key), getReply[2], float(getReply[3])):
                            self._node._pubUpdate(str(key))
        log.info("Peer %s initialized", self._id)
        self.__initialized = True
    
    def _makeRequest(self, req):
        """
        Make a request to this Peer.

        :param req: The request to send.
        :return: The response to the request.
        """
        self._sock.send_multipart(req)
        return self._sock.recv_multipart()

