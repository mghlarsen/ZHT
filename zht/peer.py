import json

class Peer(object):
    def __init__(self, node, identity, reqAddr, pubAddr, sock):
        self._node = node
        self._id = identity
        self._reqAddr = reqAddr
        self._pubAddr = pubAddr
        self._sock = sock
        self._partitions = set()
        self.__initialized = False
        self._node.spawn(self._initState)

    def _initState(self):
        reply = self._makeRequest(["PEERS"])
        if reply[0] == "PEERS":
            peerDict = json.loads(reply[1])
            for id, addr in peerDict.items():
                print "Peer %s: ID:%s repAddr:%s" % (self._id, id, addr)
        reply = self._makeRequest(["BUCKETS"])
        self._ownedBuckets = set(json.loads(reply[1]))
        for prefix in self._ownedBuckets:
            if prefix in self._node._table.ownedBuckets():
                keysReply = self._makeRequest(["KEYS", str(prefix)])
                print keysReply
                keysDict = json.loads(keysReply[2])
                for key, timestamp in keysDict.items():
                    try:
                        entry = self._node._table.getValue(str(key))
                        print "%f < %f ??" % (entry._timestamp, float(timestamp))
                        if entry._timestamp < float(timestamp):
                            print "Sending GET (TI)"
                            getReply = self._makeRequest(["GET", str(key)])
                            print "GET REPLY (TI): %s" % (getReply,)
                            entry.putValue(msg[2], float(msg[3]))
                    except KeyError:
                        print "Sending GET (KE)"
                        getReply = self._makeRequest(["GET", str(key)])
                        print "GET REPLY (KE): %s" % (getReply,)
                        self._node._table.putValue(str(key), getReply[2], float(getReply[3]))
        print "Peer %s initialized" % (self._id,)
        self.__initialized = True
    
    def _makeRequest(self, req):
        self._sock.send_multipart(req)
        return self._sock.recv_multipart()
