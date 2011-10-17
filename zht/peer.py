

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
            peerList = reply[2:]
            for i in range(len(peerList) / 2):
                print "Peer %s: %d - ID:%s repAddr:%s" % (self._id, i, peerList[(i * 2)], peerList[(i * 2) + 1])
        reply = self._makeRequest(["PARTITIONS"])
        print "Reply: %s" % (reply,)
        self.__initialized = True
    
    def _makeRequest(self, req):
        self._sock.send_multipart(req)
        return self._sock.recv_multipart()
