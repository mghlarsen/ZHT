import argparse
from multiprocessing import Process
from cmd import Cmd
import zmq
from node import Node

class ZHTControl(object):
    def __init__(self, ctx, identity):
        self._sock = ctx.socket(zmq.REQ)
        self._sock.connect('ipc://.zhtnode-control-' + identity)
        self.identity = identity
    
    def __send(self, msg):
        self._sock.send_multipart(msg)

    def __recv(self):
        return self._sock.recv_multipart()
    
    def __req(self, msg):
        self.__send(msg)
        return self.__recv()

    def EOF(self):
        self.__send(['EOF'])
    
    def connect(self, addrs):
        return self.__req(['CONNECT'] + addrs)

    def get(self, keys):
        return self.__req(['GET'] + keys)

    def put(self, key, value):
        return self.__req(['PUT', key, value])

class ZHTCmd(Cmd):
    def __init__(self, ctx, identity):
        self._control = ZHTControl(ctx, identity)
        Cmd.__init__(self)
        self._setPrompt()

    def _setPrompt(self):
        self.prompt = '[ZHT:%(identity)s] ' % self.__dict__

    def do_EOF(self, line):
        self._control.EOF()
        print ""
        return True

    def do_connect(self, line):
        print self._control.connect(line.split())

    def do_get(self, line):
        print self._control.get(line.split())

    def do_put(self, line):
        print self._control.put(*line.split(None, 1))

    def emptyline(self):
        pass

def runNode(identity, bindAddrREP, bindAddrPUB, connectAddr):
    n = Node(identity, bindAddrREP, bindAddrPUB)
    n.start()
    if connectAddr != "":
        n.spawn(n.connect, connectAddr)
    n._greenletPool.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser("DHT Node")
    parser.add_argument("--bindAddrREP", "-r")
    parser.add_argument("--bindAddrPUB", "-p")
    parser.add_argument("--connectAddr", "-c", default="", required=False)
    parser.add_argument("--identity", "-i", default="", required=False)
    parser.add_argument("--message", "-m", default="TEST", required=False)
    args = parser.parse_args()

    p = Process(target=runNode, args=(args.identity, args.bindAddrREP, args.bindAddrPUB, args.connectAddr))
    p.start()
    ZHTCmd(zmq.Context.instance(), args.identity).cmdloop()
    p.join()

