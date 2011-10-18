import argparse
from multiprocessing import Process
from cmd import Cmd
import zmq
from node import Node

class ZHTCmd(Cmd):
    def __init__(self, ctx, identity):
        self._controlSock = ctx.socket(zmq.REQ)
        self._controlSock.connect('ipc://.zhtnode-control-' + identity)
        self.identity = identity
        Cmd.__init__(self)
        self._setPrompt()

    def _setPrompt(self):
        self.prompt = '[ZHT:%(identity)s] ' % self.__dict__

    def do_EOF(self, line):
        self._controlSock.send_multipart(['EOF'])
        print ""
        return True

    def do_connect(self, line):
        self._controlSock.send_multipart(['CONNECT'] + line.split())
        print self._controlSock.recv_multipart()

    def do_get(self, line):
        self._controlSock.send_multipart(['GET'] + line.split())
        print self._controlSock.recv_multipart()

    def do_put(self, line):
        self._controlSock.send_multipart(['PUT'] + line.split(None, 1))
        print self._controlSock.recv_multipart()

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

