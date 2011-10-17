import argparse
import threading
from cmd import Cmd
from gevent_zeromq import zmq
from node import Node

class ZHTCmd(Cmd):
    def __init__(self, ctx):
        Cmd.__init__(self)
        self._controlSock = ctx.socket(zmq.PUSH)
        self._controlSock.connect('inproc://zhtnode-control')

    def do_EOF(self, line):
        self._controlSock.send_multipart(['EOF'])
        return True

    def do_connect(self, line):
        self._controlSock.send_multipart(['CONNECT'] + line.split())

if __name__ == "__main__":
    parser = argparse.ArgumentParser("DHT Node")
    parser.add_argument("--bindAddrREP", "-r")
    parser.add_argument("--bindAddrPUB", "-p")
    parser.add_argument("--connectAddr", "-c", default="", required=False)
    parser.add_argument("--identity", "-i", default="", required=False)
    parser.add_argument("--message", "-m", default="TEST", required=False)
    args = parser.parse_args()

    ctx = zmq.Context.instance()

    n = Node(args.identity, args.bindAddrREP, args.bindAddrPUB, ctx)
    n.start()
    if args.connectAddr != "":
        n.spawn(n.connect, args.connectAddr)

    t = threading.Thread(target=ZHTCmd(ctx).cmdloop)
    t.start()
    n._greenletPool.join()
    t.join()

