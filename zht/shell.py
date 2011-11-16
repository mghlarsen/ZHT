# 
# Copyright 2011 Michael Larsen <mike.gh.larsen@gmail.com>
#
"""
Implement a Command-Line interface to ZHT.

:class:`ZHTControl` provides a programmatic interface for controlling a ZHT node by control socket.

:class:`ZHTCmd` implements a basic command shell interface for controlling a ZHT node.
"""
from config import ZHTConfig
import logging
from cmd import Cmd

class ZHTControl(object):
    """
    Construct a new ZHTControl.

    :param ctx: The ZMQ context to communicate over.
    :param identity: The identity string of the node to control.
    """
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
        """
        Send a shutdown command to the :class:`Node`.
        """
        self.__send(['EOF'])
    
    def connect(self, addrs):
        """
        Send a connect command to the :class:`Node`.
        """
        return self.__req(['CONNECT'] + addrs)

    def get(self, keys):
        """
        Send a get command to the :class:`Node`.
        """
        return self.__req(['GET'] + keys)

    def put(self, key, value):
        """
        Send a put command to the :class:`Node`
        """
        return self.__req(['PUT', key, value])
    
    def peers(self):
        """
        Send a peer command to the :class:`Node`
        """
        return self.__req(['PEERS'])

class ZHTCmd(Cmd):
    """
    Construct a new ZHT Command Shell.

    :param ctx: The ZMQ context to communicate with.
    :param identity: The identity string of the :class:`Node`
    """
    def __init__(self, ctx, identity):
        self._control = ZHTControl(ctx, identity)
        self.identity = identity
        Cmd.__init__(self)
        self._setPrompt()

    def _setPrompt(self):
        """
        Set the prompt string.
        """
        self.prompt = '[ZHT:%(identity)s] ' % self.__dict__

    def do_EOF(self, line):
        """
        Handle a command line EOF.

        :param line: Not actually sure what this would be...
        """
        self._control.EOF()
        print ""
        return True

    def do_connect(self, line):
        """
        Handle a command line connect.

        :param line: The command arguments.
        """
        print self._control.connect(line.split())

    def do_get(self, line):
        """
        Handle a command line get.

        :param line: The command arguments.
        """
        print self._control.get(line.split())

    def do_put(self, line):
        """
        Handle a command line put.

        :param line: The command arguments.
        """
        print self._control.put(*line.split(None, 1))

    def do_peers(self, line):
        """
        Handle a command line peers.

        :param line: The command arguments.
        """
        print self._control.peers()

    def emptyline(self):
        """
        Handle an empty line.
        """
        pass

def runNode(identity, bindAddrREP, bindAddrPUB, connectAddr):
    """
    Start a ZHT Node.

    :param identity: The identity string to use for this node.
    :param bindAddrREP: The address to bind the :class:`Node`'s REP socket to.
    :param bindAddrPUB: The address to binf the :class:`Node`'s PUB socket to.
    :param connectAddr: The address of a :class:`Node` to connect to.
    """
    from node import Node
    n = Node(identity, bindAddrREP, bindAddrPUB)
    n.start()
    if connectAddr != "" and not connectAddr is None:
        n.spawn(n.connect, connectAddr)
    n._greenletPool.join()

if __name__ == "__main__":
    config = ZHTConfig()
    import logging.config
    try:
        logging.config.fileConfig(config.loggingConfig)
    except Exception as e:
        import warnings
        warnings.warn("logging config file %s doesn't exist: %s" % (config.loggingConfig, e))
    log = logging.getLogger('zht.shell')
    
    log.info("ID: %(identity)s, REP: %(bindAddrREP)s, PUB: %(bindAddrPUB)s, CONN: %(connectAddr)s" % config)
    
    from multiprocessing import Process
    p = Process(target=runNode, args=(config.identity, config.bindAddrREP, config.bindAddrPUB, config.connectAddr))
    p.start()
    
    import zmq
    ZHTCmd(zmq.Context.instance(), config.identity).cmdloop()
    p.join()

