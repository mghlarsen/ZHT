import zmq
from zht.shell import ZHTControl
from zht.node import Node
from unittest import TestCase

def initNode(identity, connectAddr):
    node = Node(identity, 'ipc://testSock%sREP' % identity, 'ipc://testSock%sPUB' % identity, "")
    node.start()
    if connectAddr != "" and not connectAddr is None:
        node.spawn(n.connect, connect)
    control = ZHTControl(zmq.Context.instance(), identity)
    return (node, control)

def closeNode(node, control):
    control.EOF()
    node._greenletPool.join()

class TestZHT(TestCase):
    def setUp(self):
        self.aNode, self.aControl = initNode('a', None)
        self.bNode, self.bControl = initNode('b', None)

    def tearDown(self):
        self.aNode = self.aControl = self.bNode = self.bControl = None

    def testGet(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])


