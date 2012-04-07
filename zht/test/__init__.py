# 
# Copyright 2011 Michael Larsen <mike.gh.larsen@gmail.com>
#
import zmq
import gevent
import os
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
    os.remove('testSock%sREP' % node._id)
    os.remove('testSock%sPUB' % node._id)

def clearWaitingGreenlets(n=10):
    """
    Yield enough to clear any greenlets waiting on I/O, etc.
    If there's a test failure, it may be necessary to up n for that attempt.
    """
    for i in range(2**n):
        gevent.sleep(0)

class Test2NodeZHT(TestCase):
    def setUp(self):
        self.aNode, self.aControl = initNode('a', None)
        self.bNode, self.bControl = initNode('b', None)

    def tearDown(self):
        closeNode(self.aNode, self.aControl)
        closeNode(self.bNode, self.bControl)
        self.aNode = self.aControl = self.bNode = self.bControl = None

    def testGet(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])

    def testPut(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])

    def testSync(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.connect(['ipc://testSockaREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.put('zxcv', 'poiu'), ['OK', 'zxcv', 'poiu'])
        self.assertEqual(self.aControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.bControl.get(['zxcv']), ['poiu'])

    def testRGet(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.connect(['ipc://testSockaREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.put('zxcv', 'poiu'), ['OK', 'zxcv', 'poiu'])
        self.assertEqual(self.aControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.bControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.aControl.rget(['asdf', 'zxcv']), ['qwer', 'poiu'])
        self.assertEqual(self.bControl.rget(['asdf', 'zxcv']), ['qwer', 'poiu'])

class Test3NodeZHT(TestCase):
    def setUp(self):
        self.aNode, self.aControl = initNode('a', None)
        self.bNode, self.bControl = initNode('b', None)
        self.cNode, self.cControl = initNode('c', None)

    def tearDown(self):
        closeNode(self.aNode, self.aControl)
        closeNode(self.bNode, self.bControl)
        closeNode(self.cNode, self.cControl)
        self.aNode = self.aControl = self.bNode = self.bControl = self.cNode = self.cControl = None

    def testGet(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.cControl.get(['asdf']), ['KeyError'])

    def testPut(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.cControl.get(['asdf']), ['KeyError'])

    def testSync(self):
        self.assertEqual(self.aControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.aControl.put('asdf', 'qwer'), ['OK', 'asdf', 'qwer'])
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.cControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.connect(['ipc://testSockaREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertEqual(self.aControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.bControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.cControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.put('zxcv', 'poiu'), ['OK', 'zxcv', 'poiu'])
        self.assertEqual(self.aControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.bControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.cControl.get(['asdf']), ['KeyError'])
        self.assertEqual(self.bControl.connect(['ipc://testSockcREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertEqual(self.aControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.bControl.get(['zxcv']), ['poiu'])
        self.assertEqual(self.cControl.get(['asdf']), ['qwer'])
        self.assertEqual(self.cControl.get(['zxcv']), ['poiu'])

    def testAutopeer(self):
        self.assertEqual(self.aControl.connect(['ipc://testSockbREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertItemsEqual(self.aControl.peers()[1:], ['b'])
        self.assertItemsEqual(self.bControl.peers()[1:], ['a'])
        self.assertItemsEqual(self.cControl.peers()[1:], [])
        self.assertEqual(self.bControl.connect(['ipc://testSockcREP']), ['OK'])
        clearWaitingGreenlets(12)
        self.assertItemsEqual(self.aControl.peers()[1:], ['b', 'c'])
        self.assertItemsEqual(self.bControl.peers()[1:], ['a', 'c'])
        self.assertItemsEqual(self.cControl.peers()[1:], ['a', 'b'])

