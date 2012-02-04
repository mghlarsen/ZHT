from unittest import TestCase
from zht.config import ZHTConfig

class TestConfig(TestCase):
    def setUp(self):
        pass

    def tearDown(self):
        pass

    def testFileLoading(self):
        args = ['-c', 'zht/test/zhtrc']
        c = ZHTConfig(args)
        self.assertEqual(c.identity, 'a')
        self.assertEqual(c.bindAddrREP, 'ipc://socks/aREP')
        self.assertEqual(c.bindAddrPUB, 'ipc://socks/aPUB')
    
