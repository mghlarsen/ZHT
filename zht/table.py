from functools import total_ordering
import collections
import hashlib

def _hex_hash(value):
    return hashlib.sha1(value).hexdigest()

class Table(object):
    def __init__(self, prefixLength = 1):
        self._prefixLength = prefixLength
        self._buckets = dict()
        self._owned = set()
        for prefix in self._generatePrefixes():
            self._buckets[prefix] = Bucket(prefix, True)
            self._owned.add(prefix)
    
    def _generatePrefixes(self, prefixLength = None):
        return ("%0*x" %(prefixLength or self._prefixLength, i) for i in range(2 ** (4 * (prefixLength or self._prefixLength))))

    def _getKeyHashPrefix(self, key, prefixLength = None):
        return _hex_hash(key)[:prefixLength or self._prefixLength]

    def _getKeyBucket(self, key):
        return self._buckets[self._getKeyHashPrefix(key)]

    def ownedPartitions(self):
        return frozenset(self._owned)

class Bucket(object):
    def __init__(self, prefix, owned):
        self._prefix = prefix
        self._owned = owned
        self._entries = dict()

    def _getValue(self, key):
        if self._owned or key in self._entries:
            return self._entries[key]
        raise NotImplemented("Uncached Lookup not implemented.")

    def _putValue(self, key, value, timestamp):
        if self._owned:
            if key in self._entries:
                self._entries[key].putValue(key, value, timestamp)
            else:
                self._entries[key] = TableEntry(key, value, timestamp)
        else:
            raise NotImplemented("Unowned put not implemented.")
    
    def split(self):
        newBuckets = dict()
        for newPrefix in self._generateSplitPrefixes():
            newBuckets[newPrefix] = Bucket(newPrefix, self._owned)
        for key in self._entries.keys():
            entry = self._entries[key]
            newBuckets[entry._hash[:len(self._prefix)+1]]._putValue(entry._key, entry._value, entry._timestamp)
        return newBuckets

    def _generateSplitPrefixes(self):
        return (self._prefix + "%1x" % (i,) for i in range(16))

@total_ordering
class TableEntry(object):
    def __init__(self, key, value=None, timestamp=None):
        self._key = key
        self._hash = _hex_hash(key)
        self._value = value
        self._timestamp = timestamp

    def __eq__(self, other):
        return self._key == other._key

    def __lt__(self, other):
        return self._key < other._key

    def __hash__(self):
        return hash(self._key)


t = Table()
prefixes = tuple(t._generatePrefixes(4))

def saveEntry(key, value, timestamp=None):
    bucket = t._getKeyBucket(key)
    bucket._putValue(key, value, timestamp)
    return bucket._prefix

def getEntry(key):
    bucket = t._getKeyBucket(key)
    return (bucket._prefix, bucket._getValue(key))

saveEntry('asdf', 'qwer')
saveEntry('as', 'qwer')
saveEntry('asd', 'qwer')
saveEntry('adf', 'qwer')
saveEntry('adasdff', 'qwer')
saveEntry('aweewdasdff', 'qwer')
saveEntry('aweewdasd', 'qwer')
saveEntry('awewdasd', 'qwer')

