"""
The Table is responsible for actually storing values.
"""
from functools import total_ordering
from time import time
import collections
import hashlib

def _hex_hash(value):
    """
    Return a SHA1 hex digest.

    :param value: The value to hash.
    """
    return hashlib.sha1(value).hexdigest()

class Table(object):
    """
    Construct a new Table.

    :param prefixLength: The initial hash prefix length to use.
    """
    def __init__(self, prefixLength = 1):
        self._prefixLength = prefixLength
        self._buckets = dict()
        self._owned = set()
        for prefix in self._generatePrefixes():
            self._buckets[prefix] = Bucket(prefix, True)
            self._owned.add(prefix)
    
    def _generatePrefixes(self, prefixLength = None):
        """
        Return a generator that will return all possible hash prefixes of a given length.

        :param prefixLength: The prefix length to generate. If `None`, defaults to the Table's current prefix length.
        """
        return ("%0*x" %(prefixLength or self._prefixLength, i) for i in range(2 ** (4 * (prefixLength or self._prefixLength))))

    def _getKeyHashPrefix(self, key, prefixLength = None):
        """
        Return the hash prefix for the given key.

        :param key: The key to hash.
        :param prefixLength: The prefix length to generate. If `None`, defaults to the Table's current prefix length.
        :return: The hex digest of the key, truncated to `prefixLength` digits.
        """
        return _hex_hash(key)[:prefixLength or self._prefixLength]

    def _getKeyBucket(self, key):
        """
        Get the bucket that the given key would be stored in.

        :param key: The key to search for.
        :return: The :class:`Bucket` that the key would be stored in.
        """
        return self._buckets[self._getKeyHashPrefix(key)]

    def __getitem__(self, key):
        """
        Get the value stored for the given key.

        :param key: The key to search for.
        :return: The value stored for `key`
        :raise: :class:`NotImplemented` if this key's bucket isn't owned by the table.
        :raise: :class:`KeyError` if this key's bucket is owned by the table, but the key hasn't had a value stored.
        """
        return self._getKeyBucket(key)[key]

    def __setitem__(self, key, value):
        """
        Set the value stored for the given key. Uses the current time as the timestamp.

        :param key: The key to store under.
        :param value: The value to store for `key`
        :raise: :class:`NotImplemented` if this key's bucket isn't owned by the table.
        """
        self._getKeyBucket(key)[key] = value

    def putValue(self, key, value, timestamp):
        """
        Store the given value under the given key, with timestamp.

        :param key: The key to store under.
        :param value: The value to store.
        :param timestamp: The time associated with this store. If a store with a later timestamp has already
            occurred, this store will be ignored.
        """
        return self._getKeyBucket(key).putValue(key, value, timestamp)

    def getValue(self, key):
        """
        Get the value stored for the given key.

        :param key: The key to search for.
        :return: The value stored for `key`
        :raise: :class:`NotImplemented` if this key's bucket isn't owned by the table.
        :raise: :class:`KeyError` if this key's bucket is owned by the table, but the key hasn't had a value stored.
        """
        return self._getKeyBucket(key).getValue(key)

    def getKeySet(self, prefix, includeTimestamp):
        """
        Get the set of keys for a given prefix.

        **CAVEAT**: This method's implementation is incomplete. If too short of a prefix is given, it will return
        nothing. includeTimestamp is also currently ignored.

        :param prefix: The prefix to look under. Currently, this will be truncated to the current prefix length.
        :param includeTimestamp: **Currently Unused** Return only keys modified after this timestamp.
        :return: a :class:`dict` containing all keys stored under the given prefix, with their timestamps.
        """
        if len(prefix) > self._prefixLength:
            prefix = prefix[:self._prefixLength]
        if prefix in self._buckets:
            return dict((key, entry._timestamp) for key, entry in self._buckets[prefix]._entries.items())
        else:
            return dict()

    def ownedBuckets(self):
        """
        Get the list of buckets that this Table actually owns (in no particular order).
        :return: a :class:`list` of the buckets owned by this Table.
        """
        return list(self._owned)

class Bucket(object):
    """
    Construct a new Bucket instance.

    :param prefix: The prefix of this Bucket.
    :param owned: `True` if this Bucket is actually owned by this table, `False` otherwise.
    """
    def __init__(self, prefix, owned):
        self._prefix = prefix
        self._owned = owned
        self._entries = dict()

    def __getitem__(self, key):
        """
        Get the value stored under the given key.

        :param key: The key to look under.
        :return: The value stored under `key`.
        """
        return self.getValue(key)._value

    def __setitem__(self, key, value):
        """
        Set the value stored under the given key.

        Stores with the current time as the timestamp.

        :param key: The key to store under.
        :param value: The value to store.
        """
        self.putValue(key, value, time())

    def getValue(self, key):
        """
        Return the :class:`TableEntry` stored under the given key.

        :param key: The key to look under.
        :return: The :class:`TableEntry` stored under `key`.
        """
        if self._owned or key in self._entries:
            return self._entries[key]
        raise NotImplemented("Uncached Lookup not implemented.")

    def putValue(self, key, value, timestamp):
        """
        Set the value stored under the given key.

        Stores with the given timestamp, unless the current timestamp for that entry is later.
        :param key: The key to store under.
        :param value: The value to store.
        :param timestamp: The time of this store.
        """
        if self._owned:
            if key in self._entries:
                return self._entries[key].putValue(value, timestamp)
            else:
                self._entries[key] = TableEntry(key, value, timestamp)
                return True
        else:
            raise NotImplemented("Unowned put not implemented.")
    
    def split(self):
        """
        Return a set of new Buckets with a prefix that is 1 digit longer, containing all entries in this Bucket.
        """
        newBuckets = dict()
        for newPrefix in self._generateSplitPrefixes():
            newBuckets[newPrefix] = Bucket(newPrefix, self._owned)
        for key in self._entries.keys():
            entry = self._entries[key]
            newBuckets[entry._hash[:len(self._prefix)+1]]._putValue(entry._key, entry._value, entry._timestamp)
        return newBuckets

    def _generateSplitPrefixes(self):
        """
        Generate all prefixes that are 1 digit longer than this Bucket's prefix, that start with this Bucket's prefix.
        """
        return (self._prefix + "%1x" % (i,) for i in range(16))

@total_ordering
class TableEntry(object):
    """
    Construct a new TableEntry object.

    :param key: The key for this TableEntry.
    :param value: The initial value for this TableEntry.
    :param timestamp: The initial timestamp for this TableEntry.
    """
    def __init__(self, key, value=None, timestamp=None):
        self._key = key
        self._hash = _hex_hash(key)
        self._value = value
        self._timestamp = timestamp

    def __eq__(self, other):
        """
        :return: `True` if this TableEntry's key is equal to the other object's key.
        """
        return self._key == other._key

    def __lt__(self, other):
        """
        :return: `True` if this TableEntry's key is "less than" the other object's key.
        """
        return self._key < other._key

    def __hash__(self):
        """
        :return: The hash of this TableEntry's key.
        """
        return hash(self._key)

    def putValue(self, value, timestamp):
        """
        Update this TableEntry's value.

        :param value: The value to store.
        :param timestamp: The timestamp associated with this store. If the timestamp is before the current timestamp
            this method does nothing.
        :return: `True` if the update was accepted, `False` otherwise.
        """
        if self._timestamp < timestamp or self._timestamp is None:
            print "New value:%s" % (value,)
            self._value = value
            self._timestamp = timestamp
            return True
        else:
            print "Ignored write, self:%s passed:%s" % (self._timestamp, timestamp)
            return False

