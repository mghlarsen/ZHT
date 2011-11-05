=================================
Protocol For REQ/XREP socket pair
=================================

Motivation
==========
The general idea of the REQ/XREP socket pair in the DHT is to provide point-to-point coordination and control information, as well as
to provide a means of doing lookups for partitions not available locally.

Notation
========
To make parsing easy, DHT uses the multipart functionality of ZeroMQ heavily. By convention, each part of a request or response will
be separated by | characters. Static fields are simply included in normal type, and fields are in italics. For example, this is the
format of the ECHO reply:

ECHO | [ *request[0]* | ... ]

Requests
========

Connection Establishment
------------------------
PEER | *node_id* | *XREP_addr* | *PUB_addr*

Notifies this node that a node with the provided information has connected to it.

Network Discovery
-----------------
PEERS

Requests a list of the nodes this node is connected to.

Partition Discovery
-------------------
PARTITIONS

Requests a list of the partitions this node keeps locally.

Replies
=======
Connection Establishment
------------------------
PEER | *node_id* | *PUB_addr*

Give the node identity and PUB address of this node to the newly-connected node.

Network Discovery
-----------------
PEERS | *peer_count* [ | *peer_id* | *peer_XREP_addr* | ... ]

Return a list of known peers, with their identity and XREP addresses.

Partition Discovery
------------------- 
PARTITIONS | *partition_count* [ | *partition_prefix* | ... ]

Return a list of the partitions this node keeps locally.

