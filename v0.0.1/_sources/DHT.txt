Distributed Hash Table
======================
Basic underlying persistence/broadcast mechanism

Each node maintains a *PUB*/*SUB* socket pair, an *XREP* socket, and a *REQ* socket for each peer.

*PUB*/*SUB*: Used for communicating table updates. Each node should be subscribed to the hash prefixes of any partitions that they are
responsible for.

*REQ*/*XREP*: Used for answering queries about existing information in the table (table sync), getting information about known nodes

Storage:
 - Use hex(sha1(key)) as the way to determine table position. It's a little slow, but makes partitioning easier.
 - Partition by prefixes: Number of Buckets is always a power of 2, preferably a power of 16.

Joining DHT:
 - Some initial node list must be obtained. New node should connect to the *XREP* of each initial node with a *REQ* socket.
 - Make a PEERS request to each initial node to find additional nodes until target number of connections has been reached or no
   additional nodes to try.
 - Make a PARTITIONS request to each connected node to determine which partitions are held by each node.
 - Note the current count of partitons held by each node and the size of each partition.
 - Select the least-well covered partitions in the table, and then randomly choose others until a sufficient number have been chosen
 - Make SUB connections to known nodes, set subscriptions for the partitions selected, as well as necessary control subscriptions
 - For each selected partition:
   - Make KEYS requests to each node that has this partition, and then LOOKUP requests for each reported key in the response
   - Save any and all information retrieved from these LOOKUPS

*SUB* socket reader:
 - On each update, if an actual entry in the table was changed, make the update and then *PUB* the update in case any other peers
   haven't recieved it yet

*XREP* socket reader:
 - Service each request.

Lookup procedure:
 - if parition is held locally, service request locally
 - otherwise, use known partition mapping to request the information from the responsible partition

Higher-Level ideas:
 - ADD operation: Add a particular value to the set of values under a particular key.
 - Use table itself to hold list of peers responsible for each bucket (each node that is responsible must subscribe to this hash).
 - Use table itself to hold comprehensive list of peers.
 - Provide means to do requests on a 2nd order (not directly connected) peer without initiating a connection.
 - BOOTSTRAP request where bootstrapping node provides seed info to speed up the initialization process.
 
