# P2PFileSharing
A local peer-to-peer file sharing application developed as a student project for DS1

## Messages
### Structure
|control-byte|vector-length|vector|uuid|multicast-id|length|content|
|------------|-------------|------|----|--|------|-------|
|1 byte|2 byte|?|16 byte|2 byte|4 byte|?|
### Types
|name|control-byte|length|content|
|----|------------|------|-------|
|init|0|8|IP(4byte)+port(uint32)|
|identify|1|0|-|
|disconnect|2|16|uuid|
|heartbeat|3|0|-|
|abort|4|0|-|
|register|10|28+?|hash(20byte)+size(uint64)+name(?)|
|de-register|11|20|hash(20byte)|
|request|12|28|received(uint64)+hash(20byte)|
|data|13|20+?|hash(20byte)+file data(?)|
|dataend|14|20+?|hash(20byte)+file data(?)|
|election|20|0|-|
|leader|22|16|leader-uuid|
### Reliability
For reliable mutlicasting, we take advantage of TCPs reliability guarantees and simply use a series of unicasts. However, this creates the possibility of a peer dying before having sent the message to all peers. In order to deal with this, we simply forward every multicast received to all other peers exactly once. This guarantees that either all or none of the peers receive a multicast. In order to make sure we don't run into endless message forwarding we need to forward every message exactly once. To achieve this, every message contains the sender_uuid and a multicast-id/counter. This multicast-id is locally managed by each node and incremented after each multicast sent. When a multicast is received, we only accept it if it has an unknown combination of sender_uuid and multicast-id. This is implemented by accepting all messages with a higher id as the one last seen from the sender. All "missed" ids are stored in a list and can still be accepted later.

## State Management
### Peers
In order to maintain a consistent group view among all peers we notify every peer when a new node joins the network or when a node leaves the network. Since we know that our multicasts are reliable, it is sufficient to simply send these "events" instead of comparing entire states. To monitor connections to other peers we use the heartbeat protocoll. Even if only one peer detects a connection failure via that protocoll, it will inform the entire network about it and all nodes will suspect a failure of that node. In the event that the suspected node is however still running, it can simply rejoin the network by using a discovery broadcast.
### Files
The same is true for files, as long as we carefully multicast events we can achieve a consistent state. We use multicasting to automatically deregister files of a suspected node and every node can use mutlicasts to manually register and deregister files. When a new node joins the network, each peer will inform it of the files it offers using a tcp unicast.

## Dynamic Discovery
While a node does not know of any other peers it will periodically send a broadcast on the designated port. This broadcast message contains its ip and port, as well as its uuid. When this message is received by any other peer, it will establish a tcp connection and inform all other nodes about the newly joined peer. After the connection is established, an identification message containing the uuid is immediately sent to the freshly joined node. In the case of two peers receiving each others broadcast at the same time, we might encounter duplicate connections as each peer will have and outgoing and incoming connection for the other one. To fix this, the peer with lower uuid will abort the connection and instructs the other peer to do the same via a tcp unicast.

## Election
We use the Bully election algorithm. An election is started, when the leader disconnects or when a new node joins and no leader is currently elected.

## Files
A peer can at any time register an arbitrary file. On register all other peers are notified via multicast, each file can view a list of currently registered files and may requests a file. When requesting a file, the peer receiving the request will transmit the file in chunks of 2kb. The final message uses a different bytecode to mark the end of transmission. Finally the peer will compare the hash of the received file with the expected hash and prints a warning if they don't match. Once a file is fully transmitted, the receiver will automatically register that file and will act as a provider from now on.

## Load Balancing

## Ordering
