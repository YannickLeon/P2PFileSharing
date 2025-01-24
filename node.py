import select
import socket
import uuid
import queue
from queue import Queue
from threading import Thread, Lock
import numpy as np
import time

from message import Message
from connection import Connection

BC_ADDR = ("0.0.0.0", 9000)
HEARTBEAT_INTERVAL = 3


class Node:
    def __init__(self, ip: str, port: int = 0):
        self.peers: list[Connection] = []
        self.q: Queue = Queue()
        self.stop = False
        self.uuid = uuid.uuid4()
        self.leader_uuid = None
        # 0 is reserved to identify non-multicast messages (don't need to be propagated)
        self.multicast_counter: np.uint16 = 1
        # dict to store the multicast counters for each peer
        self.multicast_dict: dict[uuid.UUID, np.uint16] = {}
        # dict to store the missed counters to later accept them
        self.missed_multicasts: dict[uuid.UUID, list[np.uint16]] = {}
        self.heartbeat_timers: dict[Connection, float] = {}
        self.heartbeat_mutex = Lock()
        # mutex to deal with threading issues when accepting and identifying new peers
        self.mutex = Lock()

        # socket to listen for incoming connections
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.sock.bind((ip, port))
        self.sock.listen(1)
        print(f"[i] Server listening on {ip}:{self.sock.getsockname()[1]}.")

        # socket to send broadcasts
        self.bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # create connection to listen for broadcasts
        bc_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bc_listen.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        bc_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bc_listen.bind(BC_ADDR)
        self.bc_listen = Connection("broadcast", BC_ADDR, bc_listen, self.q)

        # Threads
        self.connection_thread = Thread(target=self.connection_handler)
        self.connection_thread.start()
        self.message_thread = Thread(target=self.message_handler)
        self.message_thread.start()
        self.heartbeat_checker_thread = Thread(target=self.check_heartbeat)
        self.heartbeat_checker_thread.start()
        self.heartbeat_thread = Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.discovery_thread = Thread(target=self.discovery)
        self.discovery_thread.start()

    def discovery(self):
        segments = self.sock.getsockname()[0].split(".")
        content = b""
        for seg in segments:
            content += np.uint8(int(seg)).tobytes()
        content += self.sock.getsockname()[1].to_bytes(
            length=4, byteorder="big", signed=False)
        while not self.stop:
            if len(self.peers) > 0:
                time.sleep(0.5)
                continue
            print("[i] Sending discovery broadcast...")
            self.send_broadcast(
                Message(Message.bytecodes["init"], self.uuid, 8, content))
            time.sleep(5)

    def add_peer(self, peer: Connection):
        self.peers.append(peer)
        self.heartbeat_mutex.acquire()
        self.heartbeat_timers[peer] = time.time()
        self.heartbeat_mutex.release()

    # establish outgoing connection to peer
    def connect(self, addr, unique_id):
        print(f"[i] Trying to connect with: {unique_id}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        peer = Connection("outgoing", addr, sock, self.q, unique_id)
        peer.send_message(Message(Message.bytecodes["identify"], self.uuid))
        print(f"[i] New connection with {addr}")
        self.add_peer(peer)
        self.multicast_dict[peer.uuid] = 0
        self.missed_multicasts[peer.uuid] = []
        # send current leader uuid to newly connected peer if exists, if not exists trigger election
        if not self.leader_uuid:
            self.start_election()
        else:
            peer.send_message(
                Message(Message.bytecodes["leader"], self.uuid, 16, self.leader_uuid.bytes))

    def disconnect(self, connection: Connection):
        connection.close()
        print(f"[i] Disconnected peer {connection.uuid}")
        self.heartbeat_mutex.acquire()
        del self.multicast_dict[connection.uuid]
        del self.missed_multicasts[connection.uuid]
        del self.heartbeat_timers[connection]
        self.heartbeat_mutex.release()
        self.peers.remove(connection)
        # we need to make sure all peers have the same list of peers before starting the election
        if connection.uuid == self.leader_uuid:
            self.start_election()

    # notify peers and close all connections
    def leave(self):
        self.bc_listen.close()
        self.stop = True
        self.message_thread.join()
        self.connection_thread.join()
        self.heartbeat_checker_thread.join()
        self.heartbeat_thread.join()
        self.message_peers(Message(Message.bytecodes["disconnect"], self.uuid))
        for peer in self.peers:
            peer.close()
        self.peers.clear()
        self.sock.close()

    def send_broadcast(self, msg: Message):
        self.bc_sock.sendto(msg.to_bytes(), ('<broadcast>', 9000))

    # send message to all peers
    def message_peers(self, msg: Message, set_multicast_counter=True):
        # overflow is planned, when max_int is reached we continue at 0
        if set_multicast_counter:
            msg.id = np.uint16(self.multicast_counter)
            if self.multicast_counter < np.iinfo(np.uint16).max:
                self.multicast_counter += 1
            else:
                self.multicast_counter = 1
        for peer in self.peers:
            peer.send_message(msg)

    # Accept new connections from peers if no outgoing or incoming connectiosn exist
    #   outgoing: connections which are initiated by this peer
    #   incoming: connections which are accepted by this peer
    def connection_handler(self):
        while not self.stop:
            try:
                ready = select.select([self.sock], [], [], 0.5)
                if not ready[0]:
                    continue
                self.mutex.acquire(blocking=True)
                conn, addr = self.sock.accept()
                if any(peer.addr == addr for peer in self.peers):
                    continue
                peer = Connection("incoming", addr, conn, self.q)
                self.add_peer(peer)
                print(f"[i] New connection by {addr}")
                self.mutex.release()
            except:
                continue
        print("[i] Stopped connection thread.")

    def check_heartbeat(self):
        while not self.stop:
            time.sleep(1)
            removeable = []
            self.heartbeat_mutex.acquire()
            for peer, timer in self.heartbeat_timers.items():
                if timer + (2*HEARTBEAT_INTERVAL) < time.time():
                    removeable.append(peer)
            self.heartbeat_mutex.release()
            for peer in removeable:
                if peer in self.peers:
                    print(f"[i] No heartbeat from {peer.uuid}")
                    # TODO: send notification to all peers to maintain consistent state
                    self.disconnect(peer)

    def send_heartbeat(self):
        while not self.stop:
            self.message_peers(
                Message(Message.bytecodes["heartbeat"], self.uuid), set_multicast_counter=False)
            time.sleep(HEARTBEAT_INTERVAL)

    def forward_multicast(self, msg: Message) -> bool:
        # Special case if a peer has disconnected but an old message forwarded
        if not msg.sender_uuid in self.missed_multicasts or not msg.sender_uuid in self.multicast_dict:
            return False
        if msg.id in self.missed_multicasts[msg.sender_uuid]:
            # self.message_peers(msg, set_multicast_counter=False)
            self.missed_multicasts[msg.sender_uuid].remove(msg.id)
            Thread(target=self.message_peers, args=[msg, False]).start()
            return True
        if msg.id > self.multicast_dict[msg.sender_uuid]:
            self.missed_multicasts[msg.sender_uuid].extend(
                [np.uint16(i) for i in range(int(self.multicast_dict[msg.sender_uuid]+1), int(msg.id))])
            self.multicast_dict[msg.sender_uuid] = msg.id
            Thread(target=self.message_peers, args=[msg, False]).start()
            # self.message_peers(msg, set_multicast_counter=False)
            return True
        # if msg id is much smaller than the counter for that peer, we assume an overflow has happened and act as if it was larger
        if self.multicast_dict[msg.sender_uuid] - msg.id > (np.iinfo(np.uint16).max/2):
            self.missed_multicasts[msg.sender_uuid].extend(
                [np.uint16(i) for i in range(int(self.multicast_dict[msg.sender_uuid]+1), int(np.iinfo(np.uint16).max))])
            self.missed_multicasts[msg.sender_uuid].extend(
                [np.uint16(i) for i in range(1, int(msg.id))])
            self.multicast_dict[msg.sender_uuid] = msg.id
            Thread(target=self.message_peers, args=[msg, False]).start()
            # self.message_peers(msg, set_multicast_counter=False)
            return True
        return False

    # INFO: For all messages that can be sent as multicasts we cannot rely on using the msg.connection attribute,
    # instead use msg.uuid and select the peer that way (see disconnect for an example)
    def message_handler(self):
        while not self.stop:
            try:
                msg: Message = self.q.get(block=False)
                if msg.sender_uuid == self.uuid:  # ignore own messages
                    continue
                if msg.id != 0:  # check if message is a multicast
                    # if the return value is False, we can ignore the message and continue as we already processed it
                    if not self.forward_multicast(msg):
                        continue
                if msg.control_byte == msg.bytecodes["init"]:
                    if msg.length < 8:  # ignore if message is of insufficient length
                        continue
                    if any(peer.uuid == msg.sender_uuid for peer in self.peers) or self.uuid == msg.sender_uuid:
                        continue
                    ip = ""
                    for d in msg.content[:4]:
                        ip += str(np.uint8(d)) + "."
                    ip = ip[:-1]
                    port = int.from_bytes(
                        msg.content[4:8], byteorder="big", signed=False)
                    if any(peer.addr == (ip, port) for peer in self.peers):
                        continue
                    self.connect((ip, port), msg.sender_uuid)
                    continue
                if msg.control_byte == msg.bytecodes["identify"]:
                    # mutex to avoid identifying peers before adding them to the peer list
                    self.mutex.acquire(blocking=True)
                    # print(f"[i] Received identification from {msg.connection.addr}")
                    # remove peer if its a duplicate, not sure if needed
                    if any(peer.uuid == msg.sender_uuid and msg.connection != peer for peer in self.peers):
                        # print(f"\tDuplicate peer found!")
                        msg.connection.send_message(
                            Message(Message.bytecodes["disconnect"], self.uuid))
                        self.disconnect(msg.connection)
                        continue
                    # print(f"\tPeer found!")
                    msg.connection.uuid = msg.sender_uuid
                    self.multicast_dict[msg.connection.uuid] = 0
                    self.missed_multicasts[msg.connection.uuid] = []
                    self.mutex.release()
                    continue
                if msg.control_byte == msg.bytecodes["disconnect"]:
                    for peer in self.peers:
                        if peer.uuid == msg.sender_uuid:
                            self.disconnect(peer)
                            continue
                    continue
                if msg.control_byte == msg.bytecodes["heartbeat"]:
                    self.heartbeat_timers[msg.connection] = time.time()
                    continue
                if msg.control_byte == msg.bytecodes["election"]:
                    if msg.sender_uuid < self.uuid:
                        # progate the election message further
                        self.start_election()
                    continue
                if msg.control_byte == msg.bytecodes["leader"]:
                    if len(msg.content) < 16:
                        continue
                    self.leader_uuid = uuid.UUID(bytes=msg.content)
                    print(
                        f"[i] New leader announced: <{msg.sender_uuid}:{msg.id}>{self.leader_uuid}")
                    continue
                print(f"{msg}")
            except queue.Empty:
                pass
        print("[i] Stopped message handler")

    def list_peers(self):
        print("*** Peers **************")
        for peer in self.peers:
            if peer.uuid == self.leader_uuid:
                print(f"\tLeader: {peer.uuid}")
                continue
            print(f"\t{peer.uuid}")
        print("************************")

    # is it enough to just announce oneself as a leader if we have the highest uuid?
    # We already have a list of connected peers with the highest uuid as a consistent state.
    def start_election(self):
        print("Starting election...")
        higher_nodes = [peer for peer in self.peers if peer.uuid > self.uuid]

        if not higher_nodes:
            # No higher node exists, this node becomes the leader
            self.announce_leader(self.uuid)
        else:
            for node in higher_nodes:
                node.send_message(
                    Message(Message.bytecodes["election"], self.uuid))

    def announce_leader(self, leader_uuid):
        # announce this node as the leader
        self.leader_uuid = leader_uuid
        print(f"[i] Announcing leader: {leader_uuid}")
        leader_msg = Message(
            Message.bytecodes["leader"], self.uuid, 16, leader_uuid.bytes)
        self.message_peers(leader_msg)
