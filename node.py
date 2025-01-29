import select
import socket
import uuid
import queue
from queue import Queue
from threading import Thread, Lock
import numpy as np
import time
import hashlib
import os
import traceback
import random

from message import Message
from connection import Connection
from file import File
from file_part import FilePart

BC_ADDR = ("0.0.0.0", 9000)
HEARTBEAT_INTERVAL = 0.4


class Node:
    def __init__(self, ip: str, port: int = 0):
        self.peers: list[Connection] = []
        self.files: dict[bytes, File] = {}
        self.q: Queue = Queue()
        # seperate queue for data messages to stay responsive during data transmission
        self.file_q = Queue()
        self.file_request_q = Queue()
        self.file_parts: dict[bytes, FilePart] = {}
        self.stop = False
        self.uuid = uuid.uuid4()
        self.leader_uuid = None
        # 0 is reserved to identify non-multicast messages (don't need to be propagated)
        self.multicast_counter: np.uint16 = 1
        self.heartbeat_mutex = Lock()
        self.file_mutex = Lock()
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
        self.bc_listen = Connection(
            "broadcast", BC_ADDR, bc_listen, self.q, None)

        # Threads
        self.connection_thread = Thread(target=self.connection_handler)
        self.connection_thread.start()
        self.message_thread = Thread(target=self.message_handler)
        self.message_thread.start()
        self.file_thread = Thread(target=self.file_handler)
        self.file_thread.start()
        self.heartbeat_checker_thread = Thread(target=self.check_heartbeat)
        self.heartbeat_checker_thread.start()
        self.heartbeat_thread = Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.discovery_thread = Thread(target=self.discovery)
        self.discovery_thread.start()

    def register_file(self, file_path: str):
        try:
            file_hash = hashlib.sha1()
            with open(file_path, "rb") as f:
                while True:
                    segment = f.read(file_hash.block_size)
                    if not segment:
                        break
                    file_hash.update(segment)
            file_hash = file_hash.digest()
            if file_hash in self.files:
                if self.uuid in self.files[file_hash].providers:
                    print("[i] You are already providing that file.")
                    return
                file = self.files[file_hash]
                file.providers.append(self.uuid)
                file.file_path = file_path
                self.message_peers(
                    Message(Message.bytecodes["register"], self.uuid, 28+len(file.name),
                            file.hash + file.size.tobytes() + str.encode(file.name)))
                return
            file_name = file_path.split("/")[-1]
            file = File(file_hash, file_name, np.uint64(os.path.getsize(
                file_path)), [self.uuid, ], file_path)
            self.file_mutex.acquire()
            self.files[file.hash] = file
            self.file_mutex.release()
            self.message_peers(
                Message(Message.bytecodes["register"], self.uuid, 28+len(file.name),
                        file.hash + file.size.tobytes() + str.encode(file.name)))
            return
        except Exception as e:
            print(f"[!] Error when registering file:")
            traceback.print_exc()

    def deregister_file(self, file: File):
        try:
            if not file.hash in self.files:
                print("[!] No file with specified hash was found!")
            self.files[file.hash].providers.remove(self.uuid)
            if not self.files[file.hash].providers:
                self.file_mutex.acquire()
                del self.files[file.hash]
                self.file_mutex.release()
            self.message_peers(
                Message(Message.bytecodes["deregister"], self.uuid, 20, file.hash))
        except Exception as e:
            print(f"[!] Error while unlisting file: {e}")
            traceback.print_exc()

    # returns files for testing purposes, clean this up later
    def list_files(self, provided_only: bool = False):
        files = []
        c = 0
        print("*** Files **************")
        self.file_mutex.acquire()
        for _, file in self.files.items():
            if self.uuid in file.providers:
                files.append(file)
                print(f"[{c}]{file}")
                c += 1
                continue
            if not provided_only:
                files.append(file)
                print(f"[{c}]{file}")
                c += 1
        self.file_mutex.release()
        print("************************")
        return files

    def request_file(self, file: File):
        try:
            if not file.hash in self.files:
                print("[!] No file with specified hash was found!")
            # pick a random provider (temporary solution), decision will later be made by leader
            provider = self.files[file.hash].providers[random.randrange(
                0, len(self.files[file.hash].providers))]
            for peer in self.peers:
                if peer.uuid == provider:
                    peer.send_message(
                        Message(Message.bytecodes["request"], self.uuid, 28, np.uint64(0).tobytes() + file.hash))
                    return
        except Exception as e:
            print(f"[!] Error while unlisting file: {e}")
            traceback.print_exc()

    def send_file(self, connection: Connection, file: File, bytes_received: np.uint64):
        # implement message for when a while is not available anymore
        print(
            f"[i] Started sending file {file.file_path} to {connection.uuid}.")
        try:
            with open(file.file_path, "rb") as f:
                while True:
                    # currently sending 2048 bytes, might not be the best value :)
                    data = f.read(2048)
                    if not data:
                        break
                    byte = Message.bytecodes["data"]
                    if len(data) < 2048:
                        byte = Message.bytecodes["dataend"]
                    connection.send_message(
                        Message(byte, self.uuid, 20+len(data), file.hash + data))
                    # delay is needed to stay responsive
                    time.sleep(0.1)
            print(
                f"[i] Finished sending file {file.name} to {connection.uuid}.")
        except Exception as e:
            print(f"[!] Error while sending file: {e}")
            traceback.print_exc()

    def file_handler(self):
        while not self.stop:
            try:
                msg: Message = self.file_q.get(block=False)
                file_hash = msg.content[:20]
                data = msg.content[20:]
                if not file_hash in self.file_parts:
                    self.file_parts[file_hash] = FilePart(
                        self.files[file_hash].name, file_hash, data, msg.sender_uuid)
                else:
                    self.file_parts[file_hash].data += data
                if not msg.control_byte == msg.bytecodes["dataend"]:
                    continue
                if not hashlib.sha1(self.file_parts[file_hash].data).digest() == self.file_parts[file_hash].hash:
                    print(
                        f"[w] Received file {self.file_parts[file_hash].name} is not matching the hash!")
                self.file_parts[file_hash].save()
                self.register_file("./test/" + self.files[file_hash].name)
                del self.file_parts[file_hash]
            except queue.Empty:
                # small sleep to avoid taking up to much processing power
                time.sleep(0.1)
                pass
            except Exception as e:
                print("[!] Something went wrong during message handling:")
                traceback.print_exc()

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
        self.heartbeat_mutex.acquire()
        self.peers.append(peer)
        self.heartbeat_mutex.release()

    # establish outgoing connection to peer
    def connect(self, addr, msg: Message):
        print(f"[i] Trying to connect with: {msg.sender_uuid}")
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        peer = Connection("outgoing", addr, sock, self.q,
                          self.file_q, msg.sender_uuid)
        peer.send_message(Message(Message.bytecodes["identify"], self.uuid))
        print(f"[i] New connection with {addr}")
        # simply send message to all peers without forwarding to maintain consistent state (message is "self forwarding")
        Thread(target=self.message_peers, args=[msg, False]).start()
        # # notify peer of provided files
        self.file_mutex.acquire()
        for _, file in self.files.items():
            if self.uuid in file.providers:
                peer.send_message(
                    Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), file.hash + file.size.tobytes() + str.encode(file.name)))
        self.file_mutex.release()
        self.add_peer(peer)
        # send current leader uuid to newly connected peer if exists, if not exists and own uuid is lowest, trigger election
        if not self.leader_uuid:
            # if all(self.uuid < peer.uuid for peer in self.peers):
            self.start_election()
        else:
            peer.send_message(
                Message(Message.bytecodes["leader"], self.uuid, 16, self.leader_uuid.bytes))

    def disconnect(self, connection: Connection):
        connection.close()
        print(f"[i] Disconnected peer {connection.uuid}")
        # remove all files of disconnected peer
        if connection.uuid != None:
            self.file_mutex.acquire()
            removeables = []
            for _, file in self.files.items():
                if connection.uuid in file.providers:
                    file.providers.remove(connection.uuid)
                    if not file.providers:
                        removeables.append(file.hash)
            for removeable in removeables:
                del self.files[removeable]
            self.file_mutex.release()
        if connection in self.peers:
            self.heartbeat_mutex.acquire()
            self.peers.remove(connection)
            self.heartbeat_mutex.release()
        # we need to make sure all peers have the same list of peers before starting the election
        if connection.uuid == self.leader_uuid:
            # if all(self.uuid < peer.uuid for peer in self.peers):
            self.start_election()

    # notify peers and close all connections
    def leave(self):
        self.bc_listen.close()
        self.stop = True
        self.message_thread.join()
        self.file_thread.join()
        self.connection_thread.join()
        self.heartbeat_checker_thread.join()
        self.heartbeat_thread.join()
        self.message_peers(
            Message(Message.bytecodes["disconnect"], self.uuid, 16, self.uuid.bytes))
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
                peer = Connection("incoming", addr, conn, self.q, self.file_q)
                self.add_peer(peer)
                print(f"[i] New connection by {addr}")
                self.mutex.release()
                # notify peer of provided files
                self.file_mutex.acquire()
                for _, file in self.files.items():
                    if self.uuid in file.providers:
                        peer.send_message(
                            Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), file.hash + file.size.tobytes() + str.encode(file.name)))
                self.file_mutex.release()
            except:
                continue
        print("[i] Stopped connection thread.")

    def check_heartbeat(self):
        t = time.time()
        while not self.stop:
            # print(f"[i] interval: {time.time()-t}")
            t = time.time()
            removeable = []
            self.heartbeat_mutex.acquire()
            for peer in self.peers:
                if peer.heartbeat + (8*HEARTBEAT_INTERVAL) < time.time():
                    removeable.append(peer)
            self.heartbeat_mutex.release()
            for peer in removeable:
                if peer in self.peers:
                    print(f"[i] No heartbeat from {peer.uuid}")
                    # suspect failure and send notification to all peers to maintain consistent state
                    msg = Message(
                        Message.bytecodes["disconnect"], self.uuid, 16, peer.uuid.bytes)
                    Thread(target=self.message_peers,
                           args=[msg, True]).start()
                    self.disconnect(peer)
            interval = HEARTBEAT_INTERVAL - (time.time() - t)
            if interval > 0:
                time.sleep(interval)

    def send_heartbeat(self):
        while not self.stop:
            t = time.time()
            self.message_peers(
                Message(Message.bytecodes["heartbeat"], self.uuid), set_multicast_counter=False)
            interval = HEARTBEAT_INTERVAL - (time.time() - t)
            if interval > 0:
                time.sleep(interval)

    def forward_multicast(self, msg: Message) -> bool:
        # Special case if a peer has disconnected but an old message forwarded
        origin: Connection = None
        for peer in self.peers:
            if peer.uuid == msg.sender_uuid:
                origin = peer
        if not origin:
            print("[i] Message origin not found in peers!")
            return False
        if msg.id in origin.missed_multicasts:
            # self.message_peers(msg, set_multicast_counter=False)
            origin.missed_multicasts.remove(msg.id)
            Thread(target=self.message_peers, args=[msg, False]).start()
            return True
        if msg.id > origin.multicast_counter:
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(int(origin.multicast_counter+1), int(msg.id))])
            origin.multicast_counter = msg.id
            Thread(target=self.message_peers, args=[msg, False]).start()
            # self.message_peers(msg, set_multicast_counter=False)
            return True
        # if msg id is much smaller than the counter for that peer, we assume an overflow has happened and act as if it was larger
        if origin.multicast_counter - msg.id > (np.iinfo(np.uint16).max/2):
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(int(self.multicast_dict[msg.sender_uuid]+1), int(np.iinfo(np.uint16).max))])
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(1, int(msg.id))])
            origin.multicast_counter = msg.id
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
                    if any(peer.uuid == msg.sender_uuid for peer in self.peers):
                        continue
                    ip = ""
                    for d in msg.content[:4]:
                        ip += str(np.uint8(d)) + "."
                    ip = ip[:-1]
                    port = int.from_bytes(
                        msg.content[4:8], byteorder="big", signed=False)
                    if any(peer.addr == (ip, port) for peer in self.peers):
                        continue
                    self.connect((ip, port), msg)
                    continue
                if msg.control_byte == msg.bytecodes["identify"]:
                    # mutex to avoid identifying peers before adding them to the peer list
                    self.mutex.acquire(blocking=True)
                    # check if duplicate exists
                    peer = None
                    for p in self.peers:
                        if p.uuid == msg.sender_uuid:
                            peer = p
                            break
                    # duplicates are possible if two peers try to both open a connection, this happens if a broadcast arrives simultaneously
                    # Solution: peer with lower uuid aborts the incoming connection
                    if peer != None and peer.uuid < self.uuid:
                        # close connection and notify
                        peer.send_message(
                            Message(msg.bytecodes["abort"], self.uuid))
                        self.disconnect(msg.connection)
                    # print(f"[i] Received identification from {msg.connection.addr}")
                    msg.connection.uuid = msg.sender_uuid
                    self.mutex.release()
                    continue
                if msg.control_byte == msg.bytecodes["abort"]:
                    self.disconnect(msg.connection)
                    continue
                if msg.control_byte == msg.bytecodes["disconnect"]:
                    peer_id = uuid.UUID(bytes=msg.content)
                    for peer in self.peers:
                        if peer.uuid == peer_id:
                            self.disconnect(peer)
                            break
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
                if msg.control_byte == msg.bytecodes["register"]:
                    if len(msg.content) < 28:
                        continue
                    file_hash = msg.content[:20]
                    file_size = np.frombuffer(
                        msg.content[20:28], dtype=np.uint64)[0]
                    file_name = msg.content[28:].decode()
                    # check if file is already in file list
                    if file_hash in self.files:
                        if msg.sender_uuid in self.files[file_hash].providers:
                            continue
                        self.files[file_hash].providers.append(msg.sender_uuid)
                        continue
                    self.file_mutex.acquire()
                    self.files[file_hash] = File(
                        file_hash, file_name, np.uint64(file_size), [msg.sender_uuid, ])
                    self.file_mutex.release()
                    continue
                if msg.control_byte == msg.bytecodes["deregister"]:
                    if len(msg.content) < 20:
                        continue
                    file_hash = msg.content[:20]
                    if not file_hash in self.files:
                        continue
                    if msg.sender_uuid in self.files[file_hash].providers:
                        self.files[file_hash].providers.remove(msg.sender_uuid)
                        if not self.files[file_hash].providers:
                            self.file_mutex.acquire()
                            del self.file[file_hash]
                            self.file_mutex.release()
                    continue
                if msg.control_byte == msg.bytecodes["request"]:
                    if len(msg.content) < 20:
                        continue
                    bytes_received = np.frombuffer(
                        msg.content[:8], dtype=np.uint64)[0]
                    file_hash = msg.content[8:28]
                    if not file_hash in self.files:
                        continue
                    # create a thread sending file to peer
                    for peer in self.peers:
                        if peer.uuid == msg.sender_uuid:
                            Thread(target=self.send_file, args=[
                                   peer, self.files[file_hash], bytes_received]).start()
                            break
                    continue
                print(f"{msg}")
            except queue.Empty:
                # trying out sleep to decrease delay in heartbeat loop
                time.sleep(0.1)
                pass
            except Exception as e:
                print("[!] Something went wrong during message handling:")
                traceback.print_exc()
        print("[i] Stopped message handler")

    def list_peers(self):
        print("*** Peers **************")
        for peer in self.peers:
            if peer.uuid == self.leader_uuid:
                print(f"\tLeader: {peer.uuid}")
                continue
            print(f"\t{peer.uuid}")
        print("************************")

    def start_election(self):
        print("Starting election...")
        higher_nodes = [peer for peer in self.peers if peer.uuid !=
                        None and peer.uuid > self.uuid]

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
