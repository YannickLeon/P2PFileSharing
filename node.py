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

from message import Message
from connection import Connection
from file import File
from file_part import FilePart

BC_ADDR = ("0.0.0.0", 9000)
HEARTBEAT_INTERVAL = 0.4
CHUNK_SIZE = 51200


class Node:
    def __init__(self, ip: str, port: int = 0):
        self.peers: list[Connection] = []
        self.files: dict[bytes, File] = {}
        self.peer_dict: dict[uuid.UUID, Connection] = {}
        self.q: Queue = Queue()
        # seperate queue for data messages to stay responsive during data transmission
        self.file_q: Queue = Queue()
        self.file_request_q: Queue = Queue()
        self.file_parts: dict[bytes, FilePart] = {}

        # requests awaiting leader election
        self.open_requests: list[Message] = []

        self.busy_timer = 0
        self.stop = False
        self.uuid = uuid.uuid4()
        print(f"[i] Set own uuid: {self.uuid}")
        self.leader_uuid = None
        # 0 is reserved to identify non-multicast messages (don't need to be propagated)
        self.multicast_counter: np.uint16 = np.uint16(1)
        # mutexes for dict thread safety
        self.heartbeat_mutex = Lock()
        self.file_mutex = Lock()
        self.file_part_mutex = Lock()
        self.sending_mutex = Lock()
        self.peer_mutex = Lock()
        # mutex to deal with threading issues when accepting and identifying new peers
        self.mutex = Lock()

        # Initialize vector clocks for this node
        self.vector_clock: dict[uuid.UUID, np.uint16] = {self.uuid: np.uint16(0)}
        self.clock_mutex = Lock()

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
        self.file_request_thread = Thread(target=self.file_request_handler)
        self.file_request_thread.start()
        self.file_integrity_thread = Thread(target=self.file_integrity_handler)
        self.file_integrity_thread.start()
        self.heartbeat_checker_thread = Thread(target=self.check_heartbeat)
        self.heartbeat_checker_thread.start()
        self.heartbeat_thread = Thread(target=self.send_heartbeat)
        self.heartbeat_thread.start()
        self.discovery_thread = Thread(target=self.discovery)
        self.discovery_thread.start()

    def register_file(self, file_path: str):
        try:
            file_hash = hashlib.sha1()
            # Get file hash
            with open(file_path, "rb") as f:
                while True:
                    segment = f.read(file_hash.block_size)
                    if not segment:
                        break
                    file_hash.update(segment)
            file_hash = file_hash.digest()
            # if file already exists, we simply register as a provider
            if file_hash in self.files:
                if self.uuid in self.files[file_hash].providers:
                    print("[i] You are already providing that file.")
                    return
                file = self.files[file_hash]
                file.providers.append(self.uuid)
                file.file_path = file_path
                # notify peers
                msg = Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), 
                              file.hash + file.size.tobytes() + str.encode(file.name))
                self.increment_clock()
                msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
                self.message_peers(msg)
                return
            # otherwise a new file object is created, then we register as a provider
            file_name = file_path.split("/")[-1]
            file = File(file_hash, file_name, np.uint64(os.path.getsize(
                file_path)), [self.uuid, ], file_path)
            with self.file_mutex:
                self.files[file.hash] = file
            # notify peers
            msg = Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), 
                          file.hash + file.size.tobytes() + str.encode(file.name))
            self.increment_clock()
            msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
            self.message_peers(msg)
            return
        except Exception as e:
            print(f"[!] Error when registering file:")
            traceback.print_exc()

    def deregister_file(self, file: File):
        try:
            if not file.hash in self.files:
                print("[!] No file with specified hash was found!")
            self.files[file.hash].providers.remove(self.uuid)
            self.files[file.hash].file_path = None
            # if no providers are left, delete entry
            if not self.files[file.hash].providers:
                with self.file_mutex:
                    del self.files[file.hash]
            # notify peers
            msg = Message(Message.bytecodes["deregister"], self.uuid, 20, file.hash)
            self.increment_clock()
            msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
            self.message_peers(msg)
        except Exception as e:
            print(f"[!] Error while unlisting file: {e}")
            traceback.print_exc()

    # returns files as a simple way to access them in main method (quick and dirty solution)
    def list_files(self, provided_only: bool = False):
        files = []
        c = 0
        print("*** Files **************")
        with self.file_mutex:
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
        print("************************")
        return files

    def request_file(self, file: File):
        try:
            if not file.hash in self.files:
                print("[!] No file with specified hash was found!")
                return
            if self.uuid in file.providers:
                print("[i] You already have this file.")
                return
            num_bytes = np.uint64(0)
            if file.hash in self.file_parts:
                num_bytes = np.uint64(len(self.file_parts[file.hash].data))
            # if this is the leader simply perform load balancing
            if self.uuid == self.leader_uuid:
                provider_uuid = self.select_provider(file.hash)
                msg = Message(Message.bytecodes["request"], self.uuid, 28, num_bytes.tobytes() + file.hash)
                msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
                self.send_message(self.peer_dict[provider_uuid], msg)
                return
            # send message to leader, to allow for load balancing
            for peer in self.peers:
                if peer.uuid == self.leader_uuid:
                    msg = Message(Message.bytecodes["request"], self.uuid, 28, num_bytes.tobytes() + file.hash)
                    msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
                    self.send_message(peer, msg)
                    return
            # if no leader was found append to open requests
            msg = Message(Message.bytecodes["request"], self.uuid, 28, num_bytes.tobytes() + file.hash)
            msg.set_vector(Message.vector_clock_to_bytes(self.vector_clock))
            self.open_requests.append(msg)
            return
        except Exception as e:
            print(f"[!] Error while unlisting file: {e}")
            traceback.print_exc()

    def send_file(self, connection: Connection, file: File, bytes_received: np.uint64):
        # implement message for when a while is not available anymore
        print(f"[i] Started sending file {file.file_path} to {connection.uuid}.")
        try:
            with self.sending_mutex:
                with open(file.file_path, "rb") as f:
                    f.seek(bytes_received)
                    while not self.stop:
                        if not connection in self.peers:
                            break
                        data = f.read(CHUNK_SIZE)
                        if not data:
                            break
                        # verify file integrity before sending data
                        if not file.file_path or file.check_integrity() != 0:
                            break
                        byte = Message.bytecodes["data"]
                        if f.tell() >= file.size:
                            byte = Message.bytecodes["dataend"]
                        connection.send_message(Message(byte, self.uuid, 20+len(data), file.hash + data))
                        # self.send_message(connection, Message(byte, self.uuid, 20+len(data), file.hash + data))
                        # delay is needed to stay responsive
                        time.sleep(0.05)
                print(f"[i] Finished sending file {file.name} to {connection.uuid}.")
        except Exception as e:
            print(f"[!] Error while sending file: {e}")
            traceback.print_exc()

    def file_request_handler(self):
        while not self.stop:
            try:
                with self.sending_mutex:
                    thread: Thread = self.file_request_q.get(block=False)
                    thread.start()
            except queue.Empty:
                time.sleep(1)
                pass
            except Exception as e:
                print(f"[!] Something went wrong file request handling: {e}")
                traceback.print_exc()

    def file_handler(self):
        while not self.stop:
            try:
                msg: Message = self.file_q.get(block=False)
                file_hash = msg.content[:20]
                data = msg.content[20:]
                # create new FilePart object if necessary
                if not file_hash in self.file_parts:
                    with self.file_part_mutex:
                        self.file_parts[file_hash] = FilePart(
                            self.files[file_hash].name, file_hash, self.files[file_hash].size, data, msg.sender_uuid)
                # otherwise just append to data of existing object
                else:
                    self.file_parts[file_hash].append(data)
                    self.file_parts[file_hash].sender = msg.sender_uuid
                # don't do anything else unless dataend
                if not msg.control_byte == msg.bytecodes["dataend"]:
                    continue
                is_match = hashlib.sha1(self.file_parts[file_hash].data).digest() == file_hash
                self.file_parts[file_hash].save()
                # dont register if hash does not match
                if not is_match:
                    print(f"[w] Received file {self.file_parts[file_hash].name} is not matching the hash!")
                else:
                    self.register_file("./test/" + self.files[file_hash].name)
                with self.file_part_mutex:
                    del self.file_parts[file_hash]
            except queue.Empty:
                # small sleep to avoid taking up to much processing power
                time.sleep(0.1)
                pass
            except Exception as e:
                print(f"[!] Something went wrong during message handling: {e}")
                traceback.print_exc()

    def file_integrity_handler(self):
        while not self.stop:
            deleted: list[File] = []
            changed: list[File] = []
            with self.file_mutex:
                for _, file in self.files.items():
                    if not file.file_path:
                        continue
                    integrity = file.check_integrity()
                    if integrity == 0:
                        continue
                    if integrity == 1:
                        deleted.append(file)
                        continue
                    if integrity == 2:
                        changed.append(file)
                        continue
            for file in deleted:
                self.deregister_file(file)
            for file in changed:
                path = file.file_path
                self.deregister_file(file)
                self.register_file(path)
            time.sleep(0.5)
    
    def list_file_parts(self):
        print("*** Downloads **********")
        with self.file_part_mutex:
            for _, file in self.file_parts.items():
                print(f"\t{file}")
        print("************************")

    def discovery(self):
        # transform each segment of the IP into a single byte value and add to content
        segments = self.sock.getsockname()[0].split(".")
        content = b""
        for seg in segments:
            content += np.uint8(int(seg)).tobytes()
        content += self.sock.getsockname()[1].to_bytes(length=4, byteorder="big", signed=False)
        while not self.stop:
            if len(self.peers) > 0:
                time.sleep(0.5)
                continue
            print("[i] Sending discovery broadcast...")
            self.send_broadcast(
                Message(Message.bytecodes["init"], self.uuid, 8, content))
            time.sleep(5)

    def send_message(self, peer: Connection, msg: Message):
        # if an exception was raised while sending a message, suspect the peer
        if not peer.send_message(msg):
            print(f"[i] Error while sending message to {peer.uuid}, disconnecting...")
            if peer.uuid == self.leader_uuid:
                self.open_requests.append(msg)
            dc_msg = Message(Message.bytecodes["disconnect"], self.uuid, 16, peer.uuid.bytes)
            Thread(target=self.message_peers, args=[dc_msg, True]).start()
            self.disconnect(peer)


    def add_peer(self, peer: Connection):
        with self.heartbeat_mutex:
            self.peers.append(peer)

    # establish outgoing connection to peer
    def connect(self, addr, msg: Message):
        # do nothing if peer is already connected to
        if msg.sender_uuid in self.peer_dict:
            return
        print(f"[i] Trying to connect with: {msg.sender_uuid}")
        # establish TCP connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        peer = Connection("outgoing", addr, sock, self.q,
                          self.file_q, msg.sender_uuid)
        # create entries in data structures
        self.peer_dict[msg.sender_uuid] = peer
        self.vector_clock[msg.sender_uuid] = np.uint16(0)
        self.send_message(peer, Message(Message.bytecodes["identify"], self.uuid))
        print(f"[i] New connection with {addr}")
        # simply send message to all peers without forwarding to maintain consistent state (message is "self forwarding")
        Thread(target=self.message_peers, args=[msg, False]).start()
        # notify peer of provided files
        with self.file_mutex:
            for _, file in self.files.items():
                if self.uuid in file.providers:
                    custom_clock = {self.uuid: self.vector_clock[self.uuid]}
                    for unique_id in self.peer_dict:
                        custom_clock[unique_id] = np.uint16(0)
                    msg = Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), file.hash + file.size.tobytes() + str.encode(file.name))
                    msg.set_vector(Message.vector_clock_to_bytes(custom_clock))
                    self.send_message(peer, msg)
        self.add_peer(peer)
        # send current leader uuid to newly connected peer if exists, if not exists and own uuid is lowest, trigger election
        if not self.leader_uuid:
            self.start_election()
        else:
            self.send_message(peer, Message(Message.bytecodes["leader"], self.uuid, 16, self.leader_uuid.bytes))

    def disconnect(self, connection: Connection):
        # if disconnect has already been called for that peer, do nothing
        if connection.stop:
            return
        connection.close()
        print(f"[i] Disconnected peer {connection.uuid}")
        if connection.uuid != None:
            # remove all files of disconnected peer and adjust downloads
            with self.file_mutex:
                file_hashes = []
                removeables = []
                for _, file in self.files.items():
                    if connection.uuid in file.providers:
                        file.providers.remove(connection.uuid)
                        if not file.providers:
                            removeables.append(file.hash)
                            continue
                        # remember hash if there are other providers
                        file_hashes.append(file.hash)
                for removeable in removeables:
                    del self.files[removeable]
                    # cancel file downloads for files without providers
                    if removeable in self.file_parts:
                        print(f"[i] No more providers for {self.file_parts[removeable].name}, aborting download.")
                        with self.file_part_mutex:
                            del self.file_parts[removeable]
                # look for a new provider, where possible
                for file_hash in file_hashes:
                    if not file_hash in self.file_parts:
                        continue
                    if self.file_parts[file_hash].sender != connection.uuid:
                        continue
                    print(f"[i] Getting new provider for {self.file_parts[file_hash].name}.")
                    self.request_file(self.files[file_hash])
            # adjust remaining data structures
            if connection.uuid in self.vector_clock:
                with self.clock_mutex:
                    del self.vector_clock[connection.uuid]
            if connection.uuid in self.peer_dict:
                del self.peer_dict[connection.uuid]
        if connection in self.peers:
            with self.heartbeat_mutex:
                self.peers.remove(connection)
        # start election if disconnected peer was the leader
        if connection.uuid == self.leader_uuid:
            self.start_election()

    # notify peers and close all connections
    def leave(self):
        self.bc_listen.close()
        self.stop = True
        self.message_thread.join()
        self.file_thread.join()
        self.file_request_thread.join()
        self.file_integrity_thread.join()
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
        # overflow is planned, when max_int is reached we continue at 1
        if set_multicast_counter:
            msg.id = np.uint16(self.multicast_counter)
            if self.multicast_counter < np.iinfo(np.uint16).max:
                self.multicast_counter += np.uint16(1)
            else:
                self.multicast_counter = np.uint16(1)
        for peer in self.peers:
            self.send_message(peer, msg)

    # Accept new connections from peers if no outgoing or incoming connectiosn exist
    #   outgoing: connections which are initiated by this peer
    #   incoming: connections which are accepted by this peer
    def connection_handler(self):
        while not self.stop:
            try:
                ready = select.select([self.sock], [], [], 0.5)
                if not ready[0]:
                    continue
                # acquire mutex to avoid identification during connection handling
                with self.mutex:
                    conn, addr = self.sock.accept()
                    if any(peer.addr == addr for peer in self.peers):
                        continue
                    peer = Connection("incoming", addr, conn, self.q, self.file_q)
                    self.add_peer(peer)
                    print(f"[i] New connection by {addr}")
                # notify peer of provided files, and update its clock accordingly
                with self.file_mutex:
                    for _, file in self.files.items():
                        if self.uuid in file.providers:
                            custom_clock = {self.uuid: self.vector_clock[self.uuid]}
                            for unique_id in self.peer_dict:
                                custom_clock[unique_id] =  np.uint16(0)
                            msg = Message(Message.bytecodes["register"], self.uuid, 28+len(file.name), file.hash + file.size.tobytes() + str.encode(file.name))
                            msg.set_vector(Message.vector_clock_to_bytes(custom_clock))
                            self.send_message(peer, msg)
            except:
                continue
        print("[i] Stopped connection thread.")

    def check_heartbeat(self):
        while not self.stop:
            t = time.time()
            removeable = []
            # check heartbeat timer for all peers
            with self.heartbeat_mutex:
                for peer in self.peers:
                    if peer.heartbeat + (5*HEARTBEAT_INTERVAL) < time.time():
                        removeable.append(peer)
            # disconnect all suspected peers
            for peer in removeable:
                if peer in self.peers:
                    print(f"[i] No heartbeat from {peer.uuid}")
                    # suspect failure and send notification to all peers to maintain consistent state
                    msg = Message(Message.bytecodes["disconnect"], self.uuid, 16, peer.uuid.bytes)
                    Thread(target=self.message_peers, args=[msg, True]).start()
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
        with self.peer_mutex:
            if msg.sender_uuid in self.peer_dict:
                origin = self.peer_dict[msg.sender_uuid]
        if not origin:
            return False
        # accept and forward message if it is in the list of missed multicasts
        if msg.id in origin.missed_multicasts:
            origin.missed_multicasts.remove(msg.id)
            Thread(target=self.message_peers, args=[msg, False]).start()
            return True
        # accept and forward message if it has a larger id
        if msg.id > origin.multicast_counter:
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(int(origin.multicast_counter+1), int(msg.id))])
            origin.multicast_counter = msg.id
            Thread(target=self.message_peers, args=[msg, False]).start()
            return True
        # if msg id is much smaller than the counter for that peer, we assume an overflow has happened and act as if it was larger
        if origin.multicast_counter - msg.id > (np.iinfo(np.uint16).max/2): 
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(int(self.multicast_dict[msg.sender_uuid]+1), int(np.iinfo(np.uint16).max))])
            origin.missed_multicasts.extend(
                [np.uint16(i) for i in range(1, int(msg.id))])
            origin.multicast_counter = msg.id
            Thread(target=self.message_peers, args=[msg, False]).start()
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

                # Extract vector clock from the message content if message is clock_sensitive
                if msg.vector_length > 0 and msg.control_byte in msg.clock_sensitive:
                    incoming_clock = Message.vector_clock_from_bytes(msg.vector)
                    self.merge_clock(incoming_clock)
                    # print("[i] Updated vector clock.")

                if msg.control_byte == msg.bytecodes["init"]:
                    # ignore if message is of insufficient length
                    if msg.length < 8:  
                        continue
                    # ignore already known peers
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
                    with self.mutex:
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
                            self.send_message(peer, Message(msg.bytecodes["abort"], self.uuid))
                            self.disconnect(msg.connection)
                        # otherwise set UUID and create entries in data structures
                        msg.connection.uuid = msg.sender_uuid
                        self.peer_dict[msg.sender_uuid] = msg.connection
                        self.vector_clock[msg.sender_uuid] = np.uint16(0)
                    continue
                if msg.control_byte == msg.bytecodes["abort"]:
                    self.disconnect(msg.connection)
                    continue
                if msg.control_byte == msg.bytecodes["disconnect"]:
                    peer_id = uuid.UUID(bytes=msg.content)
                    # I hope this is threadsafe
                    if peer_id in self.peer_dict:
                        self.disconnect(self.peer_dict[peer_id])
                    continue
                if msg.control_byte == msg.bytecodes["election"]:
                    if msg.sender_uuid < self.uuid:
                        # propagate the election message further
                        self.start_election()
                    continue
                if msg.control_byte == msg.bytecodes["leader"]:
                    if len(msg.content) < 16:
                        continue
                    self.leader_uuid = uuid.UUID(bytes=msg.content)
                    # send open requests waiting for leader, no extra thread should be required as leader election happens almost immediately after dc
                    for request in self.open_requests:
                        if self.leader_uuid == self.uuid:
                            self.q.put(request)
                        else: 
                            self.send_message(self.peer_dict[self.leader_uuid], request)
                        time.sleep(0.05)
                    self.open_requests.clear()
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
                    # otherwise create file object
                    with self.file_mutex:
                        self.files[file_hash] = File(file_hash, file_name, np.uint64(file_size), [msg.sender_uuid, ])
                    continue
                if msg.control_byte == msg.bytecodes["deregister"]:
                    if len(msg.content) < 20:
                        continue
                    file_hash = msg.content[:20]
                    if not file_hash in self.files:
                        continue
                    if msg.sender_uuid in self.files[file_hash].providers:
                        self.files[file_hash].providers.remove(msg.sender_uuid)
                        # if no providers are left delete all related File and FilePart entries
                        if not self.files[file_hash].providers:
                            with self.file_mutex:
                                del self.files[file_hash]
                            if not file_hash in self.file_parts:
                                continue
                            print(f"[i] No more providers for {self.file_parts[file_hash].name}, aborting download.")
                            with self.file_part_mutex:
                                del self.file_parts[file_hash]
                            continue
                        # otherwise request leader to find new provider for FileParts
                        if file_hash in self.file_parts:
                            self.request_file(self.files[file_hash])
                    continue
                if msg.control_byte == msg.bytecodes["request"]:
                    if len(msg.content) < 20:
                        continue
                    # if local state is behind request state, delay request
                    if not self.compare_clocks(msg.vector_clock_from_bytes(msg.vector)):
                        self.q.put(msg)
                        continue
                    bytes_received = np.frombuffer(
                        msg.content[:8], dtype=np.uint64)[0]
                    file_hash = msg.content[8:28]
                    if not file_hash in self.files:
                        continue
                    # load balancing if leader
                    if self.uuid == self.leader_uuid:
                        provider_uuid = self.select_provider(file_hash)
                        if provider_uuid != self.uuid:
                            print("[i] Forwarded a file request to provider")
                            self.send_message(self.peer_dict[provider_uuid], msg)
                            continue
                    # create a thread sending file to peer
                    self.file_request_q.put(Thread(target=self.send_file, args=[
                                            self.peer_dict[msg.sender_uuid], self.files[file_hash], bytes_received]))
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

    # function used for load balancing
    def select_provider(self, file_hash: bytes) -> uuid.UUID:
        provider = None
        # compare other peers as provider
        for uuid in self.files[file_hash].providers:
            if uuid != self.uuid and (provider == None or self.peer_dict[uuid].busy_timer < provider.busy_timer):
                provider = self.peer_dict[uuid]
        # check for self as file provider
        if self.uuid in self.files[file_hash].providers and (provider == None or self.busy_timer < provider.busy_timer):
            provider = self
        # set provider busy_timer depending on file size
        if provider.busy_timer < time.time():
            provider.busy_timer = time.time() + self.files[file_hash].size / (CHUNK_SIZE*10)
        else:
            provider.busy_timer += self.files[file_hash].size / (CHUNK_SIZE*10)
        return provider.uuid

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
                self.send_message(node, Message(Message.bytecodes["election"], self.uuid))

    def announce_leader(self, leader_uuid):
        # announce this node as the leader
        self.leader_uuid = leader_uuid
        print(f"[i] Announcing leader: {leader_uuid}")
        leader_msg = Message(Message.bytecodes["leader"], self.uuid, 16, leader_uuid.bytes)
        self.message_peers(leader_msg)

    def increment_clock(self):
        # increment this node's clock
        with self.clock_mutex:
            self.vector_clock[self.uuid] += np.uint16(1)

    def merge_clock(self, incoming_clock: dict):
        # merge incoming vector clock with the nodes's current clock
        with self.clock_mutex:
            for node, timestamp in incoming_clock.items():
                if node not in self.vector_clock:
                    self.vector_clock[node] = timestamp
                    continue
                # take min value if overflow occured
                if abs(int(timestamp) - int(self.vector_clock[node])) > np.iinfo(np.uint16).max/2:
                    self.vector_clock[node] = min(self.vector_clock[node], timestamp)
                    continue
                self.vector_clock[node] = max(self.vector_clock[node], timestamp)

    # returns True if own clock is bigger than incoming clock
    def compare_clocks(self, clock: dict[uuid.UUID, np.uint16]) -> bool:
        with self.clock_mutex:
            for node, timestamp in clock.items():
                # sometimes vectors contain peers that recently disconnected, ignore them
                if node in self.vector_clock and timestamp > self.vector_clock[node]:
                    return False
        return True
    
    def print_vector_clock(self, clock: dict[uuid.UUID, np.uint16]):
        with self.clock_mutex:
            print("[Vector Clock]")
            for node, timestamp in clock.items():
                print(f"\tNode {node}: {timestamp}")
            print("-" * 20)