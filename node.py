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
HEARTBEAT_INTERVAL = 1


class Node:
    def __init__(self, ip: str, port: int = 0):
        self.peers: list[Connection] = []
        self.q: Queue = Queue()
        self.stop = False
        self.uuid = uuid.uuid4()
        self.heartbeat_timers: dict[Connection, float] = {}
        # mutex to make sure heartbeat is not changed as it is being accessed
        self.heartbeat_mutex = Lock()
        # mutex to deal with threading issues when accepting and identifying new peers
        self.mutex = Lock()

        # socket to listen for incoming connections
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.sock.bind((ip, port))
        self.sock.listen(1)
        ip, port = self.sock.getsockname()
        print(f"[i] Server listening on {ip}:{port}.")

        # socket to send broadcasts
        self.bc_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.bc_sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        # create connection to listen for broadcasts
        bc_listen = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        bc_listen.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        bc_listen.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        bc_listen.bind(BC_ADDR)
        self.bc_listen = Connection("broadcast", BC_ADDR, bc_listen, self.q)

        # connection and message handler threads
        self.connection_thread = Thread(target=self.connection_handler)
        self.connection_thread.start()
        self.message_thread = Thread(target=self.message_handler)
        self.message_thread.start()
        self.heartbeat_thread = Thread(target=self.check_heartbeats)
        self.heartbeat_thread.start()

        # send broadcast to join the network (place somewhere else later)
        segments = ip.split(".")
        content = b""
        for seg in segments:
            content += np.uint8(int(seg)).tobytes()
        content += self.sock.getsockname()[1].to_bytes(
            length=4, byteorder="big", signed=False)
        self.send_broadcast(
            Message(Message.bytecodes["init"], self.uuid, 8, content))

    # establish outgoing connection to peer
    def connect(self, addr, unique_id):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(addr)
        peer = Connection("outgoing", addr, sock, self.q, unique_id)
        peer.send_message(Message(Message.bytecodes["identify"], self.uuid))
        self.peers.append(peer)
        self.heartbeat_timers[peer] = time.time()
        print(f"[i] New connection with {addr}")

    def disconnect(self, connection: Connection):
        connection.close()
        print(f"[i] Disconnected peer {connection.uuid}")
        self.heartbeat_mutex.acquire()
        self.heartbeat_timers.pop(connection)
        self.heartbeat_mutex.release()
        self.peers.remove(connection)

    # notify peers and close all connections
    def leave(self):
        self.bc_listen.close()
        for peer in self.peers:
            peer.send_message(
                Message(Message.bytecodes["disconnect"], self.uuid))
            peer.close()
        self.peers.clear()
        self.stop = True
        self.message_thread.join()
        self.connection_thread.join()
        self.heartbeat_thread.join()
        self.sock.close()

    def send_broadcast(self, msg: Message):
        # print(f"[i] Sending broadcast: {msg}")
        self.bc_sock.sendto(msg.to_bytes(), ('<broadcast>', 9000))

    # send message to all peers
    def message_peers(self, msg: Message):
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
                self.peers.append(peer)
                self.heartbeat_mutex.acquire()
                self.heartbeat_timers[peer] = time.time()
                self.heartbeat_mutex.release()
                print(f"[i] New connection by {addr}")
                self.mutex.release()
            except:
                continue
        print("[i] Stopped connection thread.")

    def check_heartbeats(self):
        while not self.stop:
            removeable = []
            self.heartbeat_mutex.acquire()
            for peer, timer in self.heartbeat_timers.items():
                if timer + HEARTBEAT_INTERVAL + 1 < time.time():
                    # TODO: send notification to all peers to maintain consistent state
                    removeable.append(peer)
            self.heartbeat_mutex.release()
            for peer in removeable:
                if peer in self.peers:
                    self.disconnect(peer)
            time.sleep(1)
            self.message_peers(
                Message(Message.bytecodes["heartbeat"], self.uuid))

    # TODO: implement functionality for each message
    def message_handler(self):
        while not self.stop:
            try:
                msg: Message = self.q.get(block=False)
                if msg.control_byte == msg.bytecodes["init"]:
                    if msg.length < 8:  # ignore if message is of insufficient length
                        continue
                    if any(peer.uuid == msg.uuid for peer in self.peers) or self.uuid == msg.uuid:
                        continue
                    ip = ""
                    for d in msg.content[:4]:
                        ip += str(np.uint8(d)) + "."
                    ip = ip[:-1]
                    port = int.from_bytes(
                        msg.content[4:8], byteorder="big", signed=False)
                    if any(peer.addr == (ip, port) for peer in self.peers):
                        continue
                    self.connect((ip, port), msg.uuid)
                    continue
                if msg.control_byte == msg.bytecodes["identify"]:
                    # mutex to avoid identifying peers before adding them to the peer list
                    self.mutex.acquire(blocking=True)
                    print(
                        f"[i] Received identification from {msg.sender.addr}")
                    # remove peer if its a duplicate, not sure if needed
                    if any(peer.uuid == msg.uuid and msg.sender != peer for peer in self.peers):
                        print(f"\t Duplicate peer found!")
                        msg.sender.send_message(
                            Message(Message.bytecodes["disconnect"], self.uuid))
                        self.disconnect(msg.sender)
                        continue
                    print(f"\t Peer found!")
                    msg.sender.uuid = msg.uuid
                    self.mutex.release()
                    continue
                if msg.control_byte == msg.bytecodes["disconnect"]:
                    self.disconnect(msg.sender)
                    continue
                if msg.control_byte == msg.bytecodes["heartbeat"]:
                    self.heartbeat_timers[msg.sender] = time.time()
                    continue
                print(f"{msg}")
            except queue.Empty:
                pass
            # except Exception as e:
            #     print(f"[!] Error in message_handler: {e}")
        print("[i] Stopped message handler")

    def list_peers(self):
        print("*** Peers **************")
        for peer in self.peers:
            print(f"\t{peer.uuid}")
        print("************************")
