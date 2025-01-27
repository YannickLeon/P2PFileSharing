import socket
import numpy as np
from threading import Thread
from queue import Queue
import select
import uuid
import time

from message import Message


class Connection():
    def __init__(self, mode: str, addr: tuple[str, int], sock: socket.socket, q: Queue, file_q: Queue, unique_id: uuid.UUID = None):
        self.mode = mode
        self.addr = addr
        self.sock = sock
        self.q = q
        self.file_q = file_q
        self.heartbeat = time.time()
        self.uuid = unique_id
        self.stop = False

        self.multicast_counter: np.uint16 = 0
        self.missed_multicasts: list[np.uint16] = []

        sock.setblocking(False)

        self.listener_thread = Thread(target=self.listen)
        self.listener_thread.start()

    # Listen to messages
    def listen(self):
        data = b""
        while not self.stop:
            try:
                ready = select.select([self.sock], [], [], 0.5)
                if not ready[0]:
                    continue
                content, addr = self.sock.recvfrom(1024 * 8)
                data += content
                if (
                    not data
                    or len(data) < 23
                    or (len(data) - 23) < int.from_bytes(data[19:23], byteorder="big")
                ):
                    continue
                msg = Message(
                    control_byte=np.byte(data[0]),
                    sender_uuid=uuid.UUID(bytes=data[1:17]),
                    id=np.frombuffer(data[17:19], dtype=np.uint16),
                    length=int.from_bytes(data[19:23], byteorder="big"),
                    content=data[23: 23 +
                                 int.from_bytes(data[19:23], byteorder="big")],
                    connection=self,
                )
                if msg.control_byte == msg.bytecodes["heartbeat"]:
                    self.heartbeat = time.time()
                elif msg.control_byte == msg.bytecodes["data"] or msg.control_byte == msg.bytecodes["dataend"]:
                    self.file_q.put(msg)
                else:
                    self.q.put(msg)
                data = data[23 + int.from_bytes(data[19:23], byteorder="big"):]
            except socket.error:
                print("[!] Connection error while receiving data.")
                break
        # print(f"[i] listener stopped for {self.uuid if self.uuid else self.addr}.")

    def send_message(self, msg: Message):
        try:
            self.sock.sendall(msg.to_bytes())
        except socket.error as e:
            print(f"[!] Error while sending message: {e.strerror}")

    def close(self):
        self.stop = True
        self.listener_thread.join()
        self.sock.close()
        # print(f"[i] Closed connection to {self.uuid if self.uuid else self.addr}.")
