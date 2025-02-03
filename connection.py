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
        self.busy_timer = 0

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
                content = self.sock.recv(1024 * 8)
                data += content
                if (len(data) < 25):
                    continue
                offset = int(np.frombuffer(data[1:3], dtype=np.uint16)[0])
                if len(data) < 25 + offset:
                    continue
                if len(data) < 25 + offset + int.from_bytes(data[21 + offset:25 + offset], byteorder="big"):
                    continue
                msg = Message(
                    control_byte=np.byte(data[0]),
                    vector=data[3:3 + offset],
                    sender_uuid=uuid.UUID(bytes=data[3 + offset:19 + offset]),
                    id=np.frombuffer(
                        data[19 + offset:21 + offset], dtype=np.uint16),
                    length=int.from_bytes(
                        data[21 + offset:25 + offset], byteorder="big"),
                    content=data[25 + offset: 25 + offset +
                                 int.from_bytes(data[21 + offset:25 + offset], byteorder="big")],
                    connection=self,
                )
                if msg.control_byte == msg.bytecodes["heartbeat"]:
                    self.heartbeat = time.time()
                elif msg.control_byte == msg.bytecodes["data"] or msg.control_byte == msg.bytecodes["dataend"]:
                    self.file_q.put(msg)
                else:
                    self.q.put(msg)
                data = data[25 + offset +
                            int.from_bytes(data[21 + offset:25 + offset], byteorder="big"):]
            except socket.error:
                print("[!] Connection error while receiving data.")
                break
        # print(f"[i] listener stopped for {self.uuid if self.uuid else self.addr}.")

    def send_message(self, msg: Message) -> bool:
        try:
            self.sock.sendall(msg.to_bytes())
            return True
        except socket.error as e:
            # print(f"[!] Error while sending message: {e.strerror}")
            return False

    def close(self):
        self.stop = True
        self.listener_thread.join()
        self.sock.close()
        # print(f"[i] Closed connection to {self.uuid if self.uuid else self.addr}.")
