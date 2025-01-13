import socket 
import numpy as np
from threading import Thread
from queue import Queue
import select

from message import Message

class Connection():
    def __init__(self, mode: str, addr: tuple[str, int], sock: socket.socket, q: Queue):
        self.mode = mode
        self.addr = addr
        self.sock = sock
        sock.setblocking(False)
        self.q = q

        self.stop = False
        
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
                data += self.sock.recv(1024 * 64)
                if (
                    not data
                    or len(data) < 5
                    or (len(data) - 5) < int.from_bytes(data[1:5], byteorder="big")
                ):
                    continue
                self.q.put(
                    Message(
                        np.byte(data[0]),
                        int.from_bytes(data[1:5], byteorder="big"),
                        data[5 : 5 + int.from_bytes(data[1:5], byteorder="big")],
                        self.addr,
                    )
                )
                data = data[5 + int.from_bytes(data[1:5], byteorder="big"):]
            except socket.error:
                print("[!] Connection error while receiving data.")
                break
        print(f"[i] listener stopped for {self.addr}.")

    def send_message(self, msg: Message):
        try:
            self.sock.sendall(msg.to_bytes())
        except socket.error as e:
            print(f"[!] Error while sending message: {e.strerror}")
    
    def close(self):
        self.stop = True
        self.listener_thread.join()
        self.sock.close()
        print(f"[i] Closed connection to {self.addr}.")