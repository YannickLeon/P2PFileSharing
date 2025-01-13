import select
import socket
from queue import Queue
import queue
from threading import Thread


from message import Message
from connection import Connection

class Node:
    def __init__(self, ip, port):
        self.peers: list[Connection] = []
        self.q: Queue = Queue()
        self.stop = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setblocking(0)
        self.sock.bind((ip, port))
        self.sock.listen(1)
        print(f"[i] Server listening on {ip}:{port}.")

        self.connection_thread = Thread(target=self.handle_connections)
        self.connection_thread.start()
        self.message_thread = Thread(target=self.message_handler)
        self.message_thread.start()


    # Accept new connections from peers if no outgoing or incoming connectiosn exist
    #   outgoing: connections which are initiated by this peer
    #   incoming: connections which are accepted by this peer
    def handle_connections(self):
        while not self.stop:
            try:
                ready = select.select([self.sock], [], [], 0.5)
                if not ready[0]:
                    continue
                conn, addr = self.sock.accept() # add timeout to avoid blocking or forcefully stop thread
                if any(peer.addr == addr for peer in self.peers):
                    continue
                self.peers.append(Connection("incoming", addr, conn, self.q))
                print(f"[i] New connection by {addr}")
            except:
                continue
        print("[i] Stopped connection thread.")


    # establish outgoing connection to peer
    def connect(self, ip, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        self.peers.append(Connection("outgoing", (ip, port), sock, self.q))
        print(f"[i] New connection with {(ip, port)}")
    

    def disconnect(self, addr):
        for peer in self.peers:
            if peer.addr == addr:
                peer.close()
                self.peers.remove(peer)
                print(f"[i] Disconnected peer {addr}")
                

    # notify peers and close all connections
    def leave(self):
        for peer in self.peers:
            peer.send_message(Message(Message.bytecodes["disconnect"], 0, b""))
            peer.close()
        self.peers.clear()
        self.stop = True
        self.message_thread.join()
        self.connection_thread.join()
        self.sock.close()


    # send message to all peers
    def message_peers(self, msg: Message):
        for peer in self.peers:
            peer.send_message(msg)

    
    # TODO: implement functionality for each message
    def message_handler(self):
        while not self.stop:
            try:
                msg: Message = self.q.get(block=False)
                if msg.control_byte == msg.bytecodes["init"]:
                    pass
                if msg.control_byte == msg.bytecodes["disconnect"]:
                    self.disconnect(msg.sender)
                    continue
                print(f"{self.sock.getsockname()}: {msg}")
            except queue.Empty:
                pass
            except Exception as e:
                print(f"[!] Error in message_handler: {e}")
        print("[i] Stopped message handler")

    
    def list_peers(self):
        print("*** Peers **************")
        for peer in self.peers:
            print(f"\t{peer.addr}")
        print("************************")