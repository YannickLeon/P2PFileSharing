import socket
from queue import Queue
from threading import Thread


from message import Message
from connection import Connection

class Node:
    def __init__(self, ip, port):
        self.peers: list[Connection] = []
        self.q: Queue = Queue()
        self.stop = False

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((ip, port))
        self.sock.listen(1)
        print(f"[i] Server listening on {ip}:{port}.")

        self.connection_thread = Thread(target=self.handle_connections, daemon=True)
        self.connection_thread.start()
        self.message_thread = Thread(target=self.message_handler)
        self.message_thread.start()


    # Accept new connections from peers if no outgoing or incoming connectiosn exist
    #   outgoing: connections which are initiated by this peer
    #   incoming: connections which are accepted by this peer
    def handle_connections(self):
        while not self.stop:
            conn, addr = self.sock.accept()
            if any(peer.addr == addr for peer in self.peers):
                continue
            self.peers.append(Connection("incoming", addr, conn, self.q))
            print(f"[i] New connection by {addr}")


    # establish outgoing connection to peer
    def connect(self, ip, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((ip, port))
        self.peers.append(Connection("outgoing", (ip, port), sock, self.q))
        print(f"[i] New connection with {(ip, port)}")
    

    def disconnect(self, addr):
        for peer in self.peers:
            if peer.addr == addr:
                peer.stop = True
                peer.close()
                

    # notify peers and close all connections
    def leave(self):
        for peer in self.peers:
            #peer.send_message(Message(Message.bytecodes["disconnect"], 0, b""))
            peer.stop = True
            peer.close()
        self.peers.clear()
        self.stop = True
        self.sock.close()


    # send message to all peers
    def message_peers(self, msg: Message):
        for peer in self.peers:
            peer.send_message(msg)

    
    # TODO: implement functionality for each message
    def message_handler(self):
        while not self.stop:
            msg: Message = self.q.get()
            print(f"{self.sock.getsockname()}: {msg}")
        print("Message handler stopped")