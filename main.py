from node import Node
import time
import numpy as np
from message import Message

ip = "127.0.0.1"

def main():
    node1: Node = Node(ip, 1500)
    node2: Node = Node(ip, 1501)
    
    node1.connect(ip, 1501)
    time.sleep(1)
    print("messages...")
    node1.message_peers(Message(np.byte(1), 0, b""))
    time.sleep(1)
    node2.message_peers(Message(np.byte(1), 0, b""))
    time.sleep(1)
    node1.leave()
    node2.leave()


if __name__ == "__main__":
    main()