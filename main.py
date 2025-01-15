from node import Node
import socket
import numpy as np
from message import Message
import sys

# might be needed to receive broadcasts: sudo ufw allow 9000/udp

def get_ip():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(('8.8.8.8', 0))
    ip = s.getsockname()[0]
    s.close()
    return ip


def main():
    print("Starting node...")
    ip = get_ip()
    node: Node = Node(ip)

    while True:
        user_input = input(">")
        user_input = user_input.split(" ")
        if user_input[0] == "m" and len(user_input) >= 3:
            msg = Message(int(user_input[1]), node.uuid, int(user_input[2]), b"")
            if int(user_input[2])> 0:
                msg = Message(int(user_input[1]), node.uuid, int(user_input[2]), user_input[3].encode("utf-8"))
            node.message_peers(msg)
            continue
        if user_input[0] == "b" and len(user_input) >= 3:
            msg = Message(int(user_input[1]), node.uuid, int(user_input[2]), b"")
            if int(user_input[2])> 0:
                msg = Message(int(user_input[1]), node.uuid, int(user_input[2]), user_input[3].encode("utf-8"))
            node.send_broadcast(msg)
            continue
        if user_input[0] == "lp":
            node.list_peers()
            continue
        if user_input[0] == "c":
            node.connect((ip, int(user_input[1])))
            continue
        if user_input[0] == "l":
            node.leave()
            break
        print("No such command.")

    print("Node Stopped!")


if __name__ == "__main__":
    main()