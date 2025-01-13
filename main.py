from node import Node
import time
import numpy as np
from message import Message
import sys

ip = "127.0.0.1"

def main():
    print(f"Starting node with on port {sys.argv[1]}")
    node = Node(ip, int(sys.argv[1]))

    while True:
        user_input = input(">")
        user_input = user_input.split(" ")
        if user_input[0] == "m" and len(user_input) >= 3:
            msg = Message(int(user_input[1]), int(user_input[2]), b"")
            if int(user_input[2])> 0:
                msg = Message(int(user_input[1]), int(user_input[2]), user_input[3].encode("utf-8"))
            node.message_peers(msg)
            continue
        if user_input[0] == "lp":
            node.list_peers()
            continue
        if user_input[0] == "c":
            node.connect(ip, int(user_input[1]))
            continue
        if user_input[0] == "l":
            node.leave()
            break

    print("Node Stopped!")


if __name__ == "__main__":
    main()