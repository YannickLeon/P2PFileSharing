from node import Node
import socket
import numpy as np
from message import Message
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
            msg = Message(int(user_input[1]),
                          node.uuid, int(user_input[2]), b"")
            if int(user_input[2]) > 0:
                msg = Message(int(user_input[1]), node.uuid, int(
                    user_input[2]), user_input[3].encode("utf-8"))
            node.message_peers(msg)
            continue
        if user_input[0] == "b" and len(user_input) >= 3:
            msg = Message(int(user_input[1]),
                          node.uuid, int(user_input[2]), b"")
            if int(user_input[2]) > 0:
                msg = Message(int(user_input[1]), node.uuid, int(
                    user_input[2]), user_input[3].encode("utf-8"))
            node.send_broadcast(msg)
            continue
        if user_input[0] == "rf":
            node.register_file(user_input[1])
            continue
        if user_input[0] == "lf":
            node.list_files()
            continue
        if user_input[0] == "df" or user_input[0] == "gf":
            files = node.list_files(user_input[0] == "df")
            try:
                file_input = int(input("select file index:"))
            except Exception:
                continue
            if file_input < 0 or file_input > len(files)-1:
                print("Invalid input!")
                continue
            if user_input[0] == "df":
                node.deregister_file(files[file_input])
                continue
            node.request_file(files[file_input])
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
        if user_input[0] == "elec":
            node.start_election()
            continue
        if user_input[0] == "clock":
            node.print_vector_clock()
            continue
        print("No such command.")

    print("Node Stopped!")


if __name__ == "__main__":
    main()
