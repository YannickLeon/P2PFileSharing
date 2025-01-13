import socket

def send_broadcast(message, port=12345):
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    
    # Set the socket option to allow broadcasting
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    
    # Broadcast message to all devices on the local network (255.255.255.255)
    broadcast_address = ('<broadcast>', port)  # <broadcast> is a special address for broadcast
    sock.sendto(message.encode(), broadcast_address)
    print(f"Sent broadcast message: {message}")
    
    sock.close()

# Send a broadcast message
send_broadcast("Hello, this is a broadcast message!")
