import socket

def receive_broadcast(port=12345):
    # Create a UDP socket
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    
    # Set socket option to allow receiving broadcast messages
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
    
    # Bind to the broadcast address and port (use 0.0.0.0 for any address on the local network)
    sock.bind(('0.0.0.0', port))  # Listening on all interfaces (including loopback)
    print(f"Listening for broadcasts on port {port}...")
    
    while True:
        # Receive data from the socket (blocking call)
        message, address = sock.recvfrom(1024)  # buffer size of 1024 bytes
        print(f"Received message: {message.decode()} from {address}")
    
    sock.close()

# Start listening for broadcast messages
receive_broadcast()
