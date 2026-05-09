import socket

HOST = "localhost"
PORT = 9999

# Connect to producer
client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

print("📥 Consumer connected. Receiving streaming data...\n")

buffer = ""

while True:
    data = client_socket.recv(1024).decode()
    if not data:
        break

    buffer += data
    while "\n" in buffer:
        line, buffer = buffer.split("\n", 1)
        print("Received:", line)
