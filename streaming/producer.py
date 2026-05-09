import socket
import time
import json
import pandas as pd

# Load a small portion of cleaned review data
df = pd.read_parquet("./processed/review_clean.parquet")
df = df.head(50)  # send only 50 rows for demo

HOST = "localhost"
PORT = 9999

# Create socket stream
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen(1)

print("🚀 Producer waiting for connection...")
conn, addr = server_socket.accept()
print(f"✔ Connected to {addr}")

# Send reviews one by one
for idx, row in df.iterrows():
    data = row.to_dict()

    # Convert datetime objects to strings
    for key, value in data.items():
        if hasattr(value, "isoformat"):
            data[key] = value.isoformat()  # convert to 'YYYY-MM-DDTHH:MM:SS'

    message = json.dumps(data)
    conn.sendall(message.encode() + b"\n")

    print("Sent:", message)
    time.sleep(0.5)  # simulate delay

conn.close()
