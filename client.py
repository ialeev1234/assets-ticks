import socket

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(('', 8080))

sock.send(b'{"action": "subscribe", "message": {"assetId": 2}}')

data = sock.recv(256 * 1024)
print(data)

n = 5
while n:
    n -= 1
    data = sock.recv(1024)
    print(data)

sock.send(b'{"action": "subscribe", "message": {"assetId": 3}}')

data = sock.recv(256 * 1024)
print(data)

n = 5
while n:
    n -= 1
    data = sock.recv(1024)
    print(data)

sock.close()
