#!/usr/bin/env python3
##############################################################################
#The MIT License (MIT)
#
#Copyright (c) 2016 Hajime Nakagami
#
#Permission is hereby granted, free of charge, to any person obtaining a copy
#of this software and associated documentation files (the "Software"), to deal
#in the Software without restriction, including without limitation the rights
#to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#copies of the Software, and to permit persons to whom the Software is
#furnished to do so, subject to the following conditions:
#
#The above copyright notice and this permission notice shall be included in all
#copies or substantial portions of the Software.
#
#THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#SOFTWARE.
##############################################################################
import sys
import socket
import binascii

TDS_NAME = {
    1: "TDS_SQL_BATCH",
    3: "TDS_RPC",
    4: "TDS_TABULAR_RESULT",
    6: "TDS_ATTENTION_SIGNALE",
    7: "TDS_BULK_LOAD_DATA",
    14: "TDS_TRANSACTION_MANAGER_REQUEST",
    16: "TDS_LOGIN",
    18: "TDS_PRELOGIN",
}


def asc_dump(s):
    r = ''
    for c in s:
        r += chr(c) if (c >= 32 and c < 128) else '.'
    if r:
        print('\t[' + r + ']')

def recv_from_sock(sock, nbytes):
    n = nbytes
    recieved = b''
    while n:
        bs = sock.recv(n)
        recieved += bs
        n -= len(bs)
    return recieved


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)
    client_sock, addr = sock.accept()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.connect((server_name, server_port))

    while True:
        client_head = recv_from_sock(client_sock, 8)
        t = client_head[0]
        status = client_head[1]
        ln = int.from_bytes(client_head[2:4], byteorder='big')
        spid = int.from_bytes(client_head[4:6], byteorder='big')
        client_body = recv_from_sock(client_sock, ln-8)
        server_sock.send(client_head)
        server_sock.send(client_body)
        print("<<%s:%d, len=%d spid=%d %s data=%s" % (TDS_NAME[t], status, len(client_body), spid, binascii.b2a_hex(client_head[6:]).decode('ascii'), binascii.b2a_hex(client_body).decode('ascii')))
        if TDS_NAME[t] == 'TDS_SQL_BATCH':
            asc_dump(client_body)

        server_head = recv_from_sock(server_sock, 8)
        t = server_head[0]
        status = server_head[1]
        ln = int.from_bytes(server_head[2:4], byteorder='big')
        spid = int.from_bytes(server_head[4:6], byteorder='big')
        server_body = recv_from_sock(server_sock, ln-8)
        client_sock.send(server_head)
        client_sock.send(server_body)
        print(">>%s:%d, len=%d spid=%d data=%s" % (TDS_NAME[t], status, len(server_body), spid, binascii.b2a_hex(server_body).decode('ascii')))
        asc_dump(server_body)


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage : ' + sys.argv[0] + ' server[:port] [listen_host:]listen_port')
        sys.exit()

    server = sys.argv[1].split(':')
    server_name = server[0]
    if len(server) == 1:
        server_port = 3050
    else:
        server_port = int(server[1])

    listen = sys.argv[2].split(':')
    if len(listen) == 1:
        listen_host = 'localhost'
        listen_port = int(listen[0])
    else:
        listen_host = listen[0]
        listen_port = int(listen[1])

    proxy_wire(server_name, server_port, listen_host, listen_port)
