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


def proxy_wire(server_name, server_port, listen_host, listen_port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((listen_host, listen_port))
    sock.listen(1)
    client_sock, addr = sock.accept()
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.connect((server_name, server_port))

    while True:
        client_head = client_sock.recv(8)
        if len(client_head) == 0:
            break
        t = client_head[0]
        status = client_head[1]
        ln = int.from_bytes(client_head[2:4], byteorder='big')
        spid = int.from_bytes(client_head[4:6], byteorder='big')
        client_body = client_sock.recv(ln)
        server_sock.send(client_head)
        server_sock.send(client_body)
        print("<<%s:%d, len=%d spid=%d data=%s" % (TDS_NAME[t], status, len(client_body), spid, binascii.b2a_hex(client_body).decode('ascii')))

        server_head = server_sock.recv(8)
        t = server_head[0]
        status = server_head[1]
        ln = int.from_bytes(server_head[2:4], byteorder='big')
        spid = int.from_bytes(server_head[4:6], byteorder='big')
        server_body = server_sock.recv(ln)
        client_sock.send(server_head)
        client_sock.send(server_body)
        print(">>%s:%d, len=%d spid=%d data=%s" % (TDS_NAME[t], status, len(server_body), spid, binascii.b2a_hex(server_body).decode('ascii')))


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
