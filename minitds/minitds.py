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
# https://github.com/nakagami/minitds/

import sys
import os
import threading
import socket
import decimal
import datetime
import time
import collections
import binascii
import uuid

VERSION = (0, 0, 1)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'

DEBUG = True

def DEBUG_OUTPUT(s):
    print(s, end=' \n', file=sys.stderr)

#-----------------------------------------------------------------------------
Date = datetime.date
Time = datetime.time
TimeDelta = datetime.timedelta
Timestamp = datetime.datetime


def Binary(b):
    return bytearray(b)


class DBAPITypeObject:
    def __init__(self, *values):
        self.values = values

    def __cmp__(self, other):
        if other in self.values:
            return 0
        if other < self.values:
            return 1
        else:
            return -1


STRING = DBAPITypeObject(str)
BINARY = DBAPITypeObject(bytes)
NUMBER = DBAPITypeObject(int, decimal.Decimal)
DATETIME = DBAPITypeObject(datetime.datetime, datetime.date, datetime.time)
DATE = DBAPITypeObject(datetime.date)
TIME = DBAPITypeObject(datetime.time)
ROWID = DBAPITypeObject()


class Error(Exception):
    def __init__(self, *args):
        if len(args) > 0:
            self.message = args[0]
        else:
            self.message = b'Database Error'
        super(Error, self).__init__(*args)

    def __str__(self):
        return self.message

    def __repr__(self):
        return self.message


class Warning(Exception):
    pass


class InterfaceError(Error):
    pass


class DatabaseError(Error):
    pass


class DisconnectByPeer(Warning):
    pass


class InternalError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'InternalError')


class OperationalError(DatabaseError):
    pass


class ProgrammingError(DatabaseError):
    pass


class IntegrityError(DatabaseError):
    pass


class DataError(DatabaseError):
    pass


class NotSupportedError(DatabaseError):
    def __init__(self):
        DatabaseError.__init__(self, 'NotSupportedError')

#-----------------------------------------------------------------------------
# Message type
TDS_SQL_BATCH = 1
TDS_RPC = 3
TDS_TABULAR_RESULT = 4
TDS_ATTENTION_SIGNALE = 6
TDS_BULK_LOAD_DATA = 7
TDS_TRANSACTION_MANAGER_REQUEST = 14
TDS_LOGIN = 16
TDS_PRELOGIN = 18

TM_BEGIN_XACT = 5
TM_COMMIT_XACT = 7
TM_ROLLBACK_XACT = 8

_bin_version = b'\x00' + bytes(list(VERSION))

def _bytes_to_bint(b):
    return int.from_bytes(b, byteorder='big')


def _bint_to_2bytes(v):
    return v.to_bytes(2, byteorder='big')


def _bint_to_4bytes(v):
    return v.to_bytes(4, byteorder='big')


def _int_to_2bytes(v):
    return v.to_bytes(2, byteorder='little')


def _int_to_4bytes(v):
    return v.to_bytes(4, byteorder='little')


def _int_to_8bytes(v):
    return v.to_bytes(8, byteorder='little')


def _str_to_bytes(s):
    return s.encode('utf_16_le')


def get_prelogin_bytes(instance_name="MSSQLServer"):
    instance_name = instance_name.encode('ascii') + b'\00'
    pos = 26
    # version
    buf = b'\x00' + _bint_to_2bytes(pos) + _bint_to_2bytes(6)
    pos += 6
    # encryption
    buf += b'\x01' + _bint_to_2bytes(pos) + _bint_to_2bytes(1)
    pos += 1
    # instance name
    buf += b'\x02' + _bint_to_2bytes(pos) + _bint_to_2bytes(len(instance_name))
    pos += len(instance_name)
    # thread id
    buf += b'\x03' + _bint_to_2bytes(pos) + _bint_to_2bytes(4)
    pos += 4
    # MARS
    buf += b'\x04' + _bint_to_2bytes(pos) + _bint_to_2bytes(1)
    pos += 1
    # terminator
    buf += b'\xff'

    assert len(buf) == 26

    buf += _bin_version + _bint_to_2bytes(0)
    buf += b'\x02'  # not encryption
    buf += instance_name
    buf += _bint_to_4bytes(0)   # TODO: thread id
    buf += b'\x00'              # not use MARS

    return buf


def get_login_bytes(host, user, password, database, lcid):
    pos = 94
    client_name = socket.gethostname()[:128]
    app_name = "minitds"
    lib_name = "minitds"
    language = ""                       # server default
    db_file = ""
    now = time.time()
    min_offset = (datetime.datetime.fromtimestamp(now) - datetime.datetime.utcfromtimestamp(now)).seconds // 60

    packet_size = pos + (len(client_name) + len(app_name) + len(host) + len(user) + len(password) + len(lib_name) + len(language) + len(database) + len(db_file)) * 2

    buf = b''
    buf += _int_to_4bytes(packet_size)
    buf += b'\x04\x00\x00\x74'   # TDS 7.4
    buf += _int_to_4bytes(4096)
    buf += _bin_version
    buf += _int_to_4bytes(os.getpid())
    buf += _int_to_4bytes(0)            # connection id
    buf += bytes([
        0x20 | 0x40 | 0x80, # OptionFlags1 USE_DB_ON|INIT_DB_FATAL|SET_LANG_ON
        0x02,               # OptionFlags2 ODBC_ON
        0,                  # TypeFlags
        0x80,               # OptionFlags3 UNKNOWN_COLLATION_HANDLING
    ])
    buf += _int_to_4bytes(min_offset)   # time zone offset
    buf += _int_to_4bytes(lcid)

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(client_name))
    pos += len(client_name) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(user))
    pos += len(user) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(password))
    pos += len(password) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(app_name))
    pos += len(app_name) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(host))
    pos += len(host) * 2

    # reserved
    buf += _int_to_2bytes(0)
    buf += _int_to_2bytes(0)

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(lib_name))
    pos += len(lib_name) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(language))
    pos += len(language) * 2

    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(database))
    pos += len(database) * 2

    # Client ID
    buf += uuid.getnode().to_bytes(6, 'big')

    # authenticate
    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(0)

    # db file
    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(len(db_file))
    pos += len(db_file) * 2

    # new password
    buf += _int_to_2bytes(pos)
    buf += _int_to_2bytes(0)
    # sspi
    buf += _int_to_4bytes(0)

    buf += _str_to_bytes(client_name)
    buf += _str_to_bytes(user)
    buf += bytes([((c << 4) & 0xff | (c >> 4)) ^ 0xa5 for c in _str_to_bytes(password)])
    buf += _str_to_bytes(app_name)
    buf += _str_to_bytes(host)
    buf += _str_to_bytes(lib_name)
    buf += _str_to_bytes(language)
    buf += _str_to_bytes(database)
    buf += _str_to_bytes(db_file)
    buf += _str_to_bytes('')                # new password

    return buf


def get_trans_request_bytes(req, isolation_level, trans):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += b'\x02'
    buf += _int_to_8bytes(trans)
    buf += _int_to_4bytes(1)        # request count
    buf += bytes([req])
    buf += bytes([isolation_level])
    buf += b'\00'
    return buf


def get_query_bytes(query, trans):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += b'\x02'
    buf += _int_to_8bytes(trans)
    buf += _int_to_4bytes(1)        # request count
    buf += _str_to_bytes(query)

    return buf


#-----------------------------------------------------------------------------


class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self._rows = collections.deque()
        self._rowcount = 0
        self.arraysize = 1
        self.query = None

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def callproc(self, procname, args=()):
        raise NotSupportedError()

    def nextset(self, procname, args=()):
        raise NotSupportedError()

    def setinputsizes(sizes):
        pass

    def setoutputsize(size, column=None):
        pass

    def execute(self, query, args=()):
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")
        self.description = []
        self._rows.clear()
        self.args = args
        if args:
            escaped_args = tuple(
                self.connection.escape_parameter(arg).replace(u'%', u'%%') for arg in args
            )
            query = query.replace(u'%', u'%%').replace(u'%%s', u'%s')
            query = query % escaped_args
            query = query.replace(u'%%', u'%')
        self.query = query
        self.connection.execute(query)

    def executemany(self, query, seq_of_params):
        rowcount = 0
        for params in seq_of_params:
            self.execute(query, params)
            rowcount += self._rowcount
        self._rowcount = rowcount

    def fetchone(self):
        if not self.connection or not self.connection.is_connect():
            raise OperationalError("Lost connection")
        if len(self._rows):
            return self._rows.popleft()
        return None

    def fetchmany(self, size=1):
        rs = []
        for i in range(size):
            r = self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    def fetchall(self):
        r = list(self._rows)
        self._rows.clear()
        return r

    def close(self):
        self.connection = None

    @property
    def rowcount(self):
        return self._rowcount

    @property
    def closed(self):
        return self.connection is None or not self.connection.is_connect()

    def __iter__(self):
        return self

    def __next__(self):
        r = self.fetchone()
        if not r:
            raise StopIteration()
        return r

    def next(self):
        return self.__next__()


class Connection(object):
    def __init__(self, user, password, database, host, port, lcid, timeout):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.port = port
        self.lcid = lcid
        self.timeout = timeout
        self.autocommit = False
        self._packet_id = 0
        self._open()

        self._send_message(TDS_PRELOGIN, True, get_prelogin_bytes())
        self._read_response_packet()
        self._send_message(TDS_LOGIN, True, get_login_bytes(self.host, self.user, self.password, self.database, self.lcid))
        self._read_response_packet()

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def _read(self, ln):
        if not self.sock:
            raise OperationalError("Lost connection")
        r = b''
        while len(r) < ln:
            b = self.sock.recv(ln-len(r))
            if not b:
                raise OperationalError("Can't recv packets")
            r += b
        return r

    def _write(self, b):
        if not self.sock:
            raise OperationalError("Lost connection")
        n = 0
        while (n < len(b)):
            n += self.sock.send(b[n:])

    def _read_response_packet(self):
        b = self._read(8)
        t = b[0]
        status = b[1]
        ln = _bytes_to_bint(b[2:4]) - 8
        spid = _bytes_to_bint(b[4:6])

        return t, status, spid, self._read(ln)

    def _send_message(self, message_type, is_final, buf):
        self._write(
            bytes([message_type, 1 if is_final else 0]) +
            _bint_to_2bytes(8 + len(buf)) +
            _bint_to_2bytes(0) +
            bytes([self._packet_id, 0]) +
            buf
        )
        self._packet_id = (self._packet_id + 1) % 256

    def _open(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        if DEBUG:
            DEBUG_OUTPUT("socket %s:%d" % (self.host, self.port))

        if self.timeout is not None:
            self.sock.settimeout(float(self.timeout))

    def is_connect(self):
            return bool(self.sock)

    def cursor(self):
        return Cursor(self)

    def execute(self, query):
        self._send_message(TDS_SQL_BATCH, True, get_query_bytes(query, 0))
        self._read_response_packet()

    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def begin(self):
        pass

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        if DEBUG:
            DEBUG_OUTPUT('Connection::close()')
        if self.sock:
            self.sock.close()
            self.sock = None


def connect(host, database, user, password, port=14333, lcid=1033, timeout=None):
    return Connection(user, password, database, host, port, lcid, timeout)
