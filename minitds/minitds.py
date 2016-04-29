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
import socket
import decimal
import datetime
import time
import collections
import binascii

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

bin_version = b'\x00' + bytes(list(VERSION))

def _bytes_to_bint(b):
    return int.from_bytes(b, byteorder='big')


def _bint_to_2bytes(v):
    return v.to_bytes(2, byteorder='big')


def _bint_to_4bytes(v):
    return v.to_bytes(4, byteorder='big')


def get_prelogin_bytes(instance_name="MSSQLServer"):
    instance_name = instance_name.encode('ascii') + b'\00'
    pos = 26
    # version
    buf = b'\x00' + _bint_to_2bytes(POS) + _bint_to_2bytes(6)
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

    buf += bin_version + _bint_to_2byte(0)
    buf += b'\x02'  # not encryption
    buf += instance_name
    buf += _bint_to_4byte(0)    # TODO: thread id
    buf += b'\x00'              # not use MARS

    return buf


def get_login_bytes(user, password, database, lcid):
    app_name = "minitds"
    pos = 94



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
        self.connection.execute(query, self)

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
        self._packet_no = 0
        self._open()

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

    def _open(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        if DEBUG:
            DEBUG_OUTPUT("socket %s:%d" % (self.host, self.port))

        if self.timeout is not None:
            self.sock.settimeout(float(self.timeout))

    def send_message(self, message_type, is_final, buf):
        self._write(
            bytes([message_type, 1 if is_final else 0]) +
            _bint_to_2bytes(len(buf)) +
            _bint_to_2bytes(0) +
            bytes([self._packet_no]) +
            buf
        )
        self._packet_no = (self._packet_no + 1) % 256

    def cursor(self):
        return Cursor(self)

    def execute(self, query, obj=None):
        pass

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


def connect(host, user, password, database='', port=14333, lcid=1033, timeout=None):
    return Connection(user, password, database, host, port, lcid, timeout)
