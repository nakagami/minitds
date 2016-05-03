#!/usr/bin/env python3
##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016 Hajime Nakagami
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.
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


# -----------------------------------------------------------------------------
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
            self.message = 'Database Error'
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


class UTC(datetime.tzinfo):
    def utcoffset(self, dt):
        return datetime.timedelta(0)

    def tzname(self, dt):
        return "UTC"

    def dst(self, dt):
        return datetime.timedelta(0)


# -----------------------------------------------------------------------------

ISOLATION_LEVEL_READ_UNCOMMITTED = 1
ISOLATION_LEVEL_READ_COMMITTED = 2
ISOLATION_LEVEL_REPEATABLE_READ = 3
ISOLATION_LEVEL_SERIALIZABLE = 4
ISOLATION_LEVEL_SNAPSHOT = 5

# Message type
TDS_SQL_BATCH = 1
TDS_RPC = 3
TDS_TABULAR_RESULT = 4
TDS_ATTENTION_SIGNALE = 6
TDS_BULK_LOAD_DATA = 7
TDS_TRANSACTION_MANAGER_REQUEST = 14
TDS_LOGIN = 16
TDS_PRELOGIN = 18

# Transaction
TM_BEGIN_XACT = 5
TM_COMMIT_XACT = 7
TM_ROLLBACK_XACT = 8

# Token type
TDS_TOKEN_COLMETADATA = 0x81
TDS_TOKEN_ENVCHANGE = 0xE3
TDS_ROW_TOKEN = 0xD1
TDS_DONE_TOKEN = 0xFD

# Column type
IMAGETYPE = 34  # 0x22
TEXTTYPE = 35  # 0x23
SYBVARBINARY = 37  # 0x25
INTNTYPE = 38  # 0x26
SYBVARCHAR = 39  # 0x27
BINARYTYPE = 45  # 0x2D
SYBCHAR = 47  # 0x2F
INT1TYPE = 48  # 0x30
BITTYPE = 50  # 0x32
INT2TYPE = 52  # 0x34
INT4TYPE = 56  # 0x38
DATETIM4TYPE = 58  # 0x3A
FLT4TYPE = 59  # 0x3B
MONEYTYPE = 60  # 0x3C
DATETIMETYPE = 61  # 0x3D
FLT8TYPE = 62  # 0x3E
NTEXTTYPE = 99  # 0x63
SYBNVARCHAR = 103  # 0x67
BITNTYPE = 104  # 0x68
NUMERICNTYPE = 108  # 0x6C
DECIMALNTYPE = 106  # 0x6A
FLTNTYPE = 109  # 0x6D
MONEYNTYPE = 110  # 0x6E
DATETIMNTYPE = 111  # 0x6F
MONEY4TYPE = 122  # 0x7A

INT8TYPE = 127  # 0x7F
BIGCHARTYPE = 175  # 0xAF
BIGVARCHRTYPE = 167  # 0xA7
NVARCHARTYPE = 231  # 0xE7
NCHARTYPE = 239  # 0xEF
BIGVARBINTYPE = 165  # 0xA5
BIGBINARYTYPE = 173  # 0xAD
GUIDTYPE = 36  # 0x24
SSVARIANTTYPE = 98  # 0x62
UDTTYPE = 240  # 0xF0
XMLTYPE = 241  # 0xF1
DATENTYPE = 40  # 0x28
TIMENTYPE = 41  # 0x29
DATETIME2NTYPE = 42  # 0x2a
DATETIMEOFFSETNTYPE = 43  # 0x2b

FIXED_TYPE_MAP = {
    # type_id: (size, precision, scale)}
    INT4TYPE: (4, -1, -1),
    DATETIM4TYPE: (4, -1, -1),
    DATETIMETYPE: (8, -1, -1),
}

_bin_version = b'\x00' + bytes(list(VERSION))

def _min_timezone_offset():
    "time zone offset (minutes)"
    now = time.time()
    return (datetime.datetime.fromtimestamp(now) - datetime.datetime.utcfromtimestamp(now)).seconds // 60

def _bytes_to_bint(b):
    return int.from_bytes(b, byteorder='big')


def _bytes_to_int(b):
    return int.from_bytes(b, byteorder='little', signed=True)


def _bytes_to_uint(b):
    return int.from_bytes(b, byteorder='little', signed=False)


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


def _bytes_to_str(b):
    return b.decode('utf_16_le')


def _convert_time(b, precision):
    v = _bytes_to_uint(b)
    v *= 10 ** (7 - precision)
    nanoseconds = v * 100
    hours = nanoseconds // 1000000000 // 60 // 60
    nanoseconds -= hours * 60 * 60 * 1000000000
    minutes = nanoseconds // 1000000000 // 60
    nanoseconds -= minutes * 60 * 1000000000
    seconds = nanoseconds // 1000000000
    nanoseconds -= seconds * 1000000000
    return datetime.time(hours, minutes, seconds, nanoseconds // 1000)


def _convert_date(b):
    return datetime.datetime(1, 1, 1) + datetime.timedelta(days=_bytes_to_uint(b))


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

    packet_size = pos + (len(client_name) + len(app_name) + len(host) + len(user) + len(password) + len(lib_name) + len(language) + len(database) + len(db_file)) * 2

    buf = b''
    buf += _int_to_4bytes(packet_size)
    buf += b'\x04\x00\x00\x74'   # TDS 7.4
    buf += _int_to_4bytes(4096)
    buf += _bin_version
    buf += _int_to_4bytes(os.getpid())
    buf += _int_to_4bytes(0)            # connection id
    buf += bytes([
        0x20 | 0x40 | 0x80,  # OptionFlags1 USE_DB_ON|INIT_DB_FATAL|SET_LANG_ON
        0x02,                # OptionFlags2 ODBC_ON
        0,                   # TypeFlags
        0x80,                # OptionFlags3 UNKNOWN_COLLATION_HANDLING
    ])
    buf += _int_to_4bytes(_min_timezone_offset())
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


def get_trans_request_bytes(req, isolation_level, transaction_id):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += _int_to_2bytes(2)
    buf += transaction_id
    buf += _int_to_4bytes(1)        # request count
    buf += _int_to_2bytes(req)
    buf += bytes([isolation_level])
    buf += b'\00'
    return buf


def get_query_bytes(query, transaction_id):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += _int_to_2bytes(2)
    buf += transaction_id
    buf += _int_to_4bytes(1)        # request count
    buf += _str_to_bytes(query)

    return buf


def parse_transaction_id(data):
    "return transaction_id"
    assert data[0] == TDS_TOKEN_ENVCHANGE
    assert data[3] == 8  # Begin Transaction
    assert data[4] == 8  # transaction id size
    return data[5:13]    # transaction id


def _parse_description_type(data):
    size = precision = scale = -1
    user_type = _bytes_to_uint(data[:4])
    flags = _bytes_to_uint(data[4:6])
    type_id = data[6]

    fix_type = FIXED_TYPE_MAP.get(type_id)
    if fix_type:
        size, precision, scale = fix_type
        data = data[7:]
    elif type_id in (NUMERICNTYPE, DECIMALNTYPE):
        size = data[7]
        precision = data[8]
        scale = data[9]
        data = data[10:]
    elif type_id in (NVARCHARTYPE,):
        size = _bytes_to_uint(data[7:9])
        # skip collation
        data = data[9+5:]
    elif type_id in (DATETIME2NTYPE, DATETIMEOFFSETNTYPE,):
        precision = data[7]
        data = data[8:]
    else:
        print("Unknown type_id:", type_id)

    ln = data[0]
    name = _bytes_to_str(data[1:1+ln*2])
    data = data[1+ln*2:]
    return type_id, name, size, precision, scale, True, data


def _parse_description(data):
    assert data[0] == TDS_TOKEN_COLMETADATA
    num_cols = _bytes_to_int(data[1:3])
    if num_cols == -1:
        return []

    description = []
    data = data[3:]
    for i in range(num_cols):
        type_id, name, size, precision, scale, null_ok, data = _parse_description_type(data)
        description.append((name, type_id, size, size, precision, scale, null_ok))
    return description, data


def _parse_row(description, data):
    row = []
    for _, type_id, ln, _, precision, scale, _ in description:
        if type_id in (INT4TYPE,):
            v = _bytes_to_int(data[:ln])
            data = data[ln:]
        elif type_id in (NUMERICNTYPE, DECIMALNTYPE):
            ln = data[0]
            data = data[1:]
            positive = data[0]
            v = decimal.Decimal(_bytes_to_int(data[1:ln]))
            if not positive:
                v *= -1
            v /= 10 ** scale
            data = data[ln:]
        elif type_id in (NVARCHARTYPE,):
            ln = _bytes_to_int(data[:2])
            if ln < 0:
                v = None
            else:
                v = _bytes_to_str(data[2:ln+2])
                data = data[ln+2:]
        elif type_id in (DATETIM4TYPE, DATETIMETYPE,):
            d = _bytes_to_int(data[:ln//2])
            t = _bytes_to_int(data[ln//2:ln])
            data = data[ln:]
            ms = int(round(t % 300 * 10 / 3.0))
            secs = t // 300
            v = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=d, seconds=secs, milliseconds=ms)
        elif type_id in (DATETIME2NTYPE, ):
            ln = data[0]
            data = data[1:]
            if ln == 0:
                v = None
            else:
                t = _convert_time(data[:ln-3], precision)
                d = _convert_date(data[ln-3:ln])
                v = datetime.datetime.combine(d, t)
                data = data[ln:]
        elif type_id in (DATETIMEOFFSETNTYPE, ):
            ln = data[0]
            data = data[1:]
            if ln == 0:
                v = None
            else:
                t = _convert_time(data[:ln-5], precision)
                d = _convert_date(data[ln-5:ln-2])
                tz_offset = _bytes_to_int(data[ln-2:ln])
                v = datetime.datetime.combine(d, t) + datetime.timedelta(minutes=_min_timezone_offset()+tz_offset)
                v = v.replace(tzinfo=UTC())
                data = data[ln:]
        else:
            print("parse_row() Unknown type", type_id)
        row.append(v)
    return row, data


# -----------------------------------------------------------------------------

def escape_parameter(self, v):
    if v is None:
        return 'NULL'

    t = type(v)
    if t == str:
        return u"'" + v.replace(u"'", u"''") + u"'"
    elif t == bool:
        return u"TRUE" if v else u"FALSE"
    elif t == time.struct_time:
        return u'%04d-%02d-%02d %02d:%02d:%02d' % (
            v.tm_year, v.tm_mon, v.tm_mday, v.tm_hour, v.tm_min, v.tm_sec)
    elif t == datetime.datetime:
        return "timestamp '" + v.isoformat() + "'"
    elif t == datetime.date:
        return "date '" + str(v) + "'"
    elif t == datetime.timedelta:
        return u"interval '" + str(v) + "'"
    elif t == int or t == float or (PY2 and t == long):
        return str(v)
    elif t == decimal.Decimal:
        return "decimal '" + str(v) + "'"
    else:
        return "'" + str(v) + "'"

class Cursor(object):
    def __init__(self, connection):
        self.connection = connection
        self.description = []
        self._rows = []
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
        self.args = args
        if args:
            escaped_args = tuple(escape_parameter(arg).replace('%', '%%') for arg in args)
            query = query.replace('%', '%%').replace('%%s', '%s')
            query = query % escaped_args
            query = query.replace('%%', '%')
        self.query = query
        self.description, self._rows = self.connection._execute(query)

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
            row = self._rows[0]
            self._rows[1:]
        else:
            row = None
        return row

    def fetchmany(self, size=1):
        rs = []
        for i in range(size):
            r = self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    def fetchall(self):
        rows = self._rows
        self._rows = []
        return rows

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
    def __init__(self, user, password, database, host, isolation_level, port, lcid, timeout):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.isolation_level = isolation_level
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
        self.begin()


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

        if self.timeout is not None:
            self.sock.settimeout(float(self.timeout))

    def is_connect(self):
            return bool(self.sock)

    def cursor(self):
        return Cursor(self)

    def _execute(self, query):
        self._send_message(TDS_SQL_BATCH, True, get_query_bytes(query, self.transaction_id))
        _, _, _, data = self._read_response_packet()
        description, data = _parse_description(data)
        rows = []
        while data[0] == TDS_ROW_TOKEN:
            row, data = _parse_row(description, data[1:])
            rows.append(row)
        assert data[0] == TDS_DONE_TOKEN
        return description, rows


    def set_autocommit(self, autocommit):
        self.autocommit = autocommit

    def begin(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, True, get_trans_request_bytes(TM_BEGIN_XACT, self.isolation_level, b'\x00'*8))
        _, _, _, data = self._read_response_packet()
        self.transaction_id = parse_transaction_id(data)

    def commit(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, True, get_trans_request_bytes(TM_COMMIT_XACT, self.isolation_level, self.transaction_id))
        self._read_response_packet()
        self.begin()

    def rollback(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, True, get_trans_request_bytes(TM_ROLLBACK_XACT, self.isolation_level, self.transaction_id))
        self._read_response_packet()
        self.begin()

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None


def connect(host, database, user, password, isolation_level=0, port=14333, lcid=1033, timeout=None):
    return Connection(user, password, database, host, isolation_level, port, lcid, timeout)
