#!/usr/bin/env python3
##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016-2017 Hajime Nakagami
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
import socket
import decimal
import datetime
import time
import binascii
import uuid
import struct
import ssl
import threading
from argparse import ArgumentParser

VERSION = (0, 4, 2)
__version__ = '%s.%s.%s' % VERSION
apilevel = '2.0'
threadsafety = 1
paramstyle = 'format'


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
BUFSIZE = 4096

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

# Environment type
TDS_ENV_BEGINTRANS = 8

# Token type
TDS_TOKEN_COLMETADATA = 0x81
TDS_ERROR_TOKEN = 0xAA
TDS_TOKEN_ENVCHANGE = 0xE3
TDS_ROW_TOKEN = 0xD1
TDS_NBCROW_TOKEN = 0xD2
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
    return (datetime.datetime(1, 1, 1) + datetime.timedelta(days=_bytes_to_uint(b))).date()


def get_prelogin_bytes(use_ssl, instance_name):
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
    if use_ssl is None:
        buf += b'\x03'  # ENCRYPT_REQ
    elif use_ssl:
        buf += b'\x01'  # ENCRYPT_ON
    else:
        buf += b'\x02'  # ENCRYPT_NOT_SUP

    buf += instance_name
    buf += _bint_to_4bytes(threading.get_ident())   # thread id
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
    buf += _int_to_4bytes(BUFSIZE)
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


def get_trans_request_bytes(transaction_id, req, isolation_level):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += _int_to_2bytes(2)
    buf += transaction_id
    buf += _int_to_4bytes(1)        # request count
    buf += _int_to_2bytes(req)
    buf += bytes([isolation_level])
    buf += b'\00'
    return buf


def get_sql_batch_bytes(transaction_id, query):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += _int_to_2bytes(2)
    buf += transaction_id
    buf += _int_to_4bytes(1)        # request count

    buf += _str_to_bytes(query)

    return buf


def get_rpc_request_bytes(connection, procname, params=[]):
    buf = _int_to_4bytes(22)
    buf += _int_to_4bytes(18)
    buf += _int_to_2bytes(2)
    buf += connection.transaction_id
    buf += _int_to_4bytes(1)        # request count

    buf += _int_to_2bytes(len(procname))
    buf += _str_to_bytes(procname)
    buf += bytes([0x00, 0x00])      # OptionFlags

    for p in params:
        buf += bytes([0, 0])     # name="", StatusFlags
        if p is None:
            buf += bytes([INTNTYPE, 2])
            buf += bytes([0])
        elif isinstance(p, int):
            buf += bytes([INTNTYPE, 4])
            buf += bytes([4]) + p.to_bytes(4, byteorder='little')
        elif isinstance(p, str):
            ln = len(p) * 2
            buf += bytes([NCHARTYPE]) + ln.to_bytes(2, byteorder='little')
            buf += _int_to_2bytes(connection.lcid) + bytes([0, 0, 0])
            buf += ln.to_bytes(2, byteorder='little') + _str_to_bytes(p)
        elif isinstance(p, decimal.Decimal):
            sign, digits, disponent = p.as_tuple()
            if disponent > 0:
                exp = 256 - disponent
            else:
                exp = -disponent
            v = 0
            ln = len(digits)
            for i in range(ln):
                v += digits[i] * (10 ** (ln - i - 1))
            buf += bytes([DECIMALNTYPE, 9])
            buf += bytes([decimal.getcontext().prec, exp])
            buf += bytes([9, bool(not sign)]) + _int_to_8bytes(v)
        else:
            # another type. pack as string parameter
            s = str(p)
            ln = len(s) * 2
            buf += bytes([NCHARTYPE]) + ln.to_bytes(2, byteorder='little')
            buf += _int_to_2bytes(connection.lcid) + bytes([0, 0, 0])
            buf += ln.to_bytes(2, byteorder='little') + _str_to_bytes(s)

    return buf


def _parse_byte(data):
    return data[0], data[1:]


def _parse_int(data, ln):
    return _bytes_to_int(data[:ln]), data[ln:]


def _parse_uint(data, ln):
    return _bytes_to_uint(data[:ln]), data[ln:]


def _parse_collation(data):
    return data[:5], data[5:]


def _parse_str(data, ln):
    slen, data = _parse_uint(data, ln)
    return _bytes_to_str(data[:slen*2]), data[slen*2:]


def _parse_variant(data, ln):
    data2, data = data[:ln], data[ln:]
    type_id, data2 = _parse_byte(data2)
    prop_bytes, data2 = _parse_byte(data2)

    if type_id in (INT1TYPE, ):
        v, data2 = _parse_int(data2, 1)
    elif type_id in (INT2TYPE, ):
        v, data2 = _parse_int(data2, 2)
    elif type_id in (INT4TYPE, ):
        v, data2 = _parse_int(data2, 4)
    elif type_id in (NVARCHARTYPE, ):
        _, data2 = _parse_collation(data2)
        v, data2 = _parse_str(data2, 2)
    elif type_id in (DATETIMETYPE, ):
        d, data2 = _parse_int(data2, 4)
        t, data2 = _parse_int(data2, 4)
        ms = int(round(t % 300 * 10 / 3.0))
        secs = t // 300
        v = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=d, seconds=secs, milliseconds=ms)
    else:
        raise Error("_parse_variant() Unknown type %d" % (type_id,))
    return v, data


def parse_transaction_id(data):
    "return transaction_id"
    t, data = _parse_byte(data)
    assert t == TDS_TOKEN_ENVCHANGE
    _, data = _parse_int(data, 2)   # packet length
    e, data = _parse_byte(data)
    assert e == TDS_ENV_BEGINTRANS
    ln, data = _parse_byte(data)
    assert ln == 8                  # transaction id length
    return data[:ln], data[ln:]


def _parse_description_type(data):
    user_type, data = _parse_uint(data, 4)
    flags, data = _parse_uint(data, 2)
    null_ok = (flags & 1) == 1
    type_id, data = _parse_byte(data)

    size = precision = scale = -1
    size = {
        INT1TYPE: 1,
        BITTYPE: 1,
        INT2TYPE: 2,
        INT4TYPE: 4,
        INT8TYPE: 8,
        DATETIM4TYPE: 4,
        DATETIMETYPE: 8,
        DATENTYPE: 3,
    }.get(type_id, 0)

    if size != 0:
        pass
    elif type_id in (
        BITNTYPE, INTNTYPE, FLTNTYPE, MONEYNTYPE, DATETIMNTYPE,
    ):
        size, data = _parse_byte(data)
    elif type_id in (IMAGETYPE, TEXTTYPE):
        size, data = _parse_byte(data)
        _, data = _parse_int(data, 4)
        tab_name, data = _parse_str(data, 2)
    elif type_id in (NUMERICNTYPE, DECIMALNTYPE):
        size, data = _parse_byte(data)
        precision, data = _parse_byte(data)
        scale, data = _parse_byte(data)
    elif type_id in (SYBVARBINARY,):
        size, data = _parse_int(data, 2)
    elif type_id in (
        BIGCHARTYPE, BIGVARCHRTYPE, NCHARTYPE, NVARCHARTYPE, BIGVARCHRTYPE
    ):
        size, data = _parse_int(data, 2)
        _, data = _parse_collation(data)
    elif type_id in (DATETIME2NTYPE, DATETIMEOFFSETNTYPE, TIMENTYPE):
        precision, data = _parse_byte(data)
    elif type_id in (SSVARIANTTYPE,):
        size, data = _parse_int(data, 4)
    else:
        print("_parse_description_type() Unknown type_id:", type_id)

    name, data = _parse_str(data, 1)
    return type_id, name, size, precision, scale, null_ok, data


def parse_description(data):
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


def _parse_column(type_id, size, precision, scale, encoding, data):
    if type_id in (INT1TYPE, BITTYPE, INT2TYPE, INT4TYPE, INT8TYPE):
        v, data = _parse_int(data, size)
    elif type_id in (BITNTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            assert ln == size
            v, data = _parse_int(data, ln)
    elif type_id in (INTNTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            assert ln == size
            v, data = _parse_int(data, ln)
    elif type_id in (MONEYNTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            assert ln == size
            hi, data = _parse_int(data, ln // 2)
            lo, data = _parse_uint(data, ln // 2)
            v = decimal.Decimal(hi * (2**32) + lo) / 10000
    elif type_id in (FLTNTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            assert ln == size
            v, data = data[:size], data[size:]
            v = struct.unpack('<d' if ln == 8 else '<f', v)[0]
    elif type_id in (IMAGETYPE, TEXTTYPE):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            ln, data = _parse_int(data, 4)
            v, data = data[:ln], data[ln:]
            if type_id == TEXTTYPE:
                v = _bytes_to_str(v)
    elif type_id in (NUMERICNTYPE, DECIMALNTYPE):
        ln, data = _parse_byte(data)
        positive, data = _parse_byte(data)
        v, data = _parse_int(data, ln - 1)
        v = decimal.Decimal(v)
        if not positive:
            v *= -1
        v /= 10 ** scale
    elif type_id in (SYBVARBINARY, ):
        ln, data = _parse_uint(data, 2)
        if ln == 0xFFFF:
            v = None
        else:
            v, data = data[:ln], data[ln:]
    elif type_id in (NCHARTYPE, NVARCHARTYPE):
        ln, data = _parse_int(data, 2)
        if ln < 0:
            v = None
        else:
            v, data = data[:ln], data[ln:]
            v = _bytes_to_str(v)
    elif type_id in (BIGCHARTYPE, BIGVARCHRTYPE):
        ln = _bytes_to_int(data[:2])
        data = data[2:]
        if ln < 0:
            v = None
        else:
            v, data = data[:ln], data[ln:]
            v = v.decode(encoding)
    elif type_id in (DATETIM4TYPE, DATETIMETYPE,):
        d, data = _parse_int(size // 2)
        t, data = _parse_int(size // 2)
        ms = int(round(t % 300 * 10 / 3.0))
        secs = t // 300
        v = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=d, seconds=secs, milliseconds=ms)
    elif type_id in (DATETIME2NTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            t, data = data[:ln-3], data[ln-3:]
            t = _convert_time(t, precision)
            d, data = data[:3], data[3:]
            d = _convert_date(d)
            v = datetime.datetime.combine(d, t)
    elif type_id in (DATETIMNTYPE,):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            assert ln == size
            d, data = _parse_int(data, ln//2)
            t, data = _parse_int(data, ln//2)
            ms = int(round(t % 300 * 10 / 3.0))
            secs = t // 300
            v = datetime.datetime(1900, 1, 1) + datetime.timedelta(days=d, seconds=secs, milliseconds=ms)
    elif type_id in (DATETIMEOFFSETNTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            t, data = data[:ln-5], data[ln-5:]
            t = _convert_time(t, precision)
            d, data = data[:3], data[3:]
            d = _convert_date(d)
            tz_offset, data = data[:2], data[2:]
            tz_offset = _bytes_to_int(tz_offset)
            v = datetime.datetime.combine(d, t) + datetime.timedelta(minutes=_min_timezone_offset()+tz_offset)
            v = v.replace(tzinfo=UTC())
    elif type_id in (DATENTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            v, data = data[:ln], data[ln:]
            v = _convert_date(v)
    elif type_id in (TIMENTYPE, ):
        ln, data = _parse_byte(data)
        if ln == 0:
            v = None
        else:
            v, data = data[:ln], data[ln:]
            v = _convert_time(v, precision)
    elif type_id in (SSVARIANTTYPE, ):
        ln, data = _parse_int(data, 4)
        if ln == 0:
            v = None
        else:
            v, data = _parse_variant(data, ln)
    else:
        raise Error("_parse_column() Unknown type %d" % (type_id,))

    return v, data


def parse_row(description, encoding, data):
    t, data = _parse_byte(data)
    assert t == TDS_ROW_TOKEN

    row = []
    for _, type_id, size, _, precision, scale, _ in description:
        v, data = _parse_column(type_id, size, precision, scale, encoding, data)
        row.append(v)
    return row, data

def parse_nbcrow(description, encoding, data):
    t, data = _parse_byte(data)
    assert t == TDS_NBCROW_TOKEN

    null_bitmap_len = (len(description) + 7) // 8
    null_bitmap = data[:null_bitmap_len]
    data = data[null_bitmap_len:]

    row = []
    for i, (_, type_id, size, _, precision, scale, _) in enumerate(description):
        if null_bitmap[i // 8] & (1 << (i % 8)):
            v = None
        else:
            v, data = _parse_column(type_id, size, precision, scale, encoding, data)
        row.append(v)
    return row, data


def parse_error(data):
    assert data[0] == TDS_ERROR_TOKEN
    msg_ln = _bytes_to_int(data[9:11])
    return _bytes_to_str(data[11:msg_ln*2+11])

# -----------------------------------------------------------------------------

def escape_parameter(v):
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
    elif t == int or t == float:
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


    def callproc(self, procname, args=[]):
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")
        self.description = []
        self.query = procname
        self.args = args
        return_status, self.description, self._rows = self.connection._callproc(procname, args)
        return return_status

    def nextset(self, procname, args=[]):
        raise NotSupportedError()

    def setinputsizes(sizes):
        pass

    def setoutputsize(size, column=None):
        pass

    def execute(self, query, args=[]):
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
            row = tuple(self._rows[0])
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
    def _do_ssl_handshake(self):
        incoming = ssl.MemoryBIO()
        outgoing = ssl.MemoryBIO()
        sslobj = ssl.SSLContext().wrap_bio(incoming, outgoing, False)

        # do_handshake()
        while True:
            try:
                sslobj.do_handshake()
            except ssl.SSLWantReadError:
                self._send_message(TDS_PRELOGIN, outgoing.read())
                tag, _, _, buf = self._read_response_packet()
                assert tag == TDS_PRELOGIN
                incoming.write(buf)
            else:
                break

        return sslobj, incoming, outgoing

    def __init__(self, user, password, database, host, instance_name, isolation_level, port, lcid, encoding, use_ssl, timeout):
        self.user = user
        self.password = password
        self.database = database
        self.host = host
        self.instance_name = instance_name
        self.isolation_level = isolation_level
        self.port = port
        self.lcid = lcid
        self.encoding = encoding
        self.use_ssl = use_ssl
        self.timeout = timeout
        self.autocommit = False
        self._packet_id = 0
        self.sslobj = self.incoming = self.outgoing = None

        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.connect((self.host, self.port))
        if self.timeout is not None:
            self.sock.settimeout(float(self.timeout))

        self._send_message(TDS_PRELOGIN, get_prelogin_bytes(self.use_ssl, self.instance_name))
        _, _, _, body = self._read_response_packet()

        if body[32] == 1:
            self.sslobj, self.incoming, self.outgoing = self._do_ssl_handshake()

        self._send_message(TDS_LOGIN, get_login_bytes(self.host, self.user, self.password, self.database, self.lcid))
        self._read_response_packet()
        self.begin()

    def __enter__(self):
        return self

    def __exit__(self, exc, value, traceback):
        self.close()

    def _read(self, ln):
        if not self.sock:
            raise OperationalError("Lost connection")
        if self.sslobj:
            while True:
                try:
                    r = self.sslobj.read(ln)
                except ssl.SSLWantReadError:
                    b = self.sock.recv(32768)
                    self.incoming.write(b)
                    continue
                break
        else:
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
        if self.sslobj:
            self.sslobj.write(b)
            b = self.outgoing.read()

        n = 0
        while (n < len(b)):
            n += self.sock.send(b[n:])

    def _read_response_packet(self):
        b = self._read(8)
        tag = b[0]
        status = b[1]
        ln = _bytes_to_bint(b[2:4]) - 8
        spid = _bytes_to_bint(b[4:6])

        return tag, status, spid, self._read(ln)

    def _send_message(self, message_type, buf):
        data, buf = buf[:BUFSIZE-8], buf[BUFSIZE-8:]
        while buf:
            self._write(
                bytes([message_type, 0]) +
                _bint_to_2bytes(8 + len(data)) +
                _bint_to_2bytes(0) +
                bytes([self._packet_id, 0]) +
                data
            )
            self._packet_id = (self._packet_id + 1) % 256
            data, buf = buf[:BUFSIZE-8], buf[BUFSIZE-8:]

        self._write(
            bytes([message_type, 1]) +
            _bint_to_2bytes(8 + len(data)) +
            _bint_to_2bytes(0) +
            bytes([self._packet_id, 0]) +
            data
        )
        self._packet_id = (self._packet_id + 1) % 256

    def is_connect(self):
            return bool(self.sock)

    def cursor(self):
        return Cursor(self)

    def _execute(self, query):
        self._send_message(TDS_SQL_BATCH, get_sql_batch_bytes(self.transaction_id, query))
        token, status, spid, data = self._read_response_packet()
        while status == 0:
            token, status, spid, more_data = self._read_response_packet()
            data += more_data

        if data[0] == TDS_ERROR_TOKEN:
            raise OperationalError(parse_error(data))
        elif data[0] == TDS_TOKEN_COLMETADATA:
            description, data = parse_description(data)
        else:
            description = []
        rows = []
        while data[0] in (TDS_ROW_TOKEN, TDS_NBCROW_TOKEN):
            if data[0] == TDS_ROW_TOKEN:
                row, data = parse_row(description, self.encoding, data)
            elif data[0] == TDS_NBCROW_TOKEN:
                row, data = parse_nbcrow(description, self.encoding, data)
            else:
                assert False
            rows.append(row)
        if self.autocommit:
            self.commit()

        return description, rows


    def _callproc(self, procname, args):
        self._send_message(TDS_RPC, get_rpc_request_bytes(self, procname, args))

        token, status, spid, data = self._read_response_packet()
        while status == 0:
            _, status, spid, more_data = self._read_response_packet()
            data += more_data

        if token == TDS_TABULAR_RESULT:
            assert data[-18] == 0x79
            return_status = _bytes_to_int(data[-17:-13])
        else:
            return_status = None

        if data[0] == TDS_ERROR_TOKEN:
            raise OperationalError(parse_error(data))
        elif data[0] == TDS_TOKEN_COLMETADATA:
            description, data = parse_description(data)
        else:
            description = []
        rows = []
        while data[0] in (TDS_ROW_TOKEN, TDS_NBCROW_TOKEN):
            if data[0] == TDS_ROW_TOKEN:
                row, data = parse_row(description, self.encoding, data)
            elif data[0] == TDS_NBCROW_TOKEN:
                row, data = parse_nbcrow(description, self.encoding, data)
            else:
                assert False
            rows.append(row)
        if self.autocommit:
            self.commit()
        return return_status, description, rows


    def set_autocommit(self, autocommit):
        self.autocommit = autocommit


    def begin(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(b'\x00'*8, TM_BEGIN_XACT, self.isolation_level))
        _, _, _, data = self._read_response_packet()
        self.transaction_id, _ = parse_transaction_id(data)

    def commit(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(self.transaction_id, TM_COMMIT_XACT, self.isolation_level))
        self._read_response_packet()
        self.begin()

    def rollback(self):
        self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(self.transaction_id, TM_ROLLBACK_XACT, self.isolation_level))
        self._read_response_packet()
        self.begin()

    def close(self):
        if self.sock:
            self.sock.close()
            self.sock = None


def connect(host, database, user, password, instance_name='MSSQLServer', isolation_level=0, port=1433, lcid=1033, encoding='latin1', use_ssl=None, timeout=None):
    return Connection(user, password, database, host, instance_name, isolation_level, port, lcid, encoding, use_ssl, timeout)


def output_results(conn, query, with_header=True, separator="\t", null='null', file=sys.stdout):
    def _ustr(c):
        if c is None:
            return null
        elif c is True:
            return 'true'
        elif c is False:
            return 'false'
        elif not isinstance(c, str):
            c = str(c)
        return c

    cur = conn.cursor()
    cur.execute(query)
    if with_header:
        print(separator.join([_ustr(d[0]) for d in cur.description]), file=file)
    for r in cur.fetchall():
        print(separator.join([_ustr(c) for c in r]), file=file)


def main(file):
    parser = ArgumentParser(description='Execute query and print results.')
    parser.add_argument('-H', '--host', default='localhost', metavar='host', type=str, help='host name')
    parser.add_argument('-U', '--user', required=True, metavar='user', type=str, help='login user')
    parser.add_argument('-W', '--password', default='', metavar='password', type=str, help='login password')
    parser.add_argument('-P', '--port', default=1433, metavar='port', type=int, help='port number')
    parser.add_argument('-D', '--database', default='', metavar='database', type=str, help='database name')
    parser.add_argument('-E', '--encoding', default='latin1', metavar='encoding', type=str, help='server encoding')
    parser.add_argument('-Q', '--query', metavar='query', type=str, help='query string')
    parser.add_argument('-F', '--field-separator', default="\t", metavar='field_separator', type=str, help='field separator')
    parser.add_argument('--header', action='store_true', dest='with_header', help='Output header')
    parser.add_argument('--no-header', action='store_false', dest='with_header', help='No output header')
    parser.add_argument('--null', default='null', metavar='null', type=str, help='null value replacement string')

    parser.set_defaults(with_header=True)

    args = parser.parse_args()
    if args.query is None:
        args.query = sys.stdin.read()

    conn = connect(args.host, args.database, args.user, args.password, 0, args.port, encoding=args.encoding)
    output_results(conn, args.query, args.with_header, args.field_separator, args.null, file)

    conn.commit()

if __name__ == '__main__':
    main(sys.stdout)
