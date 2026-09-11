##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016-2026 Hajime Nakagami
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
import asyncio
import ssl
from collections.abc import Sequence
from typing import Any

from minitds.minitds import (
    BUFSIZE,
    DEBUG_OUTPUT,
    ISOLATION_LEVEL_READ_COMMITTED,
    TDS_DONEINPROC_TOKEN,
    TDS_DONEPROC_TOKEN,
    TDS_DONE_TOKEN,
    TDS_ENVCHANGE_TOKEN,
    TDS_ENV_BEGINTRANS,
    TDS_ERROR_TOKEN,
    TDS_INFO_TOKEN,
    TDS_LOGIN,
    TDS_NBCROW_TOKEN,
    TDS_ORDER_TOKEN,
    TDS_PRELOGIN,
    TDS_RETURNSTATUS_TOKEN,
    TDS_ROW_TOKEN,
    TDS_RPC,
    TDS_SQL_BATCH,
    TDS_TABULAR_RESULT,
    TDS_TOKEN_COLMETADATA,
    TDS_TRANSACTION_MANAGER_REQUEST,
    TM_BEGIN_XACT,
    TM_COMMIT_XACT,
    TM_ROLLBACK_XACT,
    DatabaseError,
    IntegrityError,
    NotSupportedError,
    OperationalError,
    ProgrammingError,
    _bint_to_2bytes,
    _bytes_to_bint,
    _bytes_to_int,
    _bytes_to_str,
    _parse_byte,
    _parse_int,
    _parse_str,
    get_login_bytes,
    get_prelogin_bytes,
    get_rpc_request_bytes,
    get_sql_batch_bytes,
    get_trans_request_bytes,
    parse_description,
    parse_nbcrow,
    parse_row,
    quote_value,
)


class AsyncCursor:
    def __init__(self, connection: 'AsyncConnection') -> None:
        self.connection: AsyncConnection | None = connection
        self.description: list[tuple] = []
        self._rows: list[tuple[Any, ...]] = []
        self._rowcount: int = 0
        self.arraysize: int = 1
        self.query: str | None = None
        self.last_sql: str | None = None
        self.last_params: Sequence[Any] | dict[str, Any] | None = None
        self.return_stats = None

    async def __aenter__(self) -> 'AsyncCursor':
        return self

    async def __aexit__(self, exc: Any, value: Any, traceback: Any) -> None:
        await self.close()

    async def callproc(self, procname: str, args: Sequence[Any] | None = None) -> Any:
        if args is None:
            args = []
        DEBUG_OUTPUT('callproc:%s' % procname)
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")

        self.last_sql = procname
        self.last_params = args
        if not self.connection.transaction_id:
            await self.connection.begin()

        self.description = []
        return_status, self.description, self._rows = await self.connection._callproc(procname, args)
        self.connection._last_description = self.description
        self.connection._last_rows = self._rows
        if self.connection.autocommit:
            await self.connection.commit()
        return return_status

    async def nextset(self, procname: str | None = None, args: Sequence[Any] | None = None) -> None:
        raise NotSupportedError()

    def setinputsizes(self, sizes: Any) -> None:
        pass

    def setoutputsize(self, size: Any, column: Any | None = None) -> None:
        pass

    async def execute(self, query: str, args: Sequence[Any] | dict[str, Any] | None = None) -> None:
        DEBUG_OUTPUT("execute:%s" % (query))
        if not self.connection or not self.connection.is_connect():
            raise ProgrammingError("Lost connection")
        self.description = []
        if args:
            if isinstance(args, dict):
                s = query % {k: quote_value(v) for k, v in args.items()}
            else:
                escaped_args = tuple(quote_value(arg).replace('%', '%%') for arg in args)
                s = query.replace('%', '%%').replace('%%s', '%s')
                s = s % escaped_args
                s = s.replace('%%', '%')
        else:
            s = query

        if not self.connection.transaction_id:
            await self.connection.begin()
        self.description, self._rows, self._rowcount = await self.connection._execute(s)
        self.connection._last_description = self.description
        self.connection._last_rows = self._rows
        if self.connection.autocommit:
            await self.connection.commit()
        self.last_sql = query
        self.last_params = args

    async def executemany(self, query: str, seq_of_params: Sequence[Sequence[Any] | dict[str, Any]]) -> None:
        DEBUG_OUTPUT("executemany:%s" % (query))
        rowcount = 0
        for params in seq_of_params:
            await self.execute(query, params)
            rowcount += self._rowcount
        self._rowcount = rowcount

    async def fetchone(self) -> tuple[Any, ...] | None:
        DEBUG_OUTPUT("fetchone()")
        if not self.connection or not self.connection.is_connect():
            raise OperationalError("Lost connection")
        if len(self._rows):
            row = tuple(self._rows[0])
            self._rows = self._rows[1:]
        else:
            row = None
        return row

    async def fetchmany(self, size: int | None = None) -> list[tuple[Any, ...]]:
        DEBUG_OUTPUT("fetchmany()")
        if size is None:
            size = self.arraysize
        rs = []
        for i in range(size):
            r = await self.fetchone()
            if not r:
                break
            rs.append(r)
        return rs

    async def fetchall(self) -> list[tuple[Any, ...]]:
        DEBUG_OUTPUT("fetchall()")
        rows = self._rows
        self._rows = []
        return rows

    async def close(self) -> None:
        pass

    @property
    def rowcount(self) -> int:
        return self._rowcount

    def __aiter__(self) -> 'AsyncCursor':
        return self

    async def __anext__(self) -> tuple[Any, ...]:
        r = await self.fetchone()
        if not r:
            raise StopAsyncIteration()
        return r


class AsyncConnection:
    async def _do_ssl_handshake(self):
        incoming = ssl.MemoryBIO()
        outgoing = ssl.MemoryBIO()
        sslobj = ssl.SSLContext(protocol=ssl.PROTOCOL_TLSv1_2).wrap_bio(incoming, outgoing, False)

        while True:
            try:
                sslobj.do_handshake()
            except ssl.SSLWantReadError:
                await self._send_message(TDS_PRELOGIN, outgoing.read())
                tag, _, _, buf = await self._read_response_packet()
                assert tag == TDS_PRELOGIN
                incoming.write(buf)
            else:
                break

        return sslobj, incoming, outgoing

    def __init__(
        self,
        user: str,
        password: str,
        database: str,
        host: str,
        instance_name: str,
        isolation_level: int,
        autocommit: bool,
        port: int,
        lcid: int,
        encoding: str,
        use_ssl: bool | None,
        timeout: float | None
    ) -> None:
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
        self.autocommit = autocommit
        self._packet_id: int = 0
        self.transaction_id: bytes | None = None
        self.is_dirty: bool = False
        self._last_description: list[tuple] = []
        self._last_rows: list[tuple[Any, ...]] = []
        self.sslobj: ssl.SSLObject | None = None
        self.incoming: ssl.MemoryBIO | None = None
        self.outgoing: ssl.MemoryBIO | None = None
        self.return_status: int | None = None
        self.reader: asyncio.StreamReader | None = None
        self.writer: asyncio.StreamWriter | None = None

    async def _initialize(self) -> None:
        DEBUG_OUTPUT('{}:connect():{}:{}:{}'.format(id(self), self.host, self.database, self.autocommit))

        if any([ord(c) > 127 for c in self.password]):
            raise DatabaseError("Invalid password")

        coro = asyncio.open_connection(self.host, self.port)
        if self.timeout is not None:
            self.reader, self.writer = await asyncio.wait_for(coro, timeout=float(self.timeout))
        else:
            self.reader, self.writer = await coro

        await self._send_message(TDS_PRELOGIN, get_prelogin_bytes(self.use_ssl, self.instance_name))
        _, _, _, body = await self._read_response_packet()

        if body[32] == 1:
            self.sslobj, self.incoming, self.outgoing = await self._do_ssl_handshake()
        await self._send_message(TDS_LOGIN, get_login_bytes(self.host, self.user, self.password, self.database, self.lcid))
        await self._read_response_packet()

    async def __aenter__(self) -> 'AsyncConnection':
        DEBUG_OUTPUT('AsyncConnection::__aenter__()')
        return self

    async def __aexit__(self, exc: Any, value: Any, traceback: Any) -> None:
        DEBUG_OUTPUT('AsyncConnection::__aexit__()')
        if exc:
            await self.rollback()
        else:
            await self.commit()

    async def _read(self, ln: int) -> bytes:
        if not self.is_connect():
            raise OperationalError("Lost connection")
        r = b''
        if self.sslobj:
            while len(r) < ln:
                try:
                    b = self.sslobj.read(ln - len(r))
                    if not b:
                        raise OperationalError("Can't recv packets")
                    r += b
                except ssl.SSLWantReadError:
                    if self.reader:
                        chunk = await self.reader.read(1024)
                        if not chunk:
                            raise OperationalError("Can't recv packets")
                        self.incoming.write(chunk)
        else:
            while len(r) < ln:
                chunk = await self.reader.read(ln - len(r))
                if not chunk:
                    raise OperationalError("Can't recv packets")
                r += chunk
        return r

    async def _write(self, b: bytes) -> None:
        if not self.is_connect():
            raise OperationalError("Lost connection")
        if self.sslobj:
            self.sslobj.write(b)
            b = self.outgoing.read()

        self.writer.write(b)
        await self.writer.drain()

    async def _read_response_packet(self) -> tuple[int, int, int, bytes]:
        DEBUG_OUTPUT('_read_response_packet()')
        b = await self._read(8)
        tag = b[0]
        status = b[1]
        ln = _bytes_to_bint(b[2:4]) - 8
        spid = _bytes_to_bint(b[4:6])

        return tag, status, spid, await self._read(ln)

    async def _send_message(self, message_type: int, buf: bytes) -> None:
        data, buf = buf[:BUFSIZE-8], buf[BUFSIZE-8:]
        while buf:
            await self._write(
                bytes([message_type, 0]) +
                _bint_to_2bytes(8 + len(data)) +
                _bint_to_2bytes(0) +
                bytes([self._packet_id, 0]) +
                data
            )
            self._packet_id = (self._packet_id + 1) % 256
            data, buf = buf[:BUFSIZE-8], buf[BUFSIZE-8:]

        await self._write(
            bytes([message_type, 1]) +
            _bint_to_2bytes(8 + len(data)) +
            _bint_to_2bytes(0) +
            bytes([self._packet_id, 0]) +
            data
        )
        self._packet_id = (self._packet_id + 1) % 256

    def parse_transaction_id(self, data: bytes) -> tuple[bytes, bytes]:
        "return transaction_id"
        if data[0] == TDS_ERROR_TOKEN:
            raise self.parse_error('begin()', data)
        t, data = _parse_byte(data)
        assert t == TDS_ENVCHANGE_TOKEN
        _, data = _parse_int(data, 2)   # packet length
        e, data = _parse_byte(data)
        assert e == TDS_ENV_BEGINTRANS
        ln, data = _parse_byte(data)
        assert ln == 8                  # transaction id length
        return data[:ln], data[ln:]

    def parse_error(self, query: str, data: bytes) -> DatabaseError:
        assert data[0] == TDS_ERROR_TOKEN
        err_num = _bytes_to_int(data[3:7])
        msg_ln = _bytes_to_int(data[9:11])
        message = _bytes_to_str(data[11:msg_ln*2+11])
        if err_num in (102, 207, 208, 2812, 4104):
            return ProgrammingError("{}:{}:{}".format(err_num, message, query), err_num)
        elif err_num in (515, 547, 2601, 2627):
            return IntegrityError("{}:{}:{}".format(err_num, message, query), err_num)
        return OperationalError("{}:{}:{}".format(err_num, message, query), err_num)

    def is_connect(self) -> bool:
        return self.writer is not None and not self.writer.is_closing()

    def cursor(self, factory: type[AsyncCursor] = AsyncCursor) -> AsyncCursor:
        return factory(self)

    async def _execute(self, query: str) -> tuple[list[tuple], list[tuple[Any, ...]], int]:
        self.is_dirty = True
        DEBUG_OUTPUT('{}:_execute():{}'.format(id(self), query), end='')
        await self._send_message(TDS_SQL_BATCH, get_sql_batch_bytes(self.transaction_id, query))
        token, status, spid, data = await self._read_response_packet()
        while status == 0:
            token, status, spid, more_data = await self._read_response_packet()
            data += more_data

        description = []
        rows = []
        rowcount = 0
        while data and data[0]:
            if data[0] == TDS_ERROR_TOKEN:
                obj = self.parse_error(query, data)
                raise obj
            elif data[0] == TDS_INFO_TOKEN:
                ln = _bytes_to_int(data[1:3])
                # info_num = _bytes_to_int(data[3:7])
                msg_ln = _bytes_to_int(data[9:11])
                message = _bytes_to_str(data[11:msg_ln*2+11])
                DEBUG_OUTPUT("TDS_INFO_TOKEN:%s" % message)
                data = data[msg_ln*2+11:]
                ln, data = _parse_int(data, 1)
                server_name, data = data[:ln*2], data[ln*2:]
                server_name = _bytes_to_str(server_name)
                ln, data = _parse_int(data, 1)
                proc_name, data = data[:ln*2], data[ln*2:]
                proc_name = _bytes_to_str(proc_name)
                lineno, data = _parse_int(data, 4)
                break
            elif data[0] == TDS_TOKEN_COLMETADATA:
                description, data = parse_description(data)
            elif data[0] == TDS_ROW_TOKEN:
                row, data = parse_row(description, self.encoding, data)
                rows.append(row)
            elif data[0] == TDS_NBCROW_TOKEN:
                row, data = parse_nbcrow(description, self.encoding, data)
                rows.append(row)
            elif data[0] in (TDS_DONE_TOKEN, TDS_DONEPROC_TOKEN, TDS_DONEINPROC_TOKEN):
                rowcount += _bytes_to_int(data[5:13])
                data = data[13:]
            elif data[0] == TDS_ORDER_TOKEN:
                ln = _bytes_to_int(data[1:3])
                data = data[3+ln:]
            elif data[0] in (TDS_ENVCHANGE_TOKEN, ):
                ln = _bytes_to_int(data[1:3])
                data = data[3+ln:]
            elif data[0] in (TDS_RETURNSTATUS_TOKEN, ):
                ln = _bytes_to_int(data[1:3])
                data = data[3+ln:]
            else:
                raise ValueError("Unknown token: {}".format(hex(data[0])))

        DEBUG_OUTPUT(":={}".format(rowcount))
        return description, rows, rowcount

    async def _callproc(self, procname: str, args: Sequence[Any]) -> tuple[int | None, list[tuple], list[tuple[Any, ...]]]:
        DEBUG_OUTPUT('_callback()')
        await self._send_message(TDS_RPC, get_rpc_request_bytes(self, procname, args))

        token, status, spid, data = await self._read_response_packet()
        while status == 0:
            _, status, spid, more_data = await self._read_response_packet()
            data += more_data

        self.return_status = None
        if token == TDS_TABULAR_RESULT:
            assert data[-18] == 0x79
            self.return_status = _bytes_to_int(data[-17:-13])

        if data and data[0] == TDS_ERROR_TOKEN:
            raise self.parse_error(procname, data)
        elif data and data[0] == TDS_TOKEN_COLMETADATA:
            description, data = parse_description(data)
        else:
            description = []
        rows = []
        while data and data[0] in (TDS_ROW_TOKEN, TDS_NBCROW_TOKEN):
            if data[0] == TDS_ROW_TOKEN:
                row, data = parse_row(description, self.encoding, data)
            elif data[0] == TDS_NBCROW_TOKEN:
                row, data = parse_nbcrow(description, self.encoding, data)
            else:
                assert False
            rows.append(row)
        return self.return_status, description, rows

    def set_autocommit(self, autocommit: bool) -> None:
        DEBUG_OUTPUT('{}:set_autocommit():{}'.format(id(self), autocommit))
        self.autocommit = autocommit

    async def begin(self) -> None:
        DEBUG_OUTPUT('{}:begin()'.format(id(self)), end=' ')
        await self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(None, TM_BEGIN_XACT, self.isolation_level))
        _, _, _, data = await self._read_response_packet()
        self.transaction_id, _ = self.parse_transaction_id(data)
        self.is_dirty = False
        DEBUG_OUTPUT('transaction_id={}'.format(self.transaction_id))

    async def _commit(self) -> None:
        DEBUG_OUTPUT('{}:_commit() transaction_id={}'.format(id(self), self.transaction_id))
        await self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(self.transaction_id, TM_COMMIT_XACT, 0))
        await self._read_response_packet()
        self.transaction_id = None
        self.is_dirty = False

    async def commit(self) -> None:
        DEBUG_OUTPUT('commit()')
        if self.is_dirty:
            await self._commit()

    async def _rollback(self) -> None:
        DEBUG_OUTPUT('{}:_rollback() transaction_id={}'.format(id(self), self.transaction_id))
        await self._send_message(TDS_TRANSACTION_MANAGER_REQUEST, get_trans_request_bytes(self.transaction_id, TM_ROLLBACK_XACT, self.isolation_level))
        await self._read_response_packet()
        self.transaction_id = None
        self.is_dirty = False

    async def rollback(self) -> None:
        DEBUG_OUTPUT('rollback()')
        if self.is_dirty:
            await self._rollback()

    async def close(self) -> None:
        if self.writer is not None:
            DEBUG_OUTPUT('close():{}:{}:{}'.format(self.host, self.database, self.autocommit))
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except Exception:
                pass
            self.reader = None
            self.writer = None


async def connect(
    host: str,
    database: str,
    user: str,
    password: str,
    instance_name: str = 'MSSQLServer',
    isolation_level: int = ISOLATION_LEVEL_READ_COMMITTED,
    autocommit: bool = False,
    port: int = 1433,
    lcid: int = 1033,
    encoding: str = 'utf8',
    use_ssl: bool | None = None,
    timeout: float | None = None
) -> AsyncConnection:
    DEBUG_OUTPUT('aio.connect():{}:{}:{}'.format(host, database, autocommit))
    conn = AsyncConnection(
        user, password, database, host, instance_name,
        isolation_level, autocommit, port, lcid, encoding, use_ssl, timeout
    )
    await conn._initialize()
    return conn
