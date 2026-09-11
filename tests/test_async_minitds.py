#!/usr/bin/env python3
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
import os
import unittest
import datetime
import decimal
import uuid
import minitds
import minitds.aio
from minitds.aio import AsyncCursor


class TestAsyncCursorStandalone(unittest.IsolatedAsyncioTestCase):
    """Standalone unit tests for AsyncCursor without requiring a database connection."""

    async def test_cursor_fetch(self):
        class DummyAsyncConn:
            def is_connect(self):
                return True

        cur = AsyncCursor(DummyAsyncConn())
        cur._rows = [(1, 'a'), (2, 'b'), (3, 'c')]
        cur.arraysize = 2

        self.assertEqual(await cur.fetchone(), (1, 'a'))
        self.assertEqual(await cur.fetchmany(), [(2, 'b'), (3, 'c')])
        self.assertIsNone(await cur.fetchone())

    async def test_cursor_async_iter(self):
        class DummyAsyncConn:
            def is_connect(self):
                return True

        cur = AsyncCursor(DummyAsyncConn())
        cur._rows = [(1,), (2,), (3,)]

        rows = []
        async for r in cur:
            rows.append(r)
        self.assertEqual(rows, [(1,), (2,), (3,)])

    async def test_cursor_context_manager(self):
        class DummyAsyncConn:
            def is_connect(self):
                return True

        async with AsyncCursor(DummyAsyncConn()) as cur:
            cur._rows = [(100,)]
            row = await cur.fetchone()
            self.assertEqual(row, (100,))


class TestAsyncPacketHandling(unittest.IsolatedAsyncioTestCase):
    """Test AsyncConnection socket read/write with a local mock server."""

    async def test_send_and_read_packet(self):
        import asyncio
        from minitds.aio import AsyncConnection
        from minitds.minitds import TDS_SQL_BATCH

        received_packets = []

        async def handle_client(reader, writer):
            data = await reader.read(4096)
            received_packets.append(data)
            # Response: TDS packet header (type=4, status=1, length=12, spid=1, packet_id=1, window=0) + 4 bytes payload
            header = bytes([4, 1, 0, 12, 0, 1, 1, 0])
            writer.write(header + b'TEST')
            await writer.drain()
            writer.close()
            await writer.wait_closed()

        server = await asyncio.start_server(handle_client, '127.0.0.1', 0)
        port = server.sockets[0].getsockname()[1]

        conn = AsyncConnection(
            user='user', password='password', database='db',
            host='127.0.0.1', instance_name='MSSQLServer',
            isolation_level=2, autocommit=False, port=port,
            lcid=1033, encoding='utf8', use_ssl=False, timeout=5.0
        )
        conn.reader, conn.writer = await asyncio.open_connection('127.0.0.1', port)

        await conn._send_message(TDS_SQL_BATCH, b'HELLO')
        tag, status, spid, payload = await conn._read_response_packet()

        self.assertEqual(tag, 4)
        self.assertEqual(status, 1)
        self.assertEqual(spid, 1)
        self.assertEqual(payload, b'TEST')
        self.assertTrue(len(received_packets) > 0)

        await conn.close()
        server.close()
        await server.wait_closed()


class TestAsyncMiniTds(unittest.IsolatedAsyncioTestCase):
    host = os.environ.get('TEST_MINITDS_HOST', 'localhost')
    user = os.environ.get('TEST_MINITDS_USER', 'sa')
    password = os.environ.get('TEST_MINITDS_PASSWORD', 'Secret123')
    database = os.environ.get('TEST_MINITDS_DATABASE', 'test')
    port = int(os.environ.get('TEST_MINITDS_PORT', '1433'))

    async def asyncSetUp(self):
        self.connection = await minitds.aio.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
        )

    async def asyncTearDown(self):
        await self.connection.close()

    async def test_basic_types(self):
        cur = self.connection.cursor()

        await cur.execute("""
            SELECT 1 a, 1.2 b, db_name() c, NULL d,
                cast(1.25 as money) e,
                cast(0.125 as float) f, cast(0.25 as real) g
        """)
        self.assertEqual(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            [d[0] for d in cur.description]
        )
        self.assertEqual(
            [1, decimal.Decimal('1.2'), 'test', None, decimal.Decimal('1.25'), 0.125, 0.25],
            list(await cur.fetchone())
        )

    async def test_datetime_types(self):
        cur = self.connection.cursor()

        await cur.execute("""
            SELECT cast('1967-08-11' as date),
                cast('12:34:56' as time),
                cast('1967-08-11 12:34:56' as datetime)
        """)
        self.assertEqual(
            [datetime.date(1967, 8, 11), datetime.time(12, 34, 56), datetime.datetime(1967, 8, 11, 12, 34, 56)],
            list(await cur.fetchone())
        )

    async def test_string_types(self):
        cur = self.connection.cursor()

        await cur.execute("""
            SELECT
                cast('A' as NVARCHAR(2)),
                cast('B' as NCHAR(2)),
                cast('C' as VARCHAR(2)),
                cast('D' as CHAR(2))
        """)
        self.assertEqual(
            ['A', 'B ', 'C', 'D '],
            list(await cur.fetchone())
        )

    async def test_bit_type(self):
        cur = self.connection.cursor()

        await cur.execute("SELECT cast(1 as BIT), cast(0 as BIT)")
        self.assertEqual(
            [1, 0],
            list(await cur.fetchone())
        )

    async def test_variant_types(self):
        cur = self.connection.cursor()

        await cur.execute("""
            SELECT
                SERVERPROPERTY('Collation'),
                SERVERPROPERTY('CollationID'),
                SERVERPROPERTY('EditionID'),
                SERVERPROPERTY('EngineEdition'),
                SERVERPROPERTY('SqlCharSet'),
                SERVERPROPERTY('ResourceLastUpdateDateTime')
        """)
        r = await cur.fetchone()
        self.assertTrue(isinstance(r[0], str))
        self.assertTrue(isinstance(r[1], int))
        self.assertTrue(isinstance(r[2], int))
        self.assertTrue(isinstance(r[3], int))
        self.assertTrue(isinstance(r[4], int))
        self.assertTrue(isinstance(r[5], datetime.datetime))

    async def test_autocommit(self):
        cur = self.connection.cursor()
        await cur.execute("drop table if exists test_aio_autocommit")
        await cur.execute("""
            CREATE TABLE test_aio_autocommit(
                id int IDENTITY(1,1) NOT NULL,
                s varchar(4096)
            )
        """)
        await self.connection.commit()

        await cur.execute("insert into test_aio_autocommit (s) values ('a')")
        await cur.execute("select count(*) from test_aio_autocommit")
        self.assertEqual((await cur.fetchone())[0], 1)
        await self.connection.rollback()
        await cur.execute("select count(*) from test_aio_autocommit")
        self.assertEqual((await cur.fetchone())[0], 0)

        self.connection.set_autocommit(True)
        await cur.execute("insert into test_aio_autocommit (s) values ('a')")
        await cur.execute("select count(*) from test_aio_autocommit")
        self.assertEqual((await cur.fetchone())[0], 1)
        await self.connection.rollback()
        await cur.execute("select count(*) from test_aio_autocommit")
        self.assertEqual((await cur.fetchone())[0], 1)

    async def test_decimal(self):
        cur = self.connection.cursor()
        await cur.execute("drop table if exists test_aio_decimal")
        await cur.execute("""
            CREATE TABLE test_aio_decimal(
                id int IDENTITY(1,1) NOT NULL,
                d decimal(10, 4)
            )
        """)
        await self.connection.commit()
        d = decimal.Decimal("1.23")
        await cur.execute("insert into test_aio_decimal (d) values (%s)", [d])
        await cur.execute("select d from test_aio_decimal where d=%s", [d])
        self.assertEqual((await cur.fetchone())[0], d)

    async def test_varbinary(self):
        cur = self.connection.cursor()
        await cur.execute("drop table if exists test_aio_varbinary")
        await cur.execute("""
            CREATE TABLE test_aio_varbinary(
                id int IDENTITY(1,1) NOT NULL,
                varbinary_column varbinary(max) null,
                primary key (id)
            )
        """)
        d = b'\x00\x01\x02'
        await cur.execute("insert into test_aio_varbinary (varbinary_column) values (%s)", [None])
        await cur.execute("insert into test_aio_varbinary (varbinary_column) values (%s)", [d])
        await self.connection.commit()

        await cur.execute("select varbinary_column from test_aio_varbinary order by id, varbinary_column")
        self.assertEqual((await cur.fetchone())[0], None)
        self.assertEqual((await cur.fetchone())[0], d)

    async def test_uuid(self):
        cur = self.connection.cursor()
        await cur.execute("""
            DECLARE @myid uniqueidentifier = NEWID();
            SELECT @myid, CONVERT(varchar(255), @myid) AS 'varchar'
        """)
        r = await cur.fetchone()
        self.assertTrue(isinstance(r[0], uuid.UUID))
        self.assertEqual(str(r[0]).upper(), r[1].upper())
        v = r[0]
        await cur.close()

        cur = self.connection.cursor()
        await cur.execute("DECLARE @myid uniqueidentifier = %s; SELECT @myid", [v])
        r = await cur.fetchone()
        self.assertTrue(isinstance(r[0], uuid.UUID))
        self.assertEqual(v, r[0])

    async def test_callproc_no_params(self):
        cur = self.connection.cursor()
        await cur.execute("drop procedure if exists test_aio_callproc_no_params")
        await cur.execute("""
            CREATE PROCEDURE test_aio_callproc_no_params
            AS
                SELECT 1 a, 1.2 b, db_name() c, NULL d,
                    cast(1.25 as money) e,
                    cast(0.125 as float) f, cast(0.25 as real) g
                RETURN 1234
        """)
        await self.connection.commit()

        self.assertEqual(await cur.callproc('test_aio_callproc_no_params'), 1234)

        self.assertEqual(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            [d[0] for d in cur.description]
        )
        self.assertEqual(
            [1, decimal.Decimal('1.2'), 'test', None, decimal.Decimal('1.25'), 0.125, 0.25],
            list(await cur.fetchone())
        )

    async def test_callproc_with_params(self):
        cur = self.connection.cursor()
        await cur.execute("drop procedure if exists test_aio_callproc_with_params")
        await cur.execute("""
            CREATE PROCEDURE test_aio_callproc_with_params
            @INT_VAL int,
            @DECIMAL_VAL decimal(10, 4),
            @STR_VAL nvarchar(50),
            @NULL_VAL nvarchar(50),
            @FLOAT_VAL float
            AS
                SELECT @INT_VAL a, @DECIMAL_VAL b, @STR_VAL c, @NULL_VAL d, @FLOAT_VAL f
        """)
        await self.connection.commit()
        await cur.callproc('test_aio_callproc_with_params', [123, decimal.Decimal('-1.2'), 'ABC', None, 0.125])
        self.assertEqual(
            [123, decimal.Decimal('-1.2'), 'ABC', None, 0.125],
            list(await cur.fetchone())
        )

    async def test_async_iterator(self):
        cur = self.connection.cursor()
        await cur.execute("drop table if exists test_aio_iterator")
        await cur.execute("""
            CREATE TABLE test_aio_iterator(
                id int NOT NULL,
                name varchar(10)
            )
        """)
        await cur.execute("insert into test_aio_iterator values (1, 'foo'), (2, 'bar')")
        await self.connection.commit()

        await cur.execute("select id, name from test_aio_iterator order by id")
        rows = []
        async for r in cur:
            rows.append(r)
        self.assertEqual(rows, [(1, 'foo'), (2, 'bar')])

    async def test_context_managers(self):
        async with await minitds.aio.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
        ) as conn:
            async with conn.cursor() as cur:
                await cur.execute("SELECT 1")
                r = await cur.fetchone()
                self.assertEqual(r, (1,))

    async def test_error(self):
        cur = self.connection.cursor()
        with self.assertRaises(minitds.ProgrammingError):
            await cur.execute("bad sql")


if __name__ == "__main__":
    unittest.main()
