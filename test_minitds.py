#!/usr/bin/env python3
##############################################################################
# The MIT License (MIT)
#
# Copyright (c) 2016-2019 Hajime Nakagami
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
import minitds
import uuid


class TestMiniTds(unittest.TestCase):
    host = os.environ.get('TEST_MINITDS_HOST', 'localhost')
    user = os.environ.get('TEST_MINITDS_USER', 'sa')
    password = os.environ.get('TEST_MINITDS_PASSWORD', 'Secret123')
    database = os.environ.get('TEST_MINITDS_DATABASE', 'test')
    port = int(os.environ.get('TEST_MINITDS_PORT', '1433'))

    def setUp(self):
        self.connection = minitds.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=self.port,
        )

    def tearDown(self):
        self.connection.close()

    def test_basic_types(self):
        cur = self.connection.cursor()

        cur.execute("""
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
            list(cur.fetchone())
        )

    def test_datetime_types(self):
        cur = self.connection.cursor()

        cur.execute("""
            SELECT cast('1967-08-11' as date),
                cast('12:34:56' as time),
                cast('1967-08-11 12:34:56' as datetime)
        """)
        self.assertEqual(
            [datetime.date(1967, 8, 11), datetime.time(12, 34, 56), datetime.datetime(1967, 8, 11, 12, 34, 56)],
            list(cur.fetchone())
        )

    def test_string_types(self):
        cur = self.connection.cursor()

        cur.execute("""
            SELECT
                cast('A' as NVARCHAR(2)),
                cast('B' as NCHAR(2)),
                cast('C' as VARCHAR(2)),
                cast('D' as CHAR(2))
        """)
        self.assertEqual(
            ['A', 'B ', 'C', 'D '],
            list(cur.fetchone())
        )

    def test_bit_type(self):
        cur = self.connection.cursor()

        cur.execute("SELECT cast(1 as BIT), cast(0 as BIT)")
        self.assertEqual(
            [1, 0],
            list(cur.fetchone())
        )

    def test_variant_types(self):
        cur = self.connection.cursor()

        cur.execute("""
            SELECT
                SERVERPROPERTY('Collation'),
                SERVERPROPERTY('CollationID'),
                SERVERPROPERTY('EditionID'),
                SERVERPROPERTY('EngineEdition'),
                SERVERPROPERTY('SqlCharSet'),
                SERVERPROPERTY('ResourceLastUpdateDateTime')
        """)
        r = cur.fetchone()
        self.assertTrue(isinstance(r[0], str))
        self.assertTrue(isinstance(r[1], int))
        self.assertTrue(isinstance(r[2], int))
        self.assertTrue(isinstance(r[3], int))
        self.assertTrue(isinstance(r[4], int))
        self.assertTrue(isinstance(r[5], datetime.datetime))


    def test_autocommit(self):
        cur = self.connection.cursor()
        cur.execute("drop table if exists test_autocommit")
        cur.execute("""
            CREATE TABLE test_autocommit(
                id int IDENTITY(1,1) NOT NULL,
                s varchar(4096)
            )
        """)
        self.connection.commit()

        cur.execute("insert into test_autocommit (s) values ('a')")
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 1)
        self.connection.rollback()
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 0)

        self.connection.set_autocommit(True)
        cur.execute("insert into test_autocommit (s) values ('a')")
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 1)
        self.connection.rollback()
        cur.execute("select count(*) from test_autocommit")
        self.assertEqual(cur.fetchone()[0], 1)

    def test_decimal(self):
        cur = self.connection.cursor()
        cur.execute("drop table if exists test_decimal")
        cur.execute("""
            CREATE TABLE test_decimal(
                id int IDENTITY(1,1) NOT NULL,
                d decimal(10, 4)
            )
        """)
        self.connection.commit()
        d = decimal.Decimal("1.23")
        cur.execute("insert into test_decimal (d) values (%s)", [d])
        cur.execute("select d from test_decimal where d=%s", [d])
        self.assertEqual(cur.fetchone()[0], d)

    def test_varbinary(self):
        cur = self.connection.cursor()
        cur.execute("drop table if exists test_varbinary")
        cur.execute("""
            CREATE TABLE test_varbinary(
                id int IDENTITY(1,1) NOT NULL,
                varbinary_column varbinary(max) null,
                primary key (id)
            )
        """)
        d = b'\x00\x01\x02'
        cur.execute("insert into test_varbinary (varbinary_column) values (%s)", [None])
        cur.execute("insert into test_varbinary (varbinary_column) values (%s)", [d])
        self.connection.commit()

        cur.execute("select varbinary_column from test_varbinary order by id, varbinary_column")
        self.assertEqual(cur.fetchone()[0], None)
        self.assertEqual(cur.fetchone()[0], d)

    def test_uuid(self):
        cur = self.connection.cursor()
        cur.execute("""
            DECLARE @myid uniqueidentifier = NEWID();
            SELECT @myid, CONVERT(varchar(255), @myid) AS 'varchar'
        """)
        r = cur.fetchone()
        self.assertTrue(isinstance(r[0], uuid.UUID))
        self.assertEqual(str(r[0]).upper(), r[1].upper())
        v = r[0]
        cur.close()

        cur = self.connection.cursor()
        cur.execute("DECLARE @myid uniqueidentifier = %s; SELECT @myid", [v])
        r = cur.fetchone()
        self.assertTrue(isinstance(r[0], uuid.UUID))
        self.assertEqual(v, r[0])

    def test_null_ok(self):
        cur = self.connection.cursor()
        cur.execute("drop table if exists test_null_ok")
        cur.execute("""
            CREATE TABLE test_null_ok(
                id int IDENTITY(1,1) NOT NULL,
                a int NOT NULL,
                b int,
                c varchar(4096) NOT NULL,
                d varchar(4096)
            )
        """)
        cur.execute("select id, a, b, c, d from test_null_ok")
        self.assertEqual(
            [False, False, True, False, True],
            [d[6] for d in cur.description]
        )
        cur.execute("insert into test_null_ok (a, c) values (1, 'c')")
        cur.execute("select id, a, b, c, d from test_null_ok")
        self.assertEqual(len(cur.fetchall()), 1)

    def test_large_results(self):
        cur = self.connection.cursor()
        cur.execute("drop table if exists test_large_results")
        cur.execute("""
            CREATE TABLE test_large_results(
                id int IDENTITY(1,1) NOT NULL,
                s varchar(4096)
            )
        """)
        for i in range(30):
            s = "insert into test_large_results (s) values ('%s')" % ("A" * 3000,)
            cur.execute(s)
            self.connection.commit()
        cur.execute("select * from test_large_results")
        self.assertEqual(len(cur.fetchall()), 30)

        cur.execute("drop procedure if exists test_callproc_large_results")
        cur.execute("""
            CREATE PROCEDURE test_callproc_large_results
            AS
                SELECT * FROM test_large_results
        """)
        self.connection.commit()

        cur.callproc('test_callproc_large_results')
        self.assertEqual(len(cur.fetchall()), 30)

    def test_callproc_no_params(self):
        cur = self.connection.cursor()
        cur.execute("drop procedure if exists test_callproc_no_params")
        cur.execute("""
            CREATE PROCEDURE test_callproc_no_params
            AS
                SELECT 1 a, 1.2 b, db_name() c, NULL d,
                    cast(1.25 as money) e,
                    cast(0.125 as float) f, cast(0.25 as real) g
                RETURN 1234
        """)
        self.connection.commit()

        self.assertEqual(cur.callproc('test_callproc_no_params'), 1234)

        self.assertEqual(
            ['a', 'b', 'c', 'd', 'e', 'f', 'g'],
            [d[0] for d in cur.description]
        )
        self.assertEqual(
            [1, decimal.Decimal('1.2'), 'test', None, decimal.Decimal('1.25'), 0.125, 0.25],
            list(cur.fetchone())
        )

    def test_callproc_with_params(self):
        cur = self.connection.cursor()
        cur.execute("drop procedure if exists test_callproc_with_params")
        cur.execute("""
            CREATE PROCEDURE test_callproc_with_params
            @INT_VAL int,
            @DECIMAL_VAL decimal(10, 4),
            @STR_VAL nvarchar(50),
            @NULL_VAL nvarchar(50),
            @FLOAT_VAL float
            AS
                SELECT @INT_VAL a, @DECIMAL_VAL b, @STR_VAL c, @NULL_VAL d, @FLOAT_VAL f
        """)
        self.connection.commit()
        cur.callproc('test_callproc_with_params', [123, decimal.Decimal('-1.2'), 'ABC', None, 0.125])
        self.assertEqual(
            [123, decimal.Decimal('-1.2'), 'ABC', None, 0.125],
            list(cur.fetchone())
        )

    def test_error(self):
        cur = self.connection.cursor()
        with self.assertRaises(minitds.ProgrammingError):
            cur.execute("bad sql")


if __name__ == "__main__":
    unittest.main()
