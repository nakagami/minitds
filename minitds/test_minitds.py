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
import unittest
import decimal
import minitds


class TestMiniTds(unittest.TestCase):
    host = 'localhost'
    user = 'sa'
    password = 'secret'
    database = 'test'

    def setUp(self):
        self.connection = minitds.connect(
            host=self.host,
            user=self.user,
            password=self.password,
            database=self.database,
            port=14333,
        )

    def tearDown(self):
        self.connection.close()

    def test_basic_type(self):
        cur = self.connection.cursor()
        cur.execute("select 1 n, 1.2, db_name()")
        self.assertEqual(['n', '', ''], [d[0] for d in cur.description])
        self.assertEqual([1, decimal.Decimal('1.2'), 'test'], list(cur.fetchone()))

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


if __name__ == "__main__":
    unittest.main()
