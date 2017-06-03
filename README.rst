=============
minitds
=============

Yet another Python SQLServer database driver.

Support platform
-----------------

- Python 3.5+

Support database
------------------

- MS SQLServer 2012, 2014, 2016
- Microsoft Azure SQL Database


Installation
-----------------

::

    $ pip install minitds

Example
-----------------

Query::

   import minitds
   conn = minitds.connect(host='localhost',
                       user='sa',
                       password='secret',
                       database='database_name')
   cur = conn.cursor()
   cur.execute('select foo, bar from baz')
   for r in cur.fetchall():
      print(r[0], r[1])
   conn.close()

Execute Procedure::

   import minitds
   conn = minitds.connect(host='localhost',
                       user='sa',
                       password='secret',
                       database='database_name')
   cur = conn.cursor()
   cur.callproc('something_proc', [123, 'ABC'])
   conn.close()


Restrictions
----------------

- Support SQL Server User Authentication only.
