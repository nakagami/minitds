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

It can install as package or module.

Install as a package

::

    $ pip install minitds

Install as a module

::

    $ cd $(PROJECT_HOME)
    $ wget https://github.com/nakagami/minitds/raw/master/minitds/minitds.py

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

Restrictions
----------------

- Support SQL Server User Authentication only.
- Not support Stored Procedure call now.
