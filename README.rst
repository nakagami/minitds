=============
minitds
=============

Yet another and pure python MS SQLServer database driver.

Support platform
-----------------

- Python 3.11 or later

Support database
------------------

We are testing it on MS SQLServer 2025 on Ubuntu.
But we think it works on older versions.

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

AsyncIO::

   import asyncio
   import minitds.aio

   async def main():
       async with await minitds.aio.connect(
           host='localhost',
           user='sa',
           password='secret',
           database='database_name'
       ) as conn:
           async with conn.cursor() as cur:
               await cur.execute('select foo, bar from baz')
               async for r in cur:
                   print(r[0], r[1])

   asyncio.run(main())


Restrictions
----------------

- Support connecting through SQL Server Authentication only.

Protocol Specification
------------------------

https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-tds/b46a581a-39de-4745-b076-ec4dbb7d13ec
