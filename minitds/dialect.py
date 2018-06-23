# Copyright (C) 2005-2018 the SQLAlchemy authors and contributors
# <see AUTHORS file>
#
# This module is part of SQLAlchemy and is released under
# the MIT License: http://www.opensource.org/licenses/mit-license.php

import re
from sqlalchemy.dialects.mssql.base import MSDialect, MSIdentifierPreparer
from sqlalchemy import types as sqltypes, util

"""
sqlalchemy minitds dialect.

::

    from sqlalchemy.dialects import registry
    registry.register("mssql.minitds", "minitds.dialect", "MSDialect_minitds")
    
    engine = create_engine('mssql+minitds://user:password@host/database')

"""


class MSIdentifierPreparer_minitds(MSIdentifierPreparer):
    def __init__(self, dialect):
        super(MSIdentifierPreparer_minitds, self).__init__(dialect)
        self._double_percents = False


class MSDialect_minitds(MSDialect):
    supports_native_decimal = True
    driver = 'minitds'

    preparer = MSIdentifierPreparer_minitds

    colspecs = util.update_copy(
        MSDialect.colspecs,
        {
            sqltypes.Numeric: sqltypes.Numeric,
            sqltypes.Float: sqltypes.Float,
        }
    )

    @classmethod
    def dbapi(cls):
        return __import__('minitds')

    def _get_server_version_info(self, connection):
        vers = connection.scalar("select @@version")
        m = re.match(
            r"Microsoft .*? - (\d+).(\d+).(\d+).(\d+)", vers)
        if m:
            return tuple(int(x) for x in m.group(1, 2, 3, 4))
        else:
            return None

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user')
        opts.update(url.query)
        return [[], opts]

    def is_disconnect(self, e, connection, cursor):
        return not connection.is_connect()


dialect = MSDialect_minitds
