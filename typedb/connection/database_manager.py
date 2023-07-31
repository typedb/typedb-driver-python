#
#   Copyright (C) 2022 Vaticle
#
#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#

from typedb.api.connection.database import DatabaseManager
from typedb.common.exception import TypeDBClientException, DATABASE_DELETED, MISSING_DB_NAME
from typedb.common.streamer import Streamer
from typedb.connection.database import _Database

from typedb.typedb_client_python import Connection as NativeConnection, databases_contains, databases_create, \
    database_manager_new, databases_get, databases_all, database_iterator_next


def _not_blank(name: str) -> str:
    if name in [None, ""] or name.isspace():
        raise TypeDBClientException.of(MISSING_DB_NAME)
    return name


class _DatabaseManagerImpl(DatabaseManager):

    def __init__(self, connection: NativeConnection):
        self._database_manager = database_manager_new(connection)

    def get(self, name: str) -> _Database:
        if self.contains(name):
            return _Database(databases_get(self._database_manager, name))
        else:
            raise TypeDBClientException.of(DATABASE_DELETED, name)

    def contains(self, name: str) -> bool:
        return databases_contains(self._database_manager, _not_blank(name))

    def create(self, name: str) -> None:
        databases_create(self._database_manager, _not_blank(name))

    def all(self) -> list[_Database]:
        return list(map(_Database, Streamer(databases_all(self._database_manager), database_iterator_next)))
