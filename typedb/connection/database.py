#
# Copyright (C) 2022 Vaticle
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#

from typedb.api.connection.database import Database
from typedb.common.exception import TypeDBClientException, DATABASE_DELETED

from typedb.typedb_client_python import Database as DatabaseFfi, database_get_name, database_schema, database_delete, database_rule_schema, database_type_schema


class _TypeDBDatabaseImpl(Database):

    def __init__(self, database: DatabaseFfi):
        self._database = database
        self._name = database_get_name(database)

    def name(self) -> str:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        return self._name

    def schema(self) -> str:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        return database_schema(self._database)

    def rule_schema(self) -> str:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        return database_rule_schema(self._database)

    def type_schema(self) -> str:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        return database_type_schema(self._database)

    def delete(self) -> None:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        database_delete(self._database)

    def __str__(self):
        return self.name()
