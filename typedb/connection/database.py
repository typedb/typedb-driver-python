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
from typing import Optional

from typedb.api.connection.database import Database
from typedb.common.exception import TypeDBClientException, DATABASE_DELETED

from typedb.typedb_client_python import Database as DatabaseFfi, database_get_name, database_schema, database_delete, database_rule_schema, database_type_schema, \
    ReplicaInfo, ReplicaInfoIterator, replica_info_get_address, replica_info_is_primary, replica_info_is_preferred, replica_info_get_term, \
    database_get_replicas_info, database_get_primary_replica_info, database_get_preferred_replica_info, replica_info_iterator_next


class _DatabaseImpl(Database):

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

    def replicas(self) -> set["_DatabaseImpl.Replica"]:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        replica_infos: list[ReplicaInfo] = []
        repl_iter = database_get_replicas_info(self._database)
        while replica := replica_info_iterator_next(repl_iter):
            replica_infos.append(replica)
        return set(_DatabaseImpl.Replica(replica_info) for replica_info in replica_infos)

    def primary_replica(self) -> Optional["_DatabaseImpl.Replica"]:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        if res := database_get_primary_replica_info(self._database):
            return _DatabaseImpl.Replica(res)
        return None

    def preferred_replica(self) -> Optional["_DatabaseImpl.Replica"]:
        if not self._database.thisown:
            raise TypeDBClientException.of(DATABASE_DELETED, self._name)
        if res := database_get_preferred_replica_info(self._database):
            return _DatabaseImpl.Replica(res)
        return None

    def __str__(self):
        return self.name()

    class Replica(Database.Replica):

        def __init__(self, replica_info: ReplicaInfo):
            self._info = replica_info

        def database(self) -> "Database":
            pass

        def address(self) -> str:
            return replica_info_get_address(self._info)

        def is_primary(self) -> bool:
            return replica_info_is_primary(self._info)

        def is_preferred(self) -> bool:
            return replica_info_is_preferred(self._info)

        def term(self) -> int:
            return replica_info_get_term(self._info)
