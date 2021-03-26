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
from typing import List

from grpc import Channel

from grakn.api.database import DatabaseManager
from grakn.common.exception import GraknClientException, DB_DOES_NOT_EXIST, MISSING_DB_NAME
from grakn.common.rpc.request_builder import core_database_manager_contains_req, core_database_manager_create_req, \
    core_database_manager_all_req
from grakn.common.rpc.stub import GraknCoreStub
from grakn.core.database import _CoreDatabase


def _not_blank(name: str) -> str:
    if name in [None, ""] or name.isspace():
        raise GraknClientException.of(MISSING_DB_NAME)
    return name


class _CoreDatabaseManager(DatabaseManager):

    def __init__(self, channel: Channel):
        self._stub = GraknCoreStub(channel)

    def get(self, name: str) -> _CoreDatabase:
        if self.contains(name):
            return _CoreDatabase(self._stub, name)
        else:
            raise GraknClientException.of(DB_DOES_NOT_EXIST, name)

    def contains(self, name: str) -> bool:
        return self._stub.databases_contains(core_database_manager_contains_req(_not_blank(name))).contains

    def create(self, name: str) -> None:
        self._stub.databases_create(core_database_manager_create_req(_not_blank(name)))

    def all(self) -> List[_CoreDatabase]:
        databases: List[str] = self._stub.databases_all(core_database_manager_all_req()).names
        return [_CoreDatabase(self._stub, name) for name in databases]

    def stub(self) -> GraknCoreStub:
        return self._stub
