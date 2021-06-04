#
# Copyright (C) 2021 Vaticle
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
from typedb.common.rpc.request_builder import core_database_schema_req, core_database_delete_req
from typedb.common.rpc.stub import TypeDBStub


class _TypeDBDatabaseImpl(Database):

    def __init__(self, stub: TypeDBStub, name: str):
        self._name = name
        self._stub = stub

    def name(self) -> str:
        return self._name

    def schema(self) -> str:
        return self._stub.database_schema(core_database_schema_req(self._name)).schema

    def delete(self) -> None:
        self._stub.database_delete(core_database_delete_req(self._name))

    def __str__(self):
        return self._name
