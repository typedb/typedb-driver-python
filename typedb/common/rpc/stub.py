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

from abc import ABC
from typing import TypeVar, Callable, Iterator


import typedb_protocol.common.session_pb2 as session_proto
import typedb_protocol.common.transaction_pb2 as transaction_proto
import typedb_protocol.core.core_database_pb2 as core_database_proto
import typedb_protocol.core.core_service_pb2_grpc as core_service_proto

from grpc import Channel, RpcError

from typedb.common.exception import TypeDBClientException

T = TypeVar('T')


def resilient_call(function: Callable[[], T]) -> T:
    try:
        # TODO actually implement forced gRPC to reconnected rapidly, which provides resilience
        return function()
    except RpcError as e:
        raise TypeDBClientException.of_rpc(e)

class TypeDBStub(ABC):

    def __init__(self, channel: Channel, stub: core_service_proto.TypeDBStub):
        self._channel = channel
        self._stub = stub

    def databases_contains(self, req: core_database_proto.CoreDatabaseManager.Contains.Req) -> core_database_proto.CoreDatabaseManager.Contains.Res:
        return resilient_call(lambda: self._stub.databases_contains(req))

    def databases_create(self, req: core_database_proto.CoreDatabaseManager.Create.Req) -> core_database_proto.CoreDatabaseManager.Create.Res:
        return resilient_call(lambda: self._stub.databases_create(req))

    def databases_all(self, req: core_database_proto.CoreDatabaseManager.All.Req) -> core_database_proto.CoreDatabaseManager.All.Res:
        return resilient_call(lambda: self._stub.databases_all(req))

    def database_schema(self, req: core_database_proto.CoreDatabase.Schema.Req) -> core_database_proto.CoreDatabase.Schema.Res:
        return resilient_call(lambda: self._stub.database_schema(req))

    def database_delete(self, req: core_database_proto.CoreDatabase.Delete.Req) -> core_database_proto.CoreDatabase.Delete.Res:
        return resilient_call(lambda: self._stub.database_delete(req))

    def session_open(self, req: session_proto.Session.Open.Req) -> session_proto.Session.Open.Res:
        return resilient_call(lambda: self._stub.session_open(req))

    def session_close(self, req: session_proto.Session.Close.Req) -> session_proto.Session.Close.Res:
        return resilient_call(lambda: self._stub.session_close(req))

    def session_pulse(self, req: session_proto.Session.Pulse.Req) -> session_proto.Session.Pulse.Res:
        return resilient_call(lambda: self._stub.session_pulse(req))

    def transaction(self, request_iterator: Iterator[transaction_proto.Transaction.Client]) -> Iterator[transaction_proto.Transaction.Server]:
        return resilient_call(lambda: self._stub.transaction(request_iterator))

