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
from typing import Iterator
from typing import TypeVar, Callable

import typedb_protocol.common.session_pb2 as session_proto
import typedb_protocol.common.transaction_pb2 as transaction_proto
import typedb_protocol.core.core_database_pb2 as core_database_proto
import typedb_protocol.core.core_service_pb2_grpc as core_service_proto
from grpc import Channel, RpcError

from typedb.common.exception import TypeDBClientException

T = TypeVar('T')


class TypeDBStub(ABC):

    def databases_contains(self, req: core_database_proto.CoreDatabaseManager.Contains.Req) -> core_database_proto.CoreDatabaseManager.Contains.Res:
        pass

    def databases_create(self, req: core_database_proto.CoreDatabaseManager.Create.Req) -> core_database_proto.CoreDatabaseManager.Create.Res:
        pass

    def databases_all(self, req: core_database_proto.CoreDatabaseManager.All.Req) -> core_database_proto.CoreDatabaseManager.All.Res:
        pass

    def database_schema(self, req: core_database_proto.CoreDatabase.Schema.Req) -> core_database_proto.CoreDatabase.Schema.Res:
        pass

    def database_delete(self, req: core_database_proto.CoreDatabase.Delete.Req) -> core_database_proto.CoreDatabase.Delete.Res:
        pass

    def session_open(self, req: session_proto.Session.Open.Req) -> session_proto.Session.Open.Res:
        pass

    def session_close(self, req: session_proto.Session.Close.Req) -> session_proto.Session.Close.Res:
        pass

    def session_pulse(self, req: session_proto.Session.Pulse.Req) -> session_proto.Session.Pulse.Res:
        pass

    def transaction(self, request_iterator: Iterator[transaction_proto.Transaction.Client]) -> Iterator[transaction_proto.Transaction.Server]:
        pass

    def channel(self) -> Channel:
        pass

    def stub(self) -> core_service_proto.TypeDBStub:
        pass

    def resilient_call(self, function: Callable[[], T]) -> T:
        try:
            # TODO actually implement forced gRPC to reconnected rapidly, which provides resilience
            return function()
        except RpcError as e:
            raise TypeDBClientException.of_rpc(e)
