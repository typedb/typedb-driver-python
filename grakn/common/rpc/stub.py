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
from typing import TypeVar, Callable, Iterator

import grakn_protocol.cluster.cluster_database_pb2 as cluster_database_proto
import grakn_protocol.cluster.cluster_server_pb2 as cluster_server_proto
import grakn_protocol.cluster.cluster_service_pb2_grpc as cluster_service_proto
import grakn_protocol.common.session_pb2 as session_proto
import grakn_protocol.common.transaction_pb2 as transaction_proto
import grakn_protocol.core.core_database_pb2 as core_database_proto
import grakn_protocol.core.core_service_pb2_grpc as core_service_proto

from grpc import Channel, RpcError

from grakn.common.exception import GraknClientException

T = TypeVar('T')


def resilient_call(function: Callable[[], T]) -> T:
    try:
        return function()
    except RpcError as e:
        raise GraknClientException.of_rpc(e)


class GraknGrpcStub:

    @staticmethod
    def core(channel: Channel) -> "GraknCoreStub":
        return GraknCoreStub(channel)

    @staticmethod
    def cluster(channel: Channel) -> "GraknClusterStub":
        return GraknClusterStub(channel)


class GraknCoreStub(GraknGrpcStub):

    def __init__(self, channel: Channel):
        self._stub = core_service_proto.GraknCoreStub(channel)

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


class GraknClusterStub(GraknGrpcStub):

    def __init__(self, channel: Channel):
        self._stub = cluster_service_proto.GraknClusterStub(channel)

    def servers_all(self, req: cluster_server_proto.ServerManager.All.Req) -> cluster_server_proto.ServerManager.All.Res:
        return resilient_call(lambda: self._stub.servers_all(req))

    def databases_get(self, req: cluster_database_proto.ClusterDatabaseManager.Get.Req) -> cluster_database_proto.ClusterDatabaseManager.Get.Res:
        return resilient_call(lambda: self._stub.databases_get(req))

    def databases_all(self, req: cluster_database_proto.ClusterDatabaseManager.All.Req) -> cluster_database_proto.ClusterDatabaseManager.All.Res:
        return resilient_call(lambda: self._stub.databases_all(req))
