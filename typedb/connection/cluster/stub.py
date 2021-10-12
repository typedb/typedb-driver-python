#
#   Copyright (C) 2021 Vaticle
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
from typing import Iterator
from typing import TypeVar, Callable

import typedb_protocol.cluster.cluster_database_pb2 as cluster_database_proto
import typedb_protocol.cluster.cluster_server_pb2 as cluster_server_proto
import typedb_protocol.cluster.cluster_service_pb2_grpc as cluster_service_proto
import typedb_protocol.cluster.cluster_user_pb2 as cluster_user_proto
import typedb_protocol.common.session_pb2 as session_proto
import typedb_protocol.common.transaction_pb2 as transaction_proto
import typedb_protocol.core.core_database_pb2 as core_database_proto
import typedb_protocol.core.core_service_pb2_grpc as core_service_proto
from grpc import Channel, RpcError

from typedb.api.connection.credential import TypeDBCredential
from typedb.common.exception import CLUSTER_TOKEN_CREDENTIAL_INVALID, TypeDBClientException, UNABLE_TO_CONNECT
from typedb.common.rpc.request_builder import cluster_user_token_req
from typedb.common.rpc.stub import TypeDBStub

T = TypeVar('T')


class _ClusterServerStub(TypeDBStub):

    def __init__(self, channel: Channel, credential: TypeDBCredential):
        super(_ClusterServerStub, self).__init__()
        self._credential = credential
        self._channel = channel
        self._stub = core_service_proto.TypeDBStub(channel)
        self._cluster_stub = cluster_service_proto.TypeDBClusterStub(channel)
        self._token = None
        try:
            res = self._cluster_stub.user_token(cluster_user_token_req(self._credential.username()))
            self._token = res.token
        except RpcError as e:
            e2 = TypeDBClientException.of_rpc(e)
            if e2.error_message is not None and e2.error_message is not UNABLE_TO_CONNECT:
                raise e2

    def servers_all(self, req: cluster_server_proto.ServerManager.All.Req) -> cluster_server_proto.ServerManager.All.Res:
        return self.may_renew_token(lambda: self._cluster_stub.servers_all(req))

    def users_all(self, req: cluster_user_proto.ClusterUserManager.All.Req) -> cluster_user_proto.ClusterUserManager.All.Res:
        return self.may_renew_token(lambda: self._cluster_stub.users_all(req))

    def users_contains(self, req: cluster_user_proto.ClusterUserManager.Contains.Req) -> cluster_user_proto.ClusterUserManager.Contains.Res:
        return self.may_renew_token(lambda: self._cluster_stub.users_contains(req))

    def users_create(self, req: cluster_user_proto.ClusterUserManager.Create.Req) -> cluster_user_proto.ClusterUserManager.Create.Res:
        return self.may_renew_token(lambda: self._cluster_stub.users_create(req))

    def user_password(self, req: cluster_user_proto.ClusterUser.Delete.Req) -> cluster_user_proto.ClusterUser.Delete.Res:
        return self.may_renew_token(lambda: self._cluster_stub.user_password(req))

    def user_delete(self, req: cluster_user_proto.ClusterUser.Delete.Req) -> cluster_user_proto.ClusterUser.Delete.Res:
        return self.may_renew_token(lambda: self._cluster_stub.user_delete(req))

    def cluster_databases_all(self, req: cluster_database_proto.ClusterDatabaseManager.All.Req) -> cluster_database_proto.ClusterDatabaseManager.All.Res:
        return self.may_renew_token(lambda: self._cluster_stub.databases_all(req))

    def databases_all(self, req: core_database_proto.CoreDatabaseManager.All.Req) -> core_database_proto.CoreDatabaseManager.All.Res:
        return self.may_renew_token(self.resilient_call(lambda: self.stub().databases_all(req)))

    def databases_get(self, req: cluster_database_proto.ClusterDatabaseManager.Get.Req) -> cluster_database_proto.ClusterDatabaseManager.Get.Res:
        return self.may_renew_token(lambda: self._cluster_stub.databases_get(req))

    def databases_contains(self, req: core_database_proto.CoreDatabaseManager.Contains.Req) -> core_database_proto.CoreDatabaseManager.Contains.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).databases_contains(req))

    def databases_create(self, req: core_database_proto.CoreDatabaseManager.Create.Req) -> core_database_proto.CoreDatabaseManager.Create.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).databases_create(req))

    def database_schema(self, req: core_database_proto.CoreDatabase.Schema.Req) -> core_database_proto.CoreDatabase.Schema.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).database_schema(req))

    def database_delete(self, req: core_database_proto.CoreDatabase.Delete.Req) -> core_database_proto.CoreDatabase.Delete.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).database_delete(req))

    def session_open(self, req: session_proto.Session.Open.Req) -> session_proto.Session.Open.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).session_open(req))

    def session_close(self, req: session_proto.Session.Close.Req) -> session_proto.Session.Close.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).session_close(req))

    def session_pulse(self, req: session_proto.Session.Pulse.Req) -> session_proto.Session.Pulse.Res:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).session_pulse(req))

    def transaction(self, request_iterator: Iterator[transaction_proto.Transaction.Client]) -> Iterator[transaction_proto.Transaction.Server]:
        return self.may_renew_token(lambda: super(_ClusterServerStub, self).transaction(request_iterator))

    def channel(self) -> Channel:
        return self._channel

    def stub(self) -> TypeDBStub:
        return self._stub

    def token(self):
        return self._token

    def may_renew_token(self, function: Callable[[], T]) -> T:
        try:
            return self.resilient_call(function)
        except TypeDBClientException as e:
            if e.error_message is not None and e.error_message is CLUSTER_TOKEN_CREDENTIAL_INVALID:
                self._token = None
                res = self._cluster_stub.user_token(cluster_user_token_req(self._credential.username()))
                self._token = res.token
                try:
                    return self.resilient_call(function)
                except RpcError as e2:
                    raise TypeDBClientException.of_rpc(e2)
            else:
                raise e
