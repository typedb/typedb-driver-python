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
import typedb_protocol.cluster.cluster_database_pb2 as cluster_database_proto
import typedb_protocol.cluster.cluster_server_pb2 as cluster_server_proto
import typedb_protocol.cluster.cluster_service_pb2_grpc as cluster_service_proto
import typedb_protocol.cluster.cluster_user_pb2 as cluster_user_proto
import typedb_protocol.core.core_service_pb2_grpc as core_service_proto
from grpc import Channel

from typedb.common.rpc.stub import TypeDBStub, resilient_call


class _ClusterServerStub(TypeDBStub):

    def __init__(self, channel: Channel, stub: core_service_proto.TypeDBStub, clusterStub: cluster_service_proto.TypeDBClusterStub):
        super(_ClusterServerStub, self).__init__(channel, stub)
        self._clusterStub = clusterStub

    @staticmethod
    def create(channel: Channel):
        return _ClusterServerStub(
            channel,
            core_service_proto.TypeDBStub(channel),
            cluster_service_proto.TypeDBClusterStub(channel)
        )

    def servers_all(self, req: cluster_server_proto.ServerManager.All.Req) -> cluster_server_proto.ServerManager.All.Res:
        return resilient_call(lambda: self._clusterStub.servers_all(req))

    def databases_get(self, req: cluster_database_proto.ClusterDatabaseManager.Get.Req) -> cluster_database_proto.ClusterDatabaseManager.Get.Res:
        return resilient_call(lambda: self._clusterStub.databases_get(req))

    def databases_all(self, req: cluster_database_proto.ClusterDatabaseManager.All.Req) -> cluster_database_proto.ClusterDatabaseManager.All.Res:
        return resilient_call(lambda: self._clusterStub.databases_all(req))

    def users_all(self, req: cluster_user_proto.ClusterUserManager.All.Req) -> cluster_user_proto.ClusterUserManager.All.Res:
        return resilient_call(lambda: self._clusterStub.users_all(req))

    def users_contains(self, req: cluster_user_proto.ClusterUserManager.Contains.Req) -> cluster_user_proto.ClusterUserManager.Contains.Res:
        return resilient_call(lambda: self._clusterStub.users_contains(req))

    def users_create(self, req: cluster_user_proto.ClusterUserManager.Create.Req) -> cluster_user_proto.ClusterUserManager.Create.Res:
        return resilient_call(lambda: self._clusterStub.users_create(req))

    def user_password(self, req: cluster_user_proto.ClusterUser.Delete.Req) -> cluster_user_proto.ClusterUser.Delete.Res:
        return resilient_call(lambda: self._clusterStub.user_password(req))

    def user_delete(self, req: cluster_user_proto.ClusterUser.Delete.Req) -> cluster_user_proto.ClusterUser.Delete.Res:
        return resilient_call(lambda: self._clusterStub.user_delete(req))

