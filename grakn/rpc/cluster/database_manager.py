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
from abc import abstractmethod
from typing import Dict, List

import grakn_protocol.protobuf.cluster.database_pb2 as database_proto

from grakn.common.exception import GraknClientException
from grakn.rpc.cluster.database import _DatabaseClusterRPC
from grakn.rpc.cluster.server_address import ServerAddress
from grakn.rpc.database import DatabaseCluster
from grakn.rpc.database_manager import DatabaseManager, _DatabaseManagerRPC


class DatabaseManagerCluster(DatabaseManager):

    @abstractmethod
    def get(self, name: str) -> DatabaseCluster:
        pass

    @abstractmethod
    def all(self) -> List[DatabaseCluster]:
        pass


class _DatabaseManagerClusterRPC(DatabaseManagerCluster):

    def __init__(self, client, database_managers: Dict[ServerAddress, "_DatabaseManagerRPC"]):
        self._client = client
        self._database_managers = database_managers

    def contains(self, name: str) -> bool:
        errors = []
        for database_manager in self._database_managers.values():
            try:
                return database_manager.contains(name)
            except GraknClientException as e:
                errors.append(e)
        raise GraknClientException("Attempted connecting to all cluster members, but the following errors occurred: " + str([str(e) for e in errors]))

    def create(self, name: str) -> None:
        for database_manager in self._database_managers.values():
            if not database_manager.contains(name):
                database_manager.create(name)

    def get(self, name: str) -> DatabaseCluster:
        errors = []
        for address in self._database_managers:
            try:
                database_get_req = database_proto.Database.Get.Req()
                database_get_req.name = name
                res = self._client.grakn_cluster_grpc_stub(address).database_get(database_get_req)
                return _DatabaseClusterRPC.of(res.database, self)
            except GraknClientException as e:
                errors.append(e)
        raise GraknClientException("Attempted connecting to all cluster members, but the following errors occurred: " + str([str(e) for e in errors]))

    def all(self) -> List[DatabaseCluster]:
        errors = []
        for address in self._database_managers:
            try:
                res = self._client.grakn_cluster_grpc_stub(address).database_all(database_proto.Database.All.Req())
                return [_DatabaseClusterRPC.of(db, self) for db in res.databases]
            except GraknClientException as e:
                errors.append(e)
        raise GraknClientException("Attempted connecting to all cluster members, but the following errors occurred: " + str([str(e) for e in errors]))

    def database_managers(self) -> Dict[ServerAddress, _DatabaseManagerRPC]:
        return self._database_managers
