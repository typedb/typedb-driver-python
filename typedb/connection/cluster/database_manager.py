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
from typing import Dict, List, TYPE_CHECKING, Callable, TypeVar

from typedb.api.connection.database import ClusterDatabaseManager
from typedb.connection.cluster.database import _ClusterDatabase, _FailsafeTask
from typedb.common.exception import TypeDBClientException, CLUSTER_ALL_NODES_FAILED, CLUSTER_REPLICA_NOT_PRIMARY, \
    DB_DOES_NOT_EXIST
from typedb.common.rpc.request_builder import cluster_database_manager_get_req, cluster_database_manager_all_req
from typedb.connection.cluster.stub import _ClusterServerStub
from typedb.connection.database_manager import _TypeDBDatabaseManagerImpl

T = TypeVar("T")

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient


class _ClusterDatabaseManager(ClusterDatabaseManager):

    def __init__(self, client: "_ClusterClient"):
        self._client = client
        self._database_mgrs: Dict[str, _TypeDBDatabaseManagerImpl] = {addr: client.databases() for (addr, client) in client._cluster_server_clients().items()}

    def contains(self, name: str) -> bool:
        return self._failsafe_task(name, lambda stub, core_db_mgr: core_db_mgr.contains(name))

    def create(self, name: str) -> None:
        self._failsafe_task(name, lambda stub, core_db_mgr: core_db_mgr.create(name))

    def get(self, name: str) -> _ClusterDatabase:
        return self._failsafe_task(name, lambda stub, core_db_mgr: self._get_database_task(name, stub))

    def _get_database_task(self, name: str, stub: _ClusterServerStub):
        if self.contains(name):
            res = stub.databases_get(cluster_database_manager_get_req(name))
            return _ClusterDatabase.of(res.database, self._client)
        raise TypeDBClientException.of(DB_DOES_NOT_EXIST, name)

    def all(self) -> List[_ClusterDatabase]:
        errors = []
        for address in self._database_mgrs:
            try:
                res = self._client._stub(address).databases_all(cluster_database_manager_all_req())
                return [_ClusterDatabase.of(db, self._client) for db in res.databases]
            except TypeDBClientException as e:
                errors.append("- %s: %s\n" % (address, e))
        raise TypeDBClientException.of(CLUSTER_ALL_NODES_FAILED, str([str(e) for e in errors]))

    def database_mgrs(self) -> Dict[str, _TypeDBDatabaseManagerImpl]:
        return self._database_mgrs

    def _failsafe_task(self, name: str, task: Callable[[_ClusterServerStub, _TypeDBDatabaseManagerImpl], T]):
        failsafe_task = _DatabaseManagerFailsafeTask(self._client, name, task)
        try:
            return failsafe_task.run_any_replica()
        except TypeDBClientException as e:
            if e.error_message == CLUSTER_REPLICA_NOT_PRIMARY:
                return failsafe_task.run_primary_replica()
            raise e


class _DatabaseManagerFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", database: str, task: Callable[[_ClusterServerStub, _TypeDBDatabaseManagerImpl], T]):
        super().__init__(client, database)
        self.task = task

    def run(self, replica: _ClusterDatabase.Replica) -> T:
        return self.task(self.client._stub(replica.address()), self.client._cluster_server_client(replica.address()).databases())
