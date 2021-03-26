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
from typing import Dict, List, TYPE_CHECKING

from grakn.api.database import ClusterDatabaseManager
from grakn.cluster.database import _ClusterDatabase
from grakn.common.exception import GraknClientException, CLUSTER_ALL_NODES_FAILED
from grakn.common.rpc.request_builder import cluster_database_manager_get_req, cluster_database_manager_all_req
from grakn.core.database_manager import _CoreDatabaseManager

if TYPE_CHECKING:
    from grakn.cluster.client import _ClusterClient


class _ClusterDatabaseManager(ClusterDatabaseManager):

    def __init__(self, client: "_ClusterClient"):
        self._client = client
        self._database_mgrs: Dict[str, _CoreDatabaseManager] = {addr: client.databases() for (addr, client) in client.core_clients().items()}

    def contains(self, name: str) -> bool:
        errors = []
        for address in self._database_mgrs:
            try:
                return self._database_mgrs[address].contains(name)
            except GraknClientException as e:
                errors.append("- %s: %s\n" % (address, e))
        raise GraknClientException.of(CLUSTER_ALL_NODES_FAILED, str([str(e) for e in errors]))

    def create(self, name: str) -> None:
        for database_manager in self._database_mgrs.values():
            if not database_manager.contains(name):
                database_manager.create(name)

    def get(self, name: str) -> _ClusterDatabase:
        errors = []
        for address in self._database_mgrs:
            try:
                res = self._client.stub(address).databases_get(cluster_database_manager_get_req(name))
                return _ClusterDatabase.of(res.database, self)
            except GraknClientException as e:
                errors.append("- %s: %s\n" % (address, e))
        raise GraknClientException.of(CLUSTER_ALL_NODES_FAILED, str([str(e) for e in errors]))

    def all(self) -> List[_ClusterDatabase]:
        errors = []
        for address in self._database_mgrs:
            try:
                res = self._client.stub(address).databases_all(cluster_database_manager_all_req())
                return [_ClusterDatabase.of(db, self) for db in res.databases]
            except GraknClientException as e:
                errors.append("- %s: %s\n" % (address, e))
        raise GraknClientException.of(CLUSTER_ALL_NODES_FAILED, str([str(e) for e in errors]))

    def database_mgrs(self) -> Dict[str, _CoreDatabaseManager]:
        return self._database_mgrs
