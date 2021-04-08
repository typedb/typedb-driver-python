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
from typing import Iterable, Dict, Set

from grakn.api.client import GraknClusterClient
from grakn.api.options import GraknOptions, GraknClusterOptions
from grakn.api.session import SessionType
from grakn.cluster.database import _ClusterDatabase
from grakn.cluster.database_manager import _ClusterDatabaseManager
from grakn.cluster.failsafe_task import _FailsafeTask
from grakn.cluster.session import _ClusterSession
from grakn.common.exception import GraknClientException, UNABLE_TO_CONNECT, CLUSTER_UNABLE_TO_CONNECT
from grakn.common.rpc.request_builder import cluster_server_manager_all_req
from grakn.common.rpc.stub import GraknClusterStub
from grakn.core.client import _CoreClient


class _ClusterClient(GraknClusterClient):

    def __init__(self, addresses: Iterable[str], parallelisation: int = None):
        self._core_clients: Dict[str, _CoreClient] = {addr: _CoreClient(addr, parallelisation) for addr in self._fetch_server_addresses(addresses)}
        self._stubs = {addr: GraknClusterStub(client.channel()) for (addr, client) in self._core_clients.items()}
        self._database_managers = _ClusterDatabaseManager(self)
        self._cluster_databases: Dict[str, _ClusterDatabase] = {}
        self._is_open = True
        print("Cluster client created")

    def _fetch_server_addresses(self, addresses: Iterable[str]) -> Set[str]:
        for address in addresses:
            try:
                with _CoreClient(address) as client:
                    print("Fetching list of cluster servers from %s..." % address)
                    grakn_cluster_stub = GraknClusterStub(client.channel())
                    res = grakn_cluster_stub.servers_all(cluster_server_manager_all_req())
                    members = {srv.address for srv in res.servers}
                    print("The cluster servers are %s" % [str(member) for member in members])
                    return members
            except GraknClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Fetching cluster servers from %s failed. %s" % (address, str(e)))
                else:
                    raise e
        raise GraknClientException.of(CLUSTER_UNABLE_TO_CONNECT, ",".join(addresses))

    def is_open(self) -> bool:
        return self._is_open

    def databases(self) -> _ClusterDatabaseManager:
        return self._database_managers

    def session(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        if not options:
            options = GraknOptions.cluster()
        return self._session_any_replica(database, session_type, options) if getattr(options, "read_any_replica", False) else self._session_primary_replica(database, session_type, options)

    def _session_primary_replica(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_primary_replica()

    def _session_any_replica(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_any_replica()

    def database_by_name(self) -> Dict[str, _ClusterDatabase]:
        return self._cluster_databases

    def core_clients(self) -> Dict[str, _CoreClient]:
        return self._core_clients

    def cluster_members(self) -> Set[str]:
        return set(self._core_clients.keys())

    def core_client(self, address: str) -> _CoreClient:
        return self._core_clients.get(address)

    def stub(self, address: str) -> GraknClusterStub:
        return self._stubs.get(address)

    def close(self) -> None:
        for client in self._core_clients.values():
            client.close()
        self._is_open = False

    def is_cluster(self) -> bool:
        return True

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False


class _OpenSessionFailsafeTask(_FailsafeTask):

    def __init__(self, database: str, session_type: SessionType, options: GraknClusterOptions, client: _ClusterClient):
        super().__init__(client, database)
        self.session_type = session_type
        self.options = options

    def run(self, replica: _ClusterDatabase.Replica):
        return _ClusterSession(self.client, replica.address(), self.database, self.session_type, self.options)
