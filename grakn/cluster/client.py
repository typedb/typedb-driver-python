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
from grakn.api.client import GraknClusterClient


class ClusterClient(GraknClusterClient):

    def __init__(self, addresses: Iterable[str]):
        self._core_clients: Dict[ServerAddress, _ClientRPC] = {addr: _ClientRPC(addr.external()) for addr in self._fetch_cluster_servers(addresses)}
        self._grakn_cluster_grpc_stubs = {addr: GraknClusterStub(client.channel()) for (addr, client) in self._core_clients.items()}
        self._database_managers = _DatabaseManagerClusterRPC(self, {addr: client.databases() for (addr, client) in self._core_clients.items()})
        self._cluster_databases: Dict[str, _DatabaseClusterRPC] = {}
        self._is_open = True

    def session(self, database: str, session_type: SessionType, options=None) -> Session:
        if not options:
            options = GraknOptions.cluster()
        return self._session_any_replica(database, session_type, options) if options.read_any_replica else self._session_primary_replica(database, session_type, options)

    def _session_primary_replica(self, database: str, session_type: SessionType, options=None) -> SessionClusterRPC:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_primary_replica()

    def _session_any_replica(self, database: str, session_type: SessionType, options=None) -> SessionClusterRPC:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_any_replica()

    def databases(self) -> DatabaseManagerCluster:
        return self._database_managers

    def is_open(self) -> bool:
        return self._is_open

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

    def cluster_databases(self) -> Dict[str, _DatabaseClusterRPC]:
        return self._cluster_databases

    def cluster_members(self) -> Set[ServerAddress]:
        return set(self._core_clients.keys())

    def core_client(self, address: ServerAddress) -> _ClientRPC:
        return self._core_clients.get(address)

    def grakn_cluster_grpc_stub(self, address: ServerAddress) -> GraknClusterStub:
        return self._grakn_cluster_grpc_stubs.get(address)

    def _fetch_cluster_servers(self, addresses: Iterable[str]) -> Set[ServerAddress]:
        for address in addresses:
            try:
                with _ClientRPC(address) as client:
                    print("Performing cluster discovery to %s..." % address)
                    grakn_cluster_stub = GraknClusterStub(client.channel())
                    res = grakn_cluster_stub.cluster_servers(cluster_proto.Cluster.Servers.Req())
                    members = set([ServerAddress.parse(srv) for srv in res.servers])
                    print("Discovered %s" % [str(member) for member in members])
                    return members
            except RpcError as e:
                print("Cluster discovery to %s failed. %s" % (address, str(e)))
        raise GraknClientException("Unable to connect to Grakn Cluster. Attempted connecting to the cluster members, but none are available: %s" % str(addresses))


class _OpenSessionFailsafeTask(_FailsafeTask):

    def __init__(self, database: str, session_type: SessionType, options: GraknClusterOptions, client: "_ClientClusterRPC"):
        super().__init__(client, database)
        self.session_type = session_type
        self.options = options

    def run(self, replica: _DatabaseClusterRPC.Replica):
        return SessionClusterRPC(self.client, replica.address(), self.database, self.session_type, self.options)
