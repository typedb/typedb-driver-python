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
from typing import Iterable, Dict, Set, cast

from typedb.api.connection.client import TypeDBClusterClient
from typedb.api.connection.credential import TypeDBCredential
from typedb.api.connection.options import TypeDBOptions, TypeDBClusterOptions
from typedb.api.connection.session import SessionType
from typedb.api.connection.user import UserManager
from typedb.connection.cluster.connection_factory import _ClusterConnectionFactory
from typedb.connection.cluster.database import _ClusterDatabase, _FailsafeTask
from typedb.connection.cluster.database_manager import _ClusterDatabaseManager
from typedb.connection.cluster.server_client import _ClusterServerClient
from typedb.connection.cluster.session import _ClusterSession
from typedb.connection.cluster.stub import _ClusterServerStub
from typedb.connection.cluster.user_manager import _ClusterUserManager
from typedb.common.rpc.request_builder import cluster_server_manager_all_req
from typedb.common.exception import TypeDBClientException, UNABLE_TO_CONNECT, CLUSTER_UNABLE_TO_CONNECT


class _ClusterClient(TypeDBClusterClient):

    def __init__(self, addresses: Iterable[str], credential: TypeDBCredential, parallelisation: int = None):
        self._credential = credential
        self._server_clients: Dict[str, _ClusterServerClient] = {addr: _ClusterServerClient(addr, credential, parallelisation) for addr in self._fetch_server_addresses(addresses)}
        self._stubs = {addr: client.connection_factory().newTypeDBStub(client.channel()) for (addr, client) in self._server_clients.items()}
        self._database_managers = _ClusterDatabaseManager(self)
        self._cluster_databases: Dict[str, _ClusterDatabase] = {}
        self._user_manager = _ClusterUserManager(self)
        self._is_open = True
        print("Cluster client created")

    def _fetch_server_addresses(self, addresses: Iterable[str]) -> Set[str]:
        for address in addresses:
            try:
                print("Fetching list of cluster servers from %s..." % address)
                with _ClusterServerClient(address, self._credential) as client:
                    typedb_cluster_stub = client.connection_factory().newTypeDBStub(client.channel())
                    res = typedb_cluster_stub.servers_all(cluster_server_manager_all_req())
                    members = {srv.address for srv in res.servers}
                    print("The cluster servers are %s" % [str(member) for member in members])
                    return members
            except TypeDBClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Fetching cluster servers from %s failed. %s" % (address, str(e)))
                else:
                    raise e
        raise TypeDBClientException.of(CLUSTER_UNABLE_TO_CONNECT, ",".join(addresses))

    def is_open(self) -> bool:
        return self._is_open

    def databases(self) -> _ClusterDatabaseManager:
        return self._database_managers

    def users(self) -> UserManager:
        return self._user_manager

    def session(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        if not options:
            options = TypeDBOptions.cluster()
        return self._session_any_replica(database, session_type, options) if getattr(options, "read_any_replica", False) else self._session_primary_replica(database, session_type, options)

    def _session_primary_replica(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_primary_replica()

    def _session_any_replica(self, database: str, session_type: SessionType, options=None) -> _ClusterSession:
        return _OpenSessionFailsafeTask(database, session_type, options, self).run_any_replica()

    def database_by_name(self) -> Dict[str, _ClusterDatabase]:
        return self._cluster_databases

    def cluster_members(self) -> Set[str]:
        return set(self._server_clients.keys())

    def _cluster_server_clients(self) -> Dict[str, _ClusterServerClient]:
        return self._server_clients

    def _cluster_server_client(self, address: str) -> _ClusterServerClient:
        return self._server_clients.get(address)

    def _stub(self, address: str) -> _ClusterServerStub:
        return self._stubs.get(address)

    def close(self) -> None:
        for client in self._server_clients.values():
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

    def __init__(self, database: str, session_type: SessionType, options: TypeDBClusterOptions, client: _ClusterClient):
        super().__init__(client, database)
        self.session_type = session_type
        self.options = options

    def run(self, replica: _ClusterDatabase.Replica):
        return _ClusterSession(self.client, replica.address(), self.database, self.session_type, self.options)
