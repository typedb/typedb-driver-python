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
from abc import ABC, abstractmethod
from typing import List, Set, Dict

import grakn_protocol.protobuf.cluster.cluster_pb2 as cluster_proto
import grpc
from grakn_protocol.protobuf.cluster.grakn_cluster_pb2_grpc import GraknClusterStub
from grpc import RpcError

# Repackaging these symbols allows them to be imported from "grakn.client"
from grakn.common.exception import GraknClientException  # noqa # pylint: disable=unused-import
from grakn.concept.type.value_type import ValueType  # noqa # pylint: disable=unused-import
from grakn.options import GraknOptions
from grakn.rpc.cluster.address import Address
from grakn.rpc.cluster.database_manager import _RPCDatabaseManagerCluster
from grakn.rpc.cluster.session import _RPCSessionCluster
from grakn.rpc.database_manager import DatabaseManager, _RPCDatabaseManager
from grakn.rpc.session import Session, SessionType, _RPCSession
from grakn.rpc.transaction import TransactionType  # noqa # pylint: disable=unused-import


class GraknClient(ABC):
    DEFAULT_ADDRESS = "localhost:1729"

    @staticmethod
    def core(address=DEFAULT_ADDRESS) -> "GraknClient":
        return _RPCGraknClient(address)

    @staticmethod
    def cluster(address=DEFAULT_ADDRESS) -> "GraknClient":
        return _RPCGraknClientCluster(address)

    @abstractmethod
    def session(self, database: str, session_type: SessionType, options: GraknOptions = None) -> Session:
        pass

    @abstractmethod
    def databases(self) -> DatabaseManager:
        pass

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _RPCGraknClient(GraknClient):

    def __init__(self, address: str):
        self._address = address
        self._channel = grpc.insecure_channel(self._address)
        self._databases = _RPCDatabaseManager(self._channel)
        self._is_open = True

    def session(self, database: str, session_type: SessionType, options=None) -> Session:
        if not options:
            options = GraknOptions.core()
        return _RPCSession(self, database, session_type, options)

    def databases(self):
        return self._databases

    def is_open(self):
        return self._is_open

    def close(self):
        self._channel.close()
        self._is_open = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False

    def channel(self):
        return self._channel


# _RPCGraknClientCluster must live in this package because of circular ref with GraknClient
class _RPCGraknClientCluster(GraknClient):

    def __init__(self, address: str):
        self._core_clients: Dict[Address.Server, _RPCGraknClient] = {addr: _RPCGraknClient(addr.client()) for addr in self._discover_cluster([address])}
        self._grakn_cluster_grpc_stubs = {addr: GraknClusterStub(client.channel()) for (addr, client) in self._core_clients.items()}
        self._databases = _RPCDatabaseManagerCluster({addr: client.databases() for (addr, client) in self._core_clients.items()})
        self._is_open = True

    def session(self, database: str, session_type: SessionType, options=None) -> Session:
        if not options:
            options = GraknOptions.cluster()
        return _RPCSessionCluster(self, database, session_type, options)

    def databases(self) -> DatabaseManager:
        return self._databases

    def is_open(self) -> bool:
        return self._is_open

    def close(self) -> None:
        for client in self._core_clients.values():
            client.close()
        self._is_open = False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def cluster_members(self) -> Set[Address.Server]:
        return set(self._core_clients.keys())

    def core_client(self, address: Address.Server) -> _RPCGraknClient:
        return self._core_clients.get(address)

    def grakn_cluster_grpc_stub(self, address: Address.Server) -> GraknClusterStub:
        return self._grakn_cluster_grpc_stubs.get(address)

    def _discover_cluster(self, addresses: List[str]) -> Set[Address.Server]:
        for address in addresses:
            try:
                with _RPCGraknClient(address) as client:
                    print("Performing cluster discovery to %s..." % address)
                    grakn_cluster_stub = GraknClusterStub(client.channel())
                    res = grakn_cluster_stub.cluster_discover(cluster_proto.Cluster.Discover.Req())
                    members = set([Address.Server.parse(srv) for srv in res.servers])
                    print("Discovered %s" % [str(member) for member in members])
                    return members
            except RpcError:
                print("Cluster discovery to %s failed." % address)
        raise GraknClientException("Unable to connect to Grakn Cluster. Attempted connecting to the cluster members, but none are available: %s" % str(addresses))
