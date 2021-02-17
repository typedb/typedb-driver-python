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
from typing import Dict, Optional, Set

import grakn_protocol.protobuf.cluster.database_pb2 as database_proto

from grakn.rpc.cluster.server_address import ServerAddress
from grakn.rpc.database import DatabaseCluster, _DatabaseRPC


class _DatabaseClusterRPC(DatabaseCluster):

    def __init__(self, database_manager_cluster, database: str):
        self._databases: Dict[ServerAddress, _DatabaseRPC] = {}
        for address in database_manager_cluster.database_managers():
            database_manager = database_manager_cluster.database_managers()[address]
            self._databases[address] = _DatabaseRPC(database_manager, name=database)
        self._name = database
        self._database_manager_cluster = database_manager_cluster
        self._replicas: Set["_DatabaseClusterRPC.Replica"] = set()

    @staticmethod
    def of(proto_db: database_proto.Database, database_manager_cluster) -> "_DatabaseClusterRPC":
        assert proto_db.replicas
        database: str = proto_db.name
        database_cluster_rpc = _DatabaseClusterRPC(database_manager_cluster, database)
        for proto_replica in proto_db.replicas:
            database_cluster_rpc.replicas().add(_DatabaseClusterRPC.Replica.of(proto_replica, database_cluster_rpc))
        print("Discovered database cluster: " + str(database_cluster_rpc))
        return database_cluster_rpc

    def primary_replica(self) -> Optional["_DatabaseClusterRPC.Replica"]:
        primaries = [replica for replica in self._replicas if replica.is_primary()]
        return max(primaries, key=lambda r: r.term) if primaries else None

    def preferred_secondary_replica(self) -> "_DatabaseClusterRPC.Replica":
        return next(iter([replica for replica in self._replicas if replica.is_preferred_secondary()]), next(iter(self._replicas)))

    def name(self) -> str:
        return self._name

    def delete(self) -> None:
        for address in self._databases:
            if self._database_manager_cluster.database_managers()[address].contains(self._name):
                self._databases[address].delete()

    def replicas(self):
        return self._replicas

    def __str__(self):
        return self._name

    class Replica:

        def __init__(self, database: "_DatabaseClusterRPC", address: ServerAddress, term: int, is_primary: bool, is_preferred_secondary: bool):
            self._database = database
            self._replica_id = _DatabaseClusterRPC.Replica.Id(address, database.name())
            self._term = term
            self._is_primary = is_primary
            self._is_preferred_secondary = is_preferred_secondary

        @staticmethod
        def of(proto_replica: database_proto.Database.Replica, database: "_DatabaseClusterRPC") -> "_DatabaseClusterRPC.Replica":
            return _DatabaseClusterRPC.Replica(database, ServerAddress.parse(proto_replica.address), proto_replica.term,
                                               proto_replica.primary, proto_replica.preferred_secondary)

        def replica_id(self) -> "_DatabaseClusterRPC.Replica.Id":
            return self._replica_id

        def term(self) -> int:
            return self._term

        def is_primary(self) -> bool:
            return self._is_primary

        def is_preferred_secondary(self) -> bool:
            return self._is_preferred_secondary

        def address(self) -> ServerAddress:
            return self._replica_id.address()

        def __eq__(self, other):
            if self is other:
                return True
            if not other or type(self) != type(other):
                return False
            return self._replica_id == other.replica_id() and self._term == other.term() and self._is_primary == other.is_primary() and self._is_preferred_secondary == other.is_preferred_secondary()

        def __hash__(self):
            return hash((self._replica_id, self._is_primary, self._is_preferred_secondary, self._term))

        def __str__(self):
            return "%s:%s:%d" % (str(self._replica_id), "P" if self._is_primary else "S", self._term)

        class Id:

            def __init__(self, address: ServerAddress, database: str):
                self._address = address
                self._database = database

            def address(self) -> ServerAddress:
                return self._address

            def database(self) -> str:
                return self._database

            def __eq__(self, other):
                if self is other:
                    return True
                if not other or type(self) != type(other):
                    return False
                return self._address == other.address() and self._database == other.database()

            def __hash__(self):
                return hash((self._address, self._database))

            def __str__(self):
                return "%s/%s" % (str(self._address), self._database)
