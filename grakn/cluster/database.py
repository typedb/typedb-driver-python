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
from typing import Dict, Optional, Set, TYPE_CHECKING

import grakn_protocol.cluster.cluster_database_pb2 as cluster_database_proto

from grakn.api.database import ClusterDatabase
from grakn.core.database import _CoreDatabase

if TYPE_CHECKING:
    from grakn.cluster.database_manager import _ClusterDatabaseManager


class _ClusterDatabase(ClusterDatabase):

    def __init__(self, database: str, cluster_database_mgr: "_ClusterDatabaseManager"):
        self._name = database
        self._database_mgr = cluster_database_mgr
        self._databases: Dict[str, _CoreDatabase] = {}
        self._replicas: Set["_ClusterDatabase.Replica"] = set()
        for address in cluster_database_mgr.database_mgrs():
            core_database_mgr = cluster_database_mgr.database_mgrs()[address]
            self._databases[address] = _CoreDatabase(core_database_mgr.stub(), name=database)

    @staticmethod
    def of(proto_db: cluster_database_proto.ClusterDatabase, cluster_database_mgr: "_ClusterDatabaseManager") -> "_ClusterDatabase":
        assert proto_db.replicas
        database: str = proto_db.name
        database_cluster_rpc = _ClusterDatabase(database, cluster_database_mgr)
        for proto_replica in proto_db.replicas:
            database_cluster_rpc.replicas().add(_ClusterDatabase.Replica.of(proto_replica, database_cluster_rpc))
        print("Discovered database cluster: %s" % database_cluster_rpc)
        return database_cluster_rpc

    def name(self) -> str:
        return self._name

    def schema(self) -> str:
        return next(iter(self._databases.values())).schema()

    def delete(self) -> None:
        for address in self._databases:
            if self._database_mgr.database_mgrs()[address].contains(self._name):
                self._databases[address].delete()

    def replicas(self):
        return self._replicas

    def primary_replica(self) -> Optional["_ClusterDatabase.Replica"]:
        primaries = [replica for replica in self._replicas if replica.is_primary()]
        return max(primaries, key=lambda r: r.term) if primaries else None

    def preferred_replica(self) -> "_ClusterDatabase.Replica":
        return next(iter([replica for replica in self._replicas if replica.is_preferred()]), next(iter(self._replicas)))

    def __str__(self):
        return self._name

    class Replica(ClusterDatabase.Replica):

        def __init__(self, database: "_ClusterDatabase", address: str, is_primary: bool, is_preferred: bool, term: int):
            self._database = database
            self._is_primary = is_primary
            self._is_preferred = is_preferred
            self._term = term
            self._replica_id = _ClusterDatabase.Replica.ID(address, database.name())

        @staticmethod
        def of(replica: cluster_database_proto.ClusterDatabase.Replica, database: "_ClusterDatabase") -> "_ClusterDatabase.Replica":
            return _ClusterDatabase.Replica(database, replica.address, replica.primary, replica.preferred, replica.term)

        def replica_id(self) -> "_ClusterDatabase.Replica.ID":
            return self._replica_id

        def database(self) -> "ClusterDatabase":
            return self._database

        def address(self) -> str:
            return self._replica_id.address()

        def is_primary(self) -> bool:
            return self._is_primary

        def is_preferred(self) -> bool:
            return self._is_preferred

        def term(self) -> int:
            return self._term

        def __str__(self):
            return "%s:%s:%d" % (str(self._replica_id), "P" if self._is_primary else "S", self._term)

        def __eq__(self, other):
            if self is other:
                return True
            if not other or type(self) != type(other):
                return False
            return self._replica_id == other.replica_id() and self._term == other.term() and self._is_primary == other.is_primary() and self._is_preferred == other.is_preferred_secondary()

        def __hash__(self):
            return hash((self._replica_id, self._is_primary, self._is_preferred, self._term))

        class ID:

            def __init__(self, address: str, database: str):
                self._address = address
                self._database = database

            def address(self) -> str:
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
                return "%s/%s" % (self._address, self._database)
