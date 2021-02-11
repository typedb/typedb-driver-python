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

from typing import Dict, Optional
import grakn_protocol.protobuf.cluster.database_pb2 as database_proto

from grakn.rpc.cluster.server_address import ServerAddress


class ReplicaInfo:

    def __init__(self, replicas: Dict["ReplicaInfo.Replica.Id", "ReplicaInfo.Replica"]):
        assert replicas
        self._replicas = replicas

    @staticmethod
    def of_proto(res: database_proto.Database.Discover.Res) -> "ReplicaInfo":
        replica_map: Dict["ReplicaInfo.Replica.Id", "ReplicaInfo.Replica"] = {}
        for replica_proto in res.replicas:
            replica_id = ReplicaInfo.Replica.Id(ServerAddress.parse(replica_proto.address), replica_proto.database)
            replica_map[replica_id] = ReplicaInfo.Replica.of_proto(replica_proto)
        return ReplicaInfo(replica_map)

    def primary_replica(self) -> Optional["ReplicaInfo.Replica"]:
        primaries = [replica for replica in self._replicas.values() if replica.is_primary()]
        return max(primaries, key=lambda r: r.term) if primaries else None

    def replicas(self):
        return self._replicas.values()

    class Replica:

        def __init__(self, replica_id: "ReplicaInfo.Replica.Id", term: int, is_primary: bool):
            self._replica_id = replica_id
            self._term = term
            self._is_primary = is_primary

        @staticmethod
        def of_proto(replica_proto: database_proto.Database.Discover.Res.Replica) -> "ReplicaInfo.Replica":
            return ReplicaInfo.Replica(
                replica_id=ReplicaInfo.Replica.Id(ServerAddress.parse(replica_proto.address), replica_proto.database),
                term=replica_proto.term,
                is_primary=replica_proto.is_primary
            )

        def replica_id(self) -> "ReplicaInfo.Replica.Id":
            return self._replica_id

        def term(self) -> int:
            return self._term

        def is_primary(self) -> bool:
            return self._is_primary

        def __eq__(self, other):
            if self is other:
                return True
            if not other or type(self) != type(other):
                return False
            return self._term == other.term() and self._is_primary == other.is_primary()

        def __hash__(self):
            return hash((self._is_primary, self._term))

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
