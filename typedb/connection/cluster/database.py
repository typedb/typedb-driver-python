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
from abc import ABC, abstractmethod
from time import sleep
from typing import Dict, Optional, Set, TYPE_CHECKING

import typedb_protocol.cluster.cluster_database_pb2 as cluster_database_proto

from typedb.api.connection.database import ClusterDatabase
from typedb.common.exception import TypeDBClientException, UNABLE_TO_CONNECT, CLUSTER_REPLICA_NOT_PRIMARY, \
    CLUSTER_UNABLE_TO_CONNECT
from typedb.common.rpc.request_builder import cluster_database_manager_get_req
from typedb.connection.database import _TypeDBDatabaseImpl

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient


class _ClusterDatabase(ClusterDatabase):

    def __init__(self, database: str, client: "_ClusterClient"):
        self._name = database
        self._client = client
        self._databases: Dict[str, _TypeDBDatabaseImpl] = {}
        self._replicas: Set["_ClusterDatabase.Replica"] = set()
        cluster_db_mgr = client.databases()
        for address in cluster_db_mgr.database_mgrs():
            core_database_mgr = cluster_db_mgr.database_mgrs()[address]
            self._databases[address] = _TypeDBDatabaseImpl(core_database_mgr.stub(), name=database)

    @staticmethod
    def of(proto_db: cluster_database_proto.ClusterDatabase, client: "_ClusterClient") -> "_ClusterDatabase":
        assert proto_db.replicas
        database: str = proto_db.name
        database_cluster_rpc = _ClusterDatabase(database, client)
        for proto_replica in proto_db.replicas:
            database_cluster_rpc.replicas().add(_ClusterDatabase.Replica.of(proto_replica, database_cluster_rpc))
        print("Discovered database cluster: %s" % database_cluster_rpc)
        return database_cluster_rpc

    def name(self) -> str:
        return self._name

    def schema(self) -> str:
        return next(iter(self._databases.values())).schema()

    def delete(self) -> None:
        delete_db_task = _DeleteDatabaseFailsafeTask(self._client, self._name, self._databases)
        delete_db_task.run_primary_replica()

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


# This class has to live here because of circular class creation between ClusterDatabase and FailsafeTask
class _FailsafeTask(ABC):

    PRIMARY_REPLICA_TASK_MAX_RETRIES = 10
    FETCH_REPLICAS_MAX_RETRIES = 10
    WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS: float = 2

    def __init__(self, client: "_ClusterClient", database: str):
        self.client = client
        self.database = database

    @abstractmethod
    def run(self, replica: "_ClusterDatabase.Replica"):
        pass

    def rerun(self, replica: "_ClusterDatabase.Replica"):
        return self.run(replica)

    def run_primary_replica(self):
        if self.database not in self.client.database_by_name() or not self.client.database_by_name()[self.database].primary_replica():
            self._seek_primary_replica()
        replica = self.client.database_by_name()[self.database].primary_replica()
        retries = 0
        while True:
            try:
                return self.run(replica) if retries == 0 else self.rerun(replica)
            except TypeDBClientException as e:
                if e.error_message in [CLUSTER_REPLICA_NOT_PRIMARY, UNABLE_TO_CONNECT]:
                    print("Unable to open a session or transaction, retrying in 2s... %s" % str(e))
                    sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                    replica = self._seek_primary_replica()
                else:
                    raise e
            retries += 1
            if retries > self.PRIMARY_REPLICA_TASK_MAX_RETRIES:
                raise self._cluster_not_available_exception()

    def run_any_replica(self):
        if self.database in self.client.database_by_name():
            cluster_database = self.client.database_by_name()[self.database]
        else:
            cluster_database = self._fetch_database_replicas()

        replicas = [cluster_database.preferred_replica()] + [replica for replica in cluster_database.replicas() if not replica.is_preferred()]
        retries = 0
        for replica in replicas:
            try:
                return self.run(replica) if retries == 0 else self.rerun(replica)
            except TypeDBClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Unable to open a session or transaction to %s. Attempting next replica. %s" % (str(replica.replica_id()), str(e)))
                else:
                    raise e
            retries += 1
        raise self._cluster_not_available_exception()

    def _seek_primary_replica(self) -> "_ClusterDatabase.Replica":
        retries = 0
        while retries < self.FETCH_REPLICAS_MAX_RETRIES:
            cluster_database = self._fetch_database_replicas()
            if cluster_database.primary_replica():
                return cluster_database.primary_replica()
            else:
                sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                retries += 1
        raise self._cluster_not_available_exception()

    def _fetch_database_replicas(self) -> "_ClusterDatabase":
        for server_address in self.client.cluster_members():
            try:
                print("Fetching replica info from %s" % server_address)
                res = self.client._stub(server_address).databases_get(cluster_database_manager_get_req(self.database))
                cluster_database = _ClusterDatabase.of(res.database, self.client)
                self.client.database_by_name()[self.database] = cluster_database
                return cluster_database
            except TypeDBClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Unable to fetch replica info for database '%s' from %s. Attempting next address. %s" % (self.database, server_address, str(e)))
                else:
                    raise e
        raise self._cluster_not_available_exception()

    def _cluster_not_available_exception(self) -> TypeDBClientException:
        return TypeDBClientException.of(CLUSTER_UNABLE_TO_CONNECT, str([str(addr) for addr in self.client.cluster_members()]))


class _DeleteDatabaseFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", database: str, databases: Dict[str, _TypeDBDatabaseImpl]):
        super().__init__(client, database)
        self.databases = databases

    def run(self, replica: _ClusterDatabase.Replica):
        self.databases.get(replica.address()).delete()
