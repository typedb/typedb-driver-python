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
from typing import TYPE_CHECKING

from grakn.cluster.database import _ClusterDatabase
from grakn.common.exception import GraknClientException, CLUSTER_REPLICA_NOT_PRIMARY, UNABLE_TO_CONNECT, \
    CLUSTER_UNABLE_TO_CONNECT
from grakn.common.rpc.request_builder import cluster_database_manager_get_req

if TYPE_CHECKING:
    from grakn.cluster.client import _ClusterClient


class _FailsafeTask(ABC):

    PRIMARY_REPLICA_TASK_MAX_RETRIES = 10
    FETCH_REPLICAS_MAX_RETRIES = 10
    WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS: float = 2

    def __init__(self, client: "_ClusterClient", database: str):
        self.client = client
        self.database = database

    @abstractmethod
    def run(self, replica: _ClusterDatabase.Replica):
        pass

    def rerun(self, replica: _ClusterDatabase.Replica):
        return self.run(replica)

    def run_primary_replica(self):
        if self.database not in self.client.database_by_name() or not self.client.database_by_name()[self.database].primary_replica():
            self._seek_primary_replica()
        replica = self.client.database_by_name()[self.database].primary_replica()
        retries = 0
        while True:
            try:
                return self.run(replica) if retries == 0 else self.rerun(replica)
            except GraknClientException as e:
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
            except GraknClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Unable to open a session or transaction to %s. Attempting next replica. %s" % (str(replica.replica_id()), str(e)))
                else:
                    raise e
            retries += 1
        raise self._cluster_not_available_exception()

    def _seek_primary_replica(self) -> _ClusterDatabase.Replica:
        retries = 0
        while retries < self.FETCH_REPLICAS_MAX_RETRIES:
            cluster_database = self._fetch_database_replicas()
            if cluster_database.primary_replica():
                return cluster_database.primary_replica()
            else:
                sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                retries += 1
        raise self._cluster_not_available_exception()

    def _fetch_database_replicas(self) -> _ClusterDatabase:
        for server_address in self.client.cluster_members():
            try:
                print("Fetching replica info from %s" % server_address)
                res = self.client.stub(server_address).databases_get(cluster_database_manager_get_req(self.database))
                cluster_database = _ClusterDatabase.of(res.database, self.client.databases())
                self.client.database_by_name()[self.database] = cluster_database
                return cluster_database
            except GraknClientException as e:
                if e.error_message is UNABLE_TO_CONNECT:
                    print("Unable to fetch replica info for database '%s' from %s. Attempting next address. %s" % (self.database, server_address, str(e)))
                else:
                    raise e
        raise self._cluster_not_available_exception()

    def _cluster_not_available_exception(self) -> GraknClientException:
        return GraknClientException.of(CLUSTER_UNABLE_TO_CONNECT, str([str(addr) for addr in self.client.cluster_members()]))
