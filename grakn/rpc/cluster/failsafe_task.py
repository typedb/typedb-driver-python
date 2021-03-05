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
import time
from abc import ABC, abstractmethod

import grakn_protocol.protobuf.cluster.database_pb2 as database_proto
from grpc import RpcError, StatusCode

from grakn.common.exception import GraknClientException
from grakn.rpc.cluster.database import _DatabaseClusterRPC


class _FailsafeTask(ABC):

    PRIMARY_REPLICA_TASK_MAX_RETRIES = 10
    FETCH_REPLICAS_MAX_RETRIES = 10
    WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS: float = 2

    def __init__(self, client, database: str):
        self.client = client
        self.database = database

    @abstractmethod
    def run(self, replica: _DatabaseClusterRPC.Replica):
        pass

    def rerun(self, replica: _DatabaseClusterRPC.Replica):
        return self.run(replica)

    def run_primary_replica(self):
        if self.database not in self.client.cluster_databases() or not self.client.cluster_databases()[self.database].primary_replica():
            self._seek_primary_replica()
        replica = self.client.cluster_databases()[self.database].primary_replica()
        retries = 0
        while True:
            try:
                return self.run(replica) if retries == 0 else self.rerun(replica)
            except GraknClientException as e:
                # TODO: propagate exception from the server in a less brittle way
                if "[RPL01]" in str(e):  # The contacted replica reported that it was not the primary replica
                    print("Unable to open a session or transaction, retrying in 2s... %s" % str(e))
                    time.sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                    replica = self._seek_primary_replica()
                else:
                    raise e
                # TODO: introduce a special type that extends RpcError and Call
            except RpcError as e:
                # TODO: this logic should be extracted into GraknClientException
                # TODO: error message should be checked in a less brittle way
                if e.code() in [StatusCode.UNAVAILABLE, StatusCode.UNKNOWN] or "[INT07]" in str(e) or "Received RST_STREAM" in str(e):
                    print("Unable to open a session or transaction, retrying in 2s... %s" % str(e))
                    time.sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                    replica = self._seek_primary_replica()
                else:
                    raise e
            retries += 1
            if retries > self.PRIMARY_REPLICA_TASK_MAX_RETRIES:
                raise self._cluster_not_available_exception()

    def run_any_replica(self):
        if self.database in self.client.cluster_databases():
            replica_info = self.client.cluster_databases()[self.database]
        else:
            replica_info = self._fetch_database_replicas()

        replicas = [replica_info.preferred_secondary_replica()] + [replica for replica in replica_info.replicas() if not replica.is_preferred_secondary()]
        retries = 0
        for replica in replicas:
            try:
                return self.run(replica) if retries == 0 else self.rerun(replica)
            except RpcError as e:
                if e.code() in [StatusCode.UNAVAILABLE, StatusCode.UNKNOWN] or "[INT07]" in str(e) or "Received RST_STREAM" in str(e):
                    print("Unable to open a session or transaction to %s. Attempting next replica. %s" % (str(replica.replica_id()), str(e)))
                else:
                    raise e
            retries += 1
        raise self._cluster_not_available_exception()

    def _seek_primary_replica(self) -> _DatabaseClusterRPC.Replica:
        retries = 0
        while retries < self.FETCH_REPLICAS_MAX_RETRIES:
            replica_info = self._fetch_database_replicas()
            if replica_info.primary_replica():
                return replica_info.primary_replica()
            else:
                time.sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                retries += 1
        raise self._cluster_not_available_exception()

    def _fetch_database_replicas(self) -> _DatabaseClusterRPC:
        for server_address in self.client.cluster_members():
            try:
                print("Fetching replica info from %s" % server_address)
                db_get_req = database_proto.Database.Get.Req()
                db_get_req.name = self.database
                res = self.client.grakn_cluster_grpc_stub(server_address).database_get(db_get_req)
                replica_info = _DatabaseClusterRPC.of(res.database, self.client.databases())
                print("Requested database discovery from peer %s, and got response: %s" % (str(server_address), str([str(replica) for replica in replica_info.replicas()])))
                self.client.cluster_databases()[self.database] = replica_info
                return replica_info
            except RpcError as e:
                print("Unable to perform database discovery to %s. Attempting next address. %s" % (str(server_address), str(e)))
        raise self._cluster_not_available_exception()

    def _cluster_not_available_exception(self) -> GraknClientException:
        addresses = str([str(addr) for addr in self.client.cluster_members()])
        return GraknClientException("Unable to connect to Grakn Cluster. Attempted connecting to the cluster members, but none are available: '%s'" % addresses)
