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
from threading import Lock
from typing import Dict

import grakn_protocol.protobuf.cluster.database_pb2 as database_proto
from grpc import RpcError, StatusCode

from grakn.common.exception import GraknClientException
from grakn.options import GraknClusterOptions, GraknOptions
from grakn.rpc.cluster.address import Address
from grakn.rpc.session import Session, SessionType
from grakn.rpc.transaction import TransactionType, Transaction


class _RPCSessionCluster(Session):
    MAX_RETRY_PER_REPLICA = 10
    WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS: float = 2

    def __init__(self, cluster_client, database: str, session_type: SessionType, options: GraknClusterOptions):
        self._lock = Lock()
        self._cluster_client = cluster_client
        self._db_name = database
        self._session_type = session_type
        self._options = options
        self._database = self._discover_database()
        self._core_sessions: Dict["_RPCSessionCluster.Replica.Id", Session] = {}
        self._is_open = True

    def transaction(self, transaction_type: TransactionType, options: GraknClusterOptions = None) -> Transaction:
        if not options:
            options = GraknOptions.cluster()
        return self._transaction_secondary_replica(transaction_type, options) if options.allow_secondary_replica else self._transaction_primary_replica(transaction_type, options)

    def session_type(self) -> SessionType:
        return self._session_type

    def is_open(self) -> bool:
        return self._is_open

    def close(self) -> None:
        for core_session in self._core_sessions.values():
            core_session.close()
        self._is_open = False

    def database(self) -> str:
        return self._db_name

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _transaction_primary_replica(self, transaction_type: TransactionType, options: GraknOptions) -> Transaction:
        for replica in self._database.replicas():
            retry = 0
            while retry < self.MAX_RETRY_PER_REPLICA:
                try:
                    primary_replica_id = self._database.primary_replica().replica_id()
                    with self._lock:
                        if primary_replica_id in self._core_sessions:
                            primary_replica_session = self._core_sessions[primary_replica_id]
                        else:
                            print("Opening a session to primary replica '%s'" % primary_replica_id)
                            primary_replica_client = self._cluster_client.core_client(primary_replica_id.address())
                            self._core_sessions[primary_replica_id] = primary_replica_client.session(primary_replica_id.database(), self._session_type, self._options)
                            primary_replica_session = self._core_sessions[primary_replica_id]
                    print("Opening a transaction to primary replica '%s'" % self._database.primary_replica().replica_id())
                    return primary_replica_session.transaction(transaction_type, options)
                except GraknClientException as e:
                    retry += 1
                    # TODO: propagate exception from the server in a less brittle way
                    if "[RPL01]" in str(e):
                        print("Unable to open a session or transaction: %s" % str(e))
                        self._database = self._discover_database(replica.replica_id().address())
                    # TODO: once GraknClientException has an error message bank, lookup this error from that
                    elif "No replica has been marked as the primary replica for latest known term" in str(e):
                        print("Unable to open a session or transaction: %s" % str(e))
                        time.sleep(self.WAIT_FOR_PRIMARY_REPLICA_SELECTION_SECONDS)
                        self._database = self._discover_database(replica.replica_id().address())
                    else:
                        raise e
                # TODO: introduce a special type that extends RpcError and Call
                except RpcError as e:
                    # TODO: this logic should be extracted into GraknClientException
                    if e.code() == StatusCode.UNAVAILABLE:
                        print("Unable to open a session or transaction to %s. Attempting next replica. %s" % (str(replica.replica_id()), str(e)))
                        break
                    else:
                        raise e
        raise self._cluster_not_available_exception()

    def _transaction_secondary_replica(self, transaction_type: TransactionType, options: GraknClusterOptions) -> Transaction:
        for replica in self._database.replicas():
            try:
                replica_id = replica.replica_id()
                with self._lock:
                    if replica_id in self._core_sessions:
                        selected_session = self._core_sessions[replica_id]
                    else:
                        print("Opening a session to '%s'" % replica_id)
                        client = self._cluster_client.core_client(replica_id.address())
                        self._core_sessions[replica_id] = client.session(replica_id.database(), self._session_type, self._options)
                        selected_session = self._core_sessions[replica_id]
                print("Opening read secondary transaction to secondary replica '%s'" % replica.replica_id())
                return selected_session.transaction(transaction_type, options)
            except RpcError as e:
                if e.code() == StatusCode.UNAVAILABLE:
                    print("Unable to open a session or transaction to %s. Attempting next replica. %s" % (str(replica.replica_id()), str(e)))
                else:
                    raise e
        raise self._cluster_not_available_exception()

    def _discover_database(self, server_address: Address.Server = None) -> "_RPCSessionCluster.Database":
        if server_address:
            db_discover_req = database_proto.Database.Discover.Req()
            db_discover_req.database = self._db_name
            res = self._cluster_client.grakn_cluster_grpc_stub(server_address).database_discover(db_discover_req)
            return _RPCSessionCluster.Database.of_proto(res)
        else:
            for server_addr in self._cluster_client.cluster_members():
                try:
                    return self._discover_database(server_addr)
                except RpcError as e:
                    print("Unable to perform database discovery to %s. Attempting next address. %s" % (str(server_addr), str(e)))
            raise self._cluster_not_available_exception()

    def _cluster_not_available_exception(self) -> GraknClientException:
        addresses = str([str(addr) for addr in self._cluster_client.cluster_members()])
        return GraknClientException("Unable to connect to Grakn Cluster. Attempted connecting to the cluster members, but none are available: '%s'" % addresses)

    class Database:

        def __init__(self, replicas: Dict["_RPCSessionCluster.Replica.Id", "_RPCSessionCluster.Replica"]):
            assert replicas
            self._replicas = replicas

        @staticmethod
        def of_proto(res: database_proto.Database.Discover.Res) -> "_RPCSessionCluster.Database":
            replica_map: Dict["_RPCSessionCluster.Replica.Id", "_RPCSessionCluster.Replica"] = {}
            for replica_proto in res.replicas:
                replica_id = _RPCSessionCluster.Replica.Id(Address.Server.parse(replica_proto.address), replica_proto.database)
                replica_map[replica_id] = _RPCSessionCluster.Replica.of_proto(replica_proto)
            return _RPCSessionCluster.Database(replica_map)

        def primary_replica(self) -> "_RPCSessionCluster.Replica":
            primaries = [replica for replica in self._replicas.values() if replica.is_primary()]
            if primaries:
                return max(primaries, key=lambda r: r.term)
            else:
                max_term = max([replica.term() for replica in self._replicas.values()])
                raise GraknClientException("No replica has been marked as the primary replica for latest known term '%d'." % max_term)

        def replicas(self):
            return self._replicas.values()

    class Replica:

        def __init__(self, replica_id: "_RPCSessionCluster.Replica.Id", term: int, is_primary: bool):
            self._replica_id = replica_id
            self._term = term
            self._is_primary = is_primary

        @staticmethod
        def of_proto(replica_proto: database_proto.Database.Discover.Res.Replica) -> "_RPCSessionCluster.Replica":
            return _RPCSessionCluster.Replica(
                replica_id=_RPCSessionCluster.Replica.Id(Address.Server.parse(replica_proto.address), replica_proto.database),
                term=replica_proto.term,
                is_primary=replica_proto.is_primary
            )

        def replica_id(self) -> "_RPCSessionCluster.Replica.Id":
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

            def __init__(self, address: Address.Server, database: str):
                self._address = address
                self._database = database

            def address(self) -> Address.Server:
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
