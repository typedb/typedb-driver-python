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
import subprocess
import unittest
from functools import partial
from multiprocessing.pool import ThreadPool
from time import sleep
from unittest import TestCase

import grakn_protocol.protobuf.cluster.cluster_pb2 as cluster_proto
import grakn_protocol.protobuf.cluster.database_pb2 as database_proto
import grpc
from grakn_protocol.protobuf.cluster.grakn_cluster_pb2_grpc import GraknClusterStub

from grakn.client import GraknClient, SessionType, TransactionType
from grakn.rpc.cluster.session import _RPCSessionCluster
from grakn.rpc.session import Session


class TestClusterFailover(TestCase):

    def setUp(self):
        with GraknClient.cluster("localhost:11729") as client:
            if "grakn" in client.databases().all():
                client.databases().delete("grakn")
            client.databases().create("grakn")

    # def test_hello_world(self):
    #     with GraknClient.cluster("localhost:11729") as client:
    #         print("Hello World")

    def get_primary_replica(self):
        channel = grpc.insecure_channel("localhost:11729")
        cluster_grpc_stub = GraknClusterStub(channel)
        while True:
            db_discover_req = database_proto.Database.Discover.Req()
            db_discover_req.database = "grakn"
            print("Discovering replicas for database 'grakn'...")
            res = cluster_grpc_stub.database_discover(db_discover_req)
            dbs = _RPCSessionCluster.Database.of_proto(res)
            print("Discovered " + str([str(replica) for replica in dbs.replicas()]))
            primary_replica = next(iter([replica for replica in dbs.replicas() if replica.is_primary()]), None)
            if primary_replica:
                return primary_replica
            else:
                print("There is no primary replica yet. Retrying in 2s...")
                sleep(2)
                return self.get_primary_replica()

    # def test_discover_database(self):
    #     self.get_primary_replica()

    def test_put_entity_type_to_crashed_primary_replica(self):
        with GraknClient.cluster("localhost:11729") as client:
            assert client.databases().contains("grakn")
            primary_replica = self.get_primary_replica()
            print("Performing operations against the primary replica " + str(primary_replica))
            with client.session("grakn", SessionType.SCHEMA) as session, session.transaction(TransactionType.WRITE) as tx:
                tx.concepts().put_entity_type("person")
                tx.commit()
            with client.session("grakn", SessionType.SCHEMA) as session, session.transaction(TransactionType.READ) as tx:
                person = tx.concepts().get_entity_type("person")
                assert person.get_label() == "person"
            print("Stopping primary replica...")
            port = primary_replica.replica_id().address().server_port()
            lsof = subprocess.check_output(["lsof", "-i", ":%d" % port])
            primary_replica_server_pid = [conn.split()[1] for conn in lsof.decode("utf-8").split("\n") if "LISTEN" in conn][0]
            print("Primary replica is hosted by server with PID %s" % primary_replica_server_pid)
            subprocess.check_call(["kill", primary_replica_server_pid])
            print("Primary replica stopped successfully.")
            with client.session("grakn", SessionType.SCHEMA) as session, session.transaction(TransactionType.READ) as tx:
                person = tx.concepts().get_entity_type("person")
                assert person.get_label() == "person"


if __name__ == "__main__":
    unittest.main(verbosity=2)
