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

from grakn.options import GraknClusterOptions, GraknOptions
from grakn.rpc.cluster.failsafe_task import _FailsafeTask
from grakn.rpc.cluster.database import _DatabaseClusterRPC
from grakn.rpc.cluster.server_address import ServerAddress
from grakn.rpc.database import Database
from grakn.rpc.session import Session, SessionType
from grakn.rpc.transaction import TransactionType, Transaction


class SessionClusterRPC(Session):

    def __init__(self, cluster_client, server_address: ServerAddress, database: str, session_type: SessionType, options: GraknClusterOptions):
        self.cluster_client = cluster_client
        self.core_client = cluster_client.core_client(server_address)
        print("Opening a session to %s" % server_address)
        self.core_session = self.core_client.session(database, session_type, options)

    def transaction(self, transaction_type: TransactionType, options: GraknClusterOptions = None) -> Transaction:
        if not options:
            options = GraknOptions.cluster()
        return self._transaction_any_replica(transaction_type, options) if options.read_any_replica else self._transaction_primary_replica(transaction_type, options)

    def _transaction_primary_replica(self, transaction_type: TransactionType, options: GraknClusterOptions) -> Transaction:
        return TransactionFailsafeTask(self, transaction_type, options).run_primary_replica()

    def _transaction_any_replica(self, transaction_type: TransactionType, options: GraknClusterOptions) -> Transaction:
        return TransactionFailsafeTask(self, transaction_type, options).run_any_replica()

    def session_type(self) -> SessionType:
        return self.core_session.session_type()

    def is_open(self) -> bool:
        return self.core_session.is_open()

    def close(self) -> None:
        self.core_session.close()

    def database(self) -> Database:
        return self.core_session.database()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class TransactionFailsafeTask(_FailsafeTask):

    def __init__(self, cluster_session: SessionClusterRPC, transaction_type: TransactionType, options: GraknClusterOptions):
        super().__init__(cluster_session.cluster_client, cluster_session.database().name())
        self.cluster_session = cluster_session
        self.transaction_type = transaction_type
        self.options = options

    def run(self, replica: _DatabaseClusterRPC.Replica):
        return self.cluster_session.core_session.transaction(self.transaction_type, self.options)

    def rerun(self, replica: _DatabaseClusterRPC.Replica):
        if self.cluster_session.core_session:
            self.cluster_session.core_session.close()
        self.cluster_session.core_client = self.cluster_session.cluster_client.core_client(replica.address())
        self.cluster_session.core_session = self.cluster_session.core_client.session(self.database, self.cluster_session.session_type(), self.options)
        return self.cluster_session.core_session.transaction(self.transaction_type, self.options)
