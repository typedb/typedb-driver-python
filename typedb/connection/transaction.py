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

from typing import TYPE_CHECKING, Iterator

import typedb_protocol.common.transaction_pb2 as transaction_proto
from grpc import RpcError

from typedb.api.connection.options import TypeDBOptions
from typedb.api.query.future import QueryFuture
from typedb.api.connection.transaction import _TypeDBTransactionExtended, TransactionType
from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED
from typedb.common.rpc.request_builder import transaction_commit_req, transaction_rollback_req, transaction_open_req
from typedb.concept.concept_manager import _ConceptManager
from typedb.logic.logic_manager import _LogicManager
from typedb.query.query_manager import _QueryManager
from typedb.stream.bidirectional_stream import BidirectionalStream

if TYPE_CHECKING:
    from typedb.connection.session import _TypeDBSessionImpl


class _TypeDBTransactionImpl(_TypeDBTransactionExtended):

    def __init__(self, session: "_TypeDBSessionImpl", transaction_type: TransactionType, options: TypeDBOptions = None):
        if not options:
            options = TypeDBOptions.core()
        self._transaction_type = transaction_type
        self._options = options
        self._concept_manager = _ConceptManager(self)
        self._query_manager = _QueryManager(self)
        self._logic_manager = _LogicManager(self)

        try:
            # Other TypeDBClient implementations reuse a single gRPC Channel, but the Python client stalls
            # when opening several transactions in parallel from one Channel.
            stub = session.client().connection_factory().newTypeDBStub(session.client().connection_factory().newChannel(session.address()))
            self._bidirectional_stream = BidirectionalStream(stub, session.transmitter())
            req = transaction_open_req(session.session_id(), transaction_type.proto(), options.proto(), session.network_latency_millis())
            self.execute(request=req, batch=False)
        except RpcError as e:
            raise TypeDBClientException.of_rpc(e)

    def transaction_type(self) -> TransactionType:
        return self._transaction_type

    def options(self) -> TypeDBOptions:
        return self._options

    def is_open(self) -> bool:
        return self._bidirectional_stream.is_open()

    def concepts(self) -> _ConceptManager:
        return self._concept_manager

    def logic(self) -> _LogicManager:
        return self._logic_manager

    def query(self) -> _QueryManager:
        return self._query_manager

    def execute(self, request: transaction_proto.Transaction.Req, batch: bool = True) -> transaction_proto.Transaction.Res:
        return self.run_query(request, batch).get()

    def run_query(self, request: transaction_proto.Transaction.Req, batch: bool = True) -> QueryFuture[transaction_proto.Transaction.Res]:
        if not self.is_open():
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        return self._bidirectional_stream.single(request, batch)

    def stream(self, request: transaction_proto.Transaction.Req) -> Iterator[transaction_proto.Transaction.ResPart]:
        if not self.is_open():
            raise TypeDBClientException.of(TRANSACTION_CLOSED)
        return self._bidirectional_stream.stream(request)

    def commit(self):
        try:
            self.execute(transaction_commit_req())
        finally:
            self.close()

    def rollback(self):
        self.execute(transaction_rollback_req())

    def close(self):
        self._bidirectional_stream.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False
