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
from queue import Empty, Queue
from typing import TypeVar, Iterator, Union, Generic
from uuid import uuid4, UUID

import typedb_protocol.common.transaction_pb2 as transaction_proto
from grpc import RpcError

from typedb.api.query.future import QueryFuture
from typedb.common.concurrent.atomic import AtomicBoolean
from typedb.common.exception import TypeDBClientException, UNKNOWN_REQUEST_ID, TRANSACTION_CLOSED, ILLEGAL_ARGUMENT
from typedb.common.rpc.stub import TypeDBStub
from typedb.stream.request_transmitter import RequestTransmitter
from typedb.stream.response_collector import ResponseCollector
from typedb.stream.response_part_iterator import ResponsePartIterator

T = TypeVar('T')


class BidirectionalStream:

    def __init__(self, stub: TypeDBStub, transmitter: RequestTransmitter):
        self._response_collector: ResponseCollector[Union[transaction_proto.Transaction.Res, transaction_proto.Transaction.ResPart]] = ResponseCollector()
        self._request_iterator = RequestIterator()
        self._response_iterator = stub.transaction(self._request_iterator)
        self._dispatcher = transmitter.dispatcher(self._request_iterator)
        self._is_open = AtomicBoolean(True)

    def single(self, req: transaction_proto.Transaction.Req, batch: bool) -> "BidirectionalStream.Single[transaction_proto.Transaction.Res]":
        request_id = uuid4()
        req.req_id = request_id.bytes
        self._response_collector.new_queue(request_id)
        if batch:
            self._dispatcher.dispatch(req)
        else:
            self._dispatcher.dispatch_now(req)
        return BidirectionalStream.Single(request_id, self)

    def stream(self, req: transaction_proto.Transaction.Req) -> Iterator[transaction_proto.Transaction.ResPart]:
        request_id = uuid4()
        req.req_id = request_id.bytes
        self._response_collector.new_queue(request_id)
        self._dispatcher.dispatch(req)
        return ResponsePartIterator(request_id, self, self._dispatcher)

    def is_open(self) -> bool:
        return self._is_open.get()

    def fetch(self, request_id: UUID) -> Union[transaction_proto.Transaction.Res, transaction_proto.Transaction.ResPart]:
        # Keep taking responses until we get one that matches the request ID
        while True:
            try:
                return self._response_collector.get(request_id).get(block=False)
            except Empty:
                pass

            try:
                if not self._is_open.get():
                    raise TypeDBClientException.of(TRANSACTION_CLOSED)
                server_msg = next(self._response_iterator)
            except RpcError as e:
                self.close(e)
                raise TypeDBClientException.of_rpc(e)
            except StopIteration:
                self.close()
                raise TypeDBClientException.of(TRANSACTION_CLOSED)

            server_case = server_msg.WhichOneof("server")
            if server_case == "res":
                self._collect(server_msg.res)
            elif server_case == "res_part":
                self._collect(server_msg.res_part)
            else:
                raise TypeDBClientException.of(ILLEGAL_ARGUMENT)

    def _collect(self, response: Union[transaction_proto.Transaction.Res, transaction_proto.Transaction.ResPart]):
        request_id = UUID(bytes=response.req_id)
        collector = self._response_collector.get(request_id)
        if collector:
            collector.put(response)
        else:
            raise TypeDBClientException.of(UNKNOWN_REQUEST_ID, request_id)

    def close(self, error: RpcError = None):
        if self._is_open.compare_and_set(True, False):
            self._response_collector.close(error)
            try:
                self._dispatcher.close()
            except RpcError as e:
                raise TypeDBClientException.of_rpc(e)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False

    # TODO: Ideally, stream package should not have to depend on api
    class Single(QueryFuture[T]):

        def __init__(self, request_id: UUID, stream: "BidirectionalStream"):
            self._request_id = request_id
            self._stream = stream

        def get(self) -> T:
            return self._stream.fetch(self._request_id)


class RequestIterator(Iterator[Union[transaction_proto.Transaction.Req, StopIteration]]):

    def __init__(self):
        self._request_queue: Queue[Union[transaction_proto.Transaction.Req, StopIteration]] = Queue()

    def __iter__(self):
        return self

    # The gRPC stream continuously iterates this Iterator.
    def __next__(self):
        request = self._request_queue.get(block=True)
        if request is StopIteration:
            raise StopIteration()  # Ends the stream.
        return request

    def put(self, request: transaction_proto.Transaction.Req):
        self._request_queue.put(request)

    def close(self):
        self._request_queue.put(StopIteration)
