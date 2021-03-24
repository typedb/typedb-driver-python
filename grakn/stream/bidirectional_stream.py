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

import grakn_protocol.common.transaction_pb2 as transaction_proto
from grpc import RpcError

from grakn.api.query.future import QueryFuture
from grakn.common.concurrent.atomic import AtomicBoolean
from grakn.common.exception import GraknClientException, UNKNOWN_REQUEST_ID, TRANSACTION_CLOSED, ILLEGAL_ARGUMENT
from grakn.common.rpc.stub import GraknCoreStub
from grakn.stream.request_transmitter import RequestTransmitter
from grakn.stream.response_collector import ResponseCollector
from grakn.stream.response_iterator import StreamedResponseIterator

T = TypeVar('T')


class BidirectionalStream:

    def __init__(self, stub: GraknCoreStub, transmitter: RequestTransmitter):
        self._res_part_collector = ResponseCollector()
        self._res_collector = ResponseCollector()
        self._request_iterator = RequestIterator()
        self._response_iterator = stub.transaction(self._request_iterator)
        self._dispatcher = transmitter.dispatcher(self._request_iterator)
        self._is_open = AtomicBoolean(True)

    def single(self, req: transaction_proto.Transaction.Req, batch: bool) -> "BidirectionalStream.Single[transaction_proto.Transaction.Res]":
        request_id = uuid4()
        req.req_id = str(request_id)
        res_queue = self._res_collector.new_queue(request_id)  # TODO: unused
        if batch:
            self._dispatcher.dispatch(req)
        else:
            self._dispatcher.dispatch_now(req)
        return BidirectionalStream.Single(request_id, self)

    def stream(self, req: transaction_proto.Transaction.Req) -> Iterator[transaction_proto.Transaction.ResPart]:
        request_id = uuid4()
        req.req_id = str(request_id)
        res_part_queue = self._res_part_collector.new_queue(request_id)
        self._dispatcher.dispatch(req)
        return StreamedResponseIterator(request_id, res_part_queue, self._dispatcher)

    def is_open(self) -> bool:
        return self._is_open.get()

    def fetch_res(self, request_id: UUID):
        # Keep taking responses until we get one that matches the request ID
        while True:
            try:
                return self._res_collector.get(request_id).get(block=False)
            except Empty:
                pass

            try:
                if not self._is_open.get():
                    raise GraknClientException.of(TRANSACTION_CLOSED)
                server_msg = next(self._response_iterator)
            except RpcError as e:
                self.close(e)
                raise GraknClientException.of_rpc(e)
            except StopIteration:
                self.close()
                raise GraknClientException.of(TRANSACTION_CLOSED)

            server_case = server_msg.WhichOneof("server")
            if server_case == "res":
                self._collect_res(server_msg.res)
            elif server_case == "res_part":
                self._collect_res_part(server_msg.res_part)
            else:
                raise GraknClientException.of(ILLEGAL_ARGUMENT)

    def fetch_res_part(self, request_id: UUID):
        pass

    def _collect_res(self, res: transaction_proto.Transaction.Res) -> None:
        request_id = UUID(res.req_id)
        collector = self._res_collector.get(request_id)
        if collector:
            collector.put(res)
        else:
            raise GraknClientException.of(UNKNOWN_REQUEST_ID, request_id)

    def _collect_res_part(self, res_part: transaction_proto.Transaction.Res) -> None:
        request_id = UUID(res_part.req_id)
        collector = self._res_part_collector.get(request_id)
        if collector:
            collector.put(res_part)
        else:
            raise GraknClientException.of(UNKNOWN_REQUEST_ID, request_id)

    def close(self, error: RpcError = None):
        if self._is_open.compare_and_set(True, False):
            self._res_collector.close(error)
            self._res_part_collector.close(error)
            try:
                self._dispatcher.close()
            except RpcError as e:
                raise GraknClientException.of_rpc(e)

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
            return self._stream.fetch_res(self._request_id)


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
