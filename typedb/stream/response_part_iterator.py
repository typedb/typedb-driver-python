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
from enum import Enum
from typing import Iterator, TYPE_CHECKING
from uuid import UUID

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.common.exception import TypeDBClientException, ILLEGAL_ARGUMENT, MISSING_RESPONSE, ILLEGAL_STATE
from typedb.common.rpc.request_builder import transaction_stream_req
from typedb.stream.request_transmitter import RequestTransmitter

if TYPE_CHECKING:
    from typedb.stream.bidirectional_stream import BidirectionalStream


class ResponsePartIterator(Iterator[transaction_proto.Transaction.ResPart]):

    def __init__(self, request_id: UUID, bidirectional_stream: "BidirectionalStream", request_dispatcher: RequestTransmitter.Dispatcher):
        self._request_id = request_id
        self._dispatcher = request_dispatcher
        self._bidirectional_stream = bidirectional_stream
        self._state = ResponsePartIterator.State.EMPTY
        self._next: transaction_proto.Transaction.ResPart = None

    class State(Enum):
        EMPTY = 0,
        FETCHED = 1,
        DONE = 2

    def _fetch_and_check(self) -> bool:
        res_part = self._bidirectional_stream.fetch(self._request_id)
        res_case = res_part.WhichOneof("res")
        if res_case == "stream_res_part":
            state = res_part.stream_res_part.state
            if state == transaction_proto.Transaction.Stream.State.Value("DONE"):
                self._state = ResponsePartIterator.State.DONE
                return False
            elif state == transaction_proto.Transaction.Stream.State.Value("CONTINUE"):
                self._dispatcher.dispatch(transaction_stream_req(self._request_id))
                return self._fetch_and_check()
            else:
                raise TypeDBClientException.of(ILLEGAL_ARGUMENT)
        elif res_case is None:
            raise TypeDBClientException.of(MISSING_RESPONSE, self._request_id)
        else:
            self._next = res_part
            self._state = ResponsePartIterator.State.FETCHED
            return True

    def _has_next(self) -> bool:
        if self._state == ResponsePartIterator.State.DONE:
            return False
        elif self._state == ResponsePartIterator.State.FETCHED:
            return True
        elif self._state == ResponsePartIterator.State.EMPTY:
            return self._fetch_and_check()
        else:
            raise TypeDBClientException.of(ILLEGAL_STATE)

    def __next__(self) -> transaction_proto.Transaction.ResPart:
        if not self._has_next():
            raise StopIteration
        self._state = ResponsePartIterator.State.EMPTY
        return self._next
