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

import queue
from threading import Lock
from typing import Generic, TypeVar, Dict, Optional
from uuid import UUID

from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED, ILLEGAL_STATE, \
    TRANSACTION_CLOSED_WITH_ERRORS

R = TypeVar('R')


class ResponseCollector(Generic[R]):

    def __init__(self):
        self._response_queues: Dict[UUID, ResponseCollector.Queue[R]] = {}
        self._collectors_lock = Lock()

    def new_queue(self, request_id: UUID):
        with self._collectors_lock:
            collector: ResponseCollector.Queue[R] = ResponseCollector.Queue()
            self._response_queues[request_id] = collector
            return collector

    def get(self, request_id: UUID) -> Optional["ResponseCollector.Queue[R]"]:
        return self._response_queues.get(request_id)

    def remove(self, request_id: UUID):
        with self._collectors_lock:
            del self._response_queues[request_id]

    def close(self, error: Optional[TypeDBClientException]):
        with self._collectors_lock:
            for collector in self._response_queues.values():
                collector.close(error)

    class Queue(Generic[R]):

        def __init__(self):
            self._response_queue: queue.Queue[Response] = queue.Queue()
            self._error: TypeDBClientException = None

        def get(self, block: bool) -> R:
            response = self._response_queue.get(block=block)
            if response.is_value():
                return response.value
            elif response.is_done():
                self._raise_transaction_closed_error()
            else:
                raise TypeDBClientException.of(ILLEGAL_STATE)

        def _raise_transaction_closed_error(self):
            raise TypeDBClientException.of(TRANSACTION_CLOSED_WITH_ERRORS, self._error) if self._error else TypeDBClientException.of(TRANSACTION_CLOSED)

        def put(self, response: R):
            self._response_queue.put(ValueResponse(response))

        def close(self, error: Optional[TypeDBClientException]):
            self._error = error
            self._response_queue.put(DoneResponse())


class Response:

    def is_value(self):
        return False

    def is_done(self):
        return False


class ValueResponse(Response, Generic[R]):

    def __init__(self, value: R):
        self.value = value

    def is_value(self):
        return True


class DoneResponse(Response):

    def __init__(self):
        pass

    def is_done(self):
        return True
