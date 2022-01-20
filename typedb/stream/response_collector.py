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
from typing import Generic, TypeVar, Dict, Optional, Union
from uuid import UUID

from grpc import RpcError
from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED, ILLEGAL_STATE, \
    TRANSACTION_CLOSED_WITH_ERRORS

R = TypeVar('R')


class ResponseCollector(Generic[R]):

    def __init__(self):
        self._collectors: Dict[UUID, ResponseCollector.Queue[R]] = {}
        self._collectors_lock = Lock()

    def new_queue(self, request_id: UUID):
        with self._collectors_lock:
            collector: ResponseCollector.Queue[R] = ResponseCollector.Queue()
            self._collectors[request_id] = collector
            return collector

    def get(self, request_id: UUID) -> Optional["ResponseCollector.Queue[R]"]:
        return self._collectors.get(request_id)

    def remove(self, request_id: UUID):
        with self._collectors_lock:
            del self._collectors[request_id]

    def close(self, error: Optional[TypeDBClientException]):
        with self._collectors_lock:
            for collector in self._collectors.values():
                collector.close(error)

    def get_errors(self) -> [TypeDBClientException]:
        errors = []
        with self._collectors_lock:
            for collector in self._collectors.values():
                error = collector.get_error()
                if error is not None:
                    errors.append(error)
        return errors

    class Queue(Generic[R]):

        def __init__(self):
            self._response_queue: queue.Queue[Union[Response[R], Done]] = queue.Queue()
            self._error: TypeDBClientException = None

        def get(self, block: bool) -> R:
            response = self._response_queue.get(block=block)
            if response.is_response():
                return response.message
            elif response.is_done() and self._error is None:
                raise TypeDBClientException.of(TRANSACTION_CLOSED)
            elif response.is_done() and self._error is not None:
                raise TypeDBClientException.of(TRANSACTION_CLOSED_WITH_ERRORS, self._error)
            else:
                raise TypeDBClientException.of(ILLEGAL_STATE)

        def put(self, response: R):
            self._response_queue.put(Response(response))

        def close(self, error: Optional[TypeDBClientException]):
            self._error = error
            self._response_queue.put(Done())

        def get_error(self) -> TypeDBClientException:
            return self._error


class Response(Generic[R]):

    def __init__(self, value: R):
        self.message = value

    def is_response(self):
        return True

    def is_done(self):
        return False


class Done:

    def __init__(self):
        pass

    def is_response(self):
        return False

    def is_done(self):
        return True
