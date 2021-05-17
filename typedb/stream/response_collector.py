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

from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED

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

    def close(self, error: Optional[RpcError]):
        with self._collectors_lock:
            for collector in self._collectors.values():
                collector.close(error)

    class Queue(Generic[R]):

        def __init__(self):
            self._response_queue: queue.Queue[Union[Response[R], Done]] = queue.Queue()

        def get(self, block: bool) -> R:
            response = self._response_queue.get(block=block)
            if response.message:
                return response.message
            elif response.error:
                raise TypeDBClientException.of_rpc(response.error)
            else:
                raise TypeDBClientException.of(TRANSACTION_CLOSED)

        def put(self, response: R):
            self._response_queue.put(Response(response))

        def close(self, error: Optional[RpcError]):
            self._response_queue.put(Done(error))


class Response(Generic[R]):

    def __init__(self, value: R):
        self.message = value


class Done:

    def __init__(self, error: Optional[RpcError]):
        self.error = error
