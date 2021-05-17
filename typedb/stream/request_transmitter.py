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

from queue import Queue, Empty
from threading import Lock, Semaphore, Thread
from time import sleep
from typing import List, TYPE_CHECKING

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.common.concurrent.lock import ReadWriteLock
from typedb.common.exception import TypeDBClientException, CLIENT_CLOSED
from typedb.common.rpc.request_builder import transaction_client_msg

if TYPE_CHECKING:
    from typedb.stream.bidirectional_stream import RequestIterator

BATCH_WINDOW_SMALL_SECS = 0.001
BATCH_WINDOW_LARGE_SECS = 0.003


class RequestTransmitter:

    def __init__(self, parallelisation: int):
        self._executors: List[RequestTransmitter.Executor] = []
        self._executor_index = 0
        self._is_open = True
        self._executor_index_lock = Lock()
        self.access_lock = ReadWriteLock()
        for i in range(parallelisation):
            self._executors.append(RequestTransmitter.Executor(self))

    def _next_executor(self):
        with self._executor_index_lock:
            self._executor_index += 1
            self._executor_index %= len(self._executors)
            return self._executors[self._executor_index]

    def dispatcher(self, request_iterator: "RequestIterator") -> "RequestTransmitter.Dispatcher":
        try:
            self.access_lock.acquire_read()
            if not self._is_open:
                raise TypeDBClientException.of(CLIENT_CLOSED)
            executor = self._next_executor()
            disp = RequestTransmitter.Dispatcher(executor, request_iterator, self)
            executor.dispatchers.append(disp)
            return disp
        finally:
            self.access_lock.release_read()

    def is_open(self):
        return self._is_open

    def close(self):
        try:
            self.access_lock.acquire_write()
            if self._is_open:
                self._is_open = False
                for executor in self._executors:
                    executor.close()
        finally:
            self.access_lock.release_write()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False

    class Executor:

        def __init__(self, transmitter: "RequestTransmitter"):
            self._transmitter = transmitter
            self.dispatchers: List[RequestTransmitter.Dispatcher] = []
            self._is_running = False
            self._permission_to_run = Semaphore(0)
            # TODO: generate thread names
            Thread(target=self.run, daemon=True).start()

        def may_start_running(self):
            if not self._is_running:
                self._is_running = True
                self._permission_to_run.release()

        def run(self):
            while self._transmitter.is_open():
                try:
                    self._permission_to_run.acquire()
                    first = True
                    while True:
                        sleep(BATCH_WINDOW_SMALL_SECS if first else BATCH_WINDOW_LARGE_SECS)
                        if not self.dispatchers:
                            break
                        for dispatcher in self.dispatchers:
                            dispatcher.send_batched_requests()
                        first = False
                finally:
                    self._is_running = False
                if self.dispatchers:
                    self.may_start_running()

        def close(self):
            for dispatcher in self.dispatchers:
                dispatcher.close()
            self.may_start_running()

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
            if exc_tb is not None:
                return False

    class Dispatcher:

        def __init__(self, executor: "RequestTransmitter.Executor", request_iterator: "RequestIterator", transmitter: "RequestTransmitter"):
            self._executor = executor
            self._request_iterator = request_iterator
            self._transmitter = transmitter
            self._request_queue = Queue()
            self._send_batch_lock = Lock()

        def send_batched_requests(self):
            with self._send_batch_lock:
                if not self._transmitter.is_open():
                    return
                requests = []
                while True:
                    try:
                        requests.append(self._request_queue.get(block=False))
                    except Empty:
                        break
                if requests:
                    self._request_iterator.put(transaction_client_msg(requests))

        def dispatch(self, proto_req: transaction_proto.Transaction.Req):
            try:
                self._transmitter.access_lock.acquire_read()
                if not self._transmitter.is_open():
                    raise TypeDBClientException.of(CLIENT_CLOSED)
                self._request_queue.put(proto_req)
                self._executor.may_start_running()
            finally:
                self._transmitter.access_lock.release_read()

        def dispatch_now(self, proto_req: transaction_proto.Transaction.Req):
            try:
                self._transmitter.access_lock.acquire_read()
                if not self._transmitter.is_open():
                    raise TypeDBClientException.of(CLIENT_CLOSED)
                self._request_queue.put(proto_req)
                self.send_batched_requests()
            finally:
                self._transmitter.access_lock.release_read()

        def close(self):
            self._request_iterator.close()
            self._executor.dispatchers.remove(self)

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc_val, exc_tb):
            self.close()
            if exc_tb is not None:
                return False
