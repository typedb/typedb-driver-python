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

import six
from collections import deque
from six.moves import queue
from grakn.exception.GraknError import GraknError


class IterationResolver(six.Iterator):
    def __init__(self, communicator, request, is_last_response=lambda x: True):
        self._request = request
        self._is_last_response = is_last_response
        self._communicator = communicator
        self._ended = False
        self._response_buffer = deque()
        communicator._send_with_resolver(self, request)

    def __iter__(self):
        return self

    def __next__(self):
        try:
            return self._response_buffer.popleft()
        except IndexError:
            if self._ended:
                raise StopIteration()
            return self._communicator._block_for_next(self)

    def _buffer_response(self, response):
        self._response_buffer.append(response)

    def _end(self):
        self._ended = False


class Communicator(six.Iterator):
    def __init__(self, grpc_stream_constructor):
        self._request_queue = queue.Queue()
        self._resolver_queue = deque()
        self._response_iterator = grpc_stream_constructor(self)
        self._closed = False

    def __iter__(self):
        return self

    # Used by the GRPC stream to iterate requests as they arrive
    def __next__(self):
        request = self._request_queue.get(block=True)
        if request is None:
            raise StopIteration()
        return request

    # Put a request for GRPC to consume
    def _send_with_resolver(self, resolver, request):
        self._resolver_queue.append(resolver)
        self._request_queue.put(request)

    def _block_for_next(self, resolver):
        if self._closed:
            raise GraknError("This connection is closed")
        while True:
            current = None
            try:
                current = self._resolver_queue[0]
                response = next(self._response_iterator)
                if current._is_last_response(response):
                    self._resolver_queue.popleft()
                    current._ended = True
                if current is resolver:
                    return response
                current._buffer_response(response)  # This result is for another

            except Exception as e:
                self._closed = True
                if not current:
                    raise GraknError("Internal client/protocol error, request/response pair not matched: {0}\n\n Ensure client version is compatible with server version.".format(e))
                raise GraknError("Server/network error: {0}\n\n generated from request: {1}".format(e, current._request))

    def iteration_request(self, request, is_last_response):
        return IterationResolver(self, request, is_last_response)

    def single_request(self, request):
        return next(IterationResolver(self, request))

    def close(self):
        if not self._closed:
            with self._request_queue.mutex:  # probably don't even need the mutex
                self._request_queue.queue.clear()
            self._request_queue.put(None)
            self._closed = True
            # force exhaust the iterator so `onCompleted()` is called on the server
            try:
                next(self._response_iterator)
            except StopIteration:
                pass


class OldCommunicator(six.Iterator):
    """ An iterator and interface for GRPC stream """

    def __init__(self, grpc_stream_constructor):
        self._queue = queue.Queue()
        self._response_iterator = grpc_stream_constructor(self)
        self._closed = False

    def _add_request(self, request):
        self._queue.put(request)

    def __next__(self):
        # print("`next` called on Communicator")
        # print("Current queue: {0}".format(list(self._queue.queue)))
        next_item = self._queue.get(block=True)
        if next_item is None:
            raise StopIteration()
        return next_item

    def __iter__(self):
        return self

    def send_only(self, request):
        if self._closed:
            raise GraknError("This connection is closed")
        try:
            self._add_request(request)
        except Exception as e:
            self._closed = True
            raise GraknError("Server/network error: {0}\n\n generated from request: {1}".format(e, request))

    def send_receive(self, request):
        if self._closed:
            # TODO integrate this into TransactionService to throw a "Transaction is closed" rather than "connection is closed..."
            raise GraknError("This connection is closed")
        try:
            self._add_request(request)
            response = next(self._response_iterator)
        except Exception as e:  # specialize into different gRPC exceptions?
            # invalidate this communicator, functionally this occurs automatically on exception (iterator not usable anymore)
            self._closed = True
            raise GraknError("Server/network error: {0}\n\n generated from request: {1}".format(e, request))

        if response is None:
            raise GraknError("No response received")

        return response

    def receive(self):
        if self._closed:
            raise GraknError("This connection is closed")
        try:
            response = next(self._response_iterator)
        except Exception as e:  # specialize into different gRPC exceptions?
            self._closed = True
            raise GraknError("Server/network error: {0}".format(e))

        if response is None:
            raise GraknError("No response received")

        return response

    def close(self):
        if not self._closed:
            with self._queue.mutex:  # probably don't even need the mutex
                self._queue.queue.clear()
            self._queue.put(None)
            self._closed = True
            # force exhaust the iterator so `onCompleted()` is called on the server
            try:
                next(self._response_iterator)
            except StopIteration:
                pass
