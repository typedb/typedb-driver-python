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

# Communicator Architecture
# =========================
#
# This communicator allows us to track the sending of multiple requests and match their responses, which may be streams
# of multiple results, without blocking the initial request call and without any complex threading. This allows the
# usage of a single stream from a single thread to have predictable results, as there will never be race conditions
# between requests.
#
# The result is that requests are:
# * Async: like promises.
# * Serial: always made in the order they are requested by the caller thread.
#
# When a request is made, a Resolver is pushed onto the `resolver_queue`. The Resolver is used to *pull* the correct
# responses from the GRPC stream, which must arrive in a corresponding order but could be pulled by the client in any
# order. There is no "push" from GRPC, the GRPC stream is pulled until the correct responses are found.
#
# No matter which resolver is being resolved, the resolvers *must* be iterated in the order  of the `resolver_queue`,
# since that wil be the order of the responses. If another resolver is ahead of the one we want to retrieve responses
# for, any responses we find are pushed to its buffer, so that when the user later retrieves responses through that
# resolver, it will not miss any. **This push only happens when trying to retrieve results out of the order you request
# them**, it does not happen in a separate thread!
#
# As a final step, closing the transaction must ensure that at least one valid response is received for each resolver,
# to catch and raise any errors before returning control to the user code.


import six
from collections import deque
from six.moves import queue
from grakn.exception.GraknError import GraknError


class SingleResolver:
    def __init__(self, communicator, request):
        self._request = request
        self._communicator = communicator
        self._buffered_response = None
        communicator._send_with_resolver(self, request)

    def _is_last_response(self, response):
        return True

    def _buffer_response(self, response):
        self._buffered_response = response

    def get(self):
        response = self._buffered_response
        if response:
            return response
        else:
            response = self._communicator._block_for_next(self)
            self._buffered_response = response
            return response

    def _end(self):
        pass

    def _on_close(self):
        # Ensure any error is correctly raised by waiting for the response even if we are closing
        self.get()


class IterationResolver(six.Iterator):
    def __init__(self, communicator, request, is_last_response):
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
        self._ended = True

    def _on_close(self):
        try:
            while not self._ended:
                response = self._communicator._block_for_next(self)
                self._buffer_response(response)
        except StopIteration:
            pass


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

    def error_if_closed(self):
        if self._closed:
            raise GraknError("This connection is closed")

    # Put a request for GRPC to consume
    def _send_with_resolver(self, resolver, request):
        self._resolver_queue.append(resolver)
        self._request_queue.put(request)

    def _block_for_next(self, resolver):
        self.error_if_closed()
        while True:
            current = None
            try:
                current = self._resolver_queue[0]
                response = next(self._response_iterator)

                if current._is_last_response(response):
                    self._resolver_queue.popleft()
                    current._end()

                if current is resolver:
                    return response
                else:
                    current._buffer_response(response)

            except Exception as e:
                self._closed = True
                if not current:
                    raise GraknError("Internal client/protocol error, request/response pair not matched: {0}\n\n "
                                     "Ensure client version is compatible with server version.".format(e))
                raise GraknError("Server/network error: {0}\n\n generated from request: {1}".format(e, current._request))

    def iteration_request(self, request, is_last_response):
        self.error_if_closed()
        return IterationResolver(self, request, is_last_response)

    def single_request(self, request):
        self.error_if_closed()
        return SingleResolver(self, request).get()

    def close(self):
        if not self._closed:

            # Exhaust all resolvers, ensuring that transaction is closed without error

            for resolver in list(self._resolver_queue):
                resolver._on_close()

            with self._request_queue.mutex:  # probably don't even need the mutex
                self._request_queue.queue.clear()
            self._request_queue.put(None)
            self._closed = True
            # force exhaust the iterator so `onCompleted()` is called on the server
            try:
                next(self._response_iterator)
            except StopIteration:
                pass
