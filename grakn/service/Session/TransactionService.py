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
from six.moves import queue, map

from grakn.service.Session.util.RequestBuilder import RequestBuilder
import grakn.service.Session.util.ResponseReader as ResponseReader # for circular import issue
from grakn.service.Session.util import enums
from grakn.exception.GraknError import GraknError


class TransactionService(object):

    def __init__(self, session_id, tx_type, transaction_endpoint):
        self.session_id = session_id
        self.tx_type = tx_type.value

        self._communicator = Communicator(transaction_endpoint)

        # open the transaction with an 'open' message
        open_req = RequestBuilder.open_tx(session_id, tx_type)
        self._communicator.send_receive(open_req)
    __init__.__annotations__ = {'tx_type': enums.TxType}

    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query, infer=True):
        return ResponseReader.ResponseReader.get_query_results(self, Iterator(self._communicator, RequestBuilder.start_iterating_query(query, infer)))
    query.__annotations__ = {'query': str}

    def commit(self):
        request = RequestBuilder.commit()
        self._communicator.send_receive(request)

    def close(self):
        self._communicator.close()

    def is_closed(self):
        return self._communicator._closed

    def get_concept(self, concept_id):
        request = RequestBuilder.get_concept(concept_id)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.get_concept(self, response.getConcept_res)
    get_concept.__annotations__ = {'concept_id': str}

    def get_schema_concept(self, label):
        request = RequestBuilder.get_schema_concept(label)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.get_schema_concept(self, response.getSchemaConcept_res)
    get_schema_concept.__annotations__ = {'label': str}

    def get_attributes_by_value(self, attribute_value, data_type):
        request = RequestBuilder.start_iterating_get_attributes_by_value(attribute_value, data_type)
        iterator = Iterator(self._communicator, request)
        return ResponseReader.ResponseReader.get_attributes_by_value(self, iterator)
    get_attributes_by_value.__annotations__ = {'data_type': enums.DataType}

    def put_entity_type(self, label):
        request = RequestBuilder.put_entity_type(label)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.put_entity_type(self, response.putEntityType_res)
    put_entity_type.__annotations__ = {'label': str}

    def put_relation_type(self, label):
        request = RequestBuilder.put_relation_type(label)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.put_relation_type(self, response.putRelationType_res)
    put_relation_type.__annotations__ = {'label': str}

    def put_attribute_type(self, label, data_type):
        request = RequestBuilder.put_attribute_type(label, data_type)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.put_attribute_type(self, response.putAttributeType_res)
    put_attribute_type.__annotations__ = {'label': str, 'data_type': enums.DataType}

    def put_role(self, label):
        request = RequestBuilder.put_role(label)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.put_role(self, response.putRole_res)
    put_role.__annotations__ = {'label': str}

    def put_rule(self, label, when, then):
        request = RequestBuilder.put_rule(label, when, then)
        response = self._communicator.send_receive(request)
        return ResponseReader.ResponseReader.put_rule(self, response.putRule_res)
    put_rule.__annotations__ = {'label': str, 'when': str, 'then': str}

    # --- Transaction Messages ---

    def run_concept_method(self, concept_id, grpc_concept_method_req):
        # wrap method_req into a transaction message
        tx_request = RequestBuilder.concept_method_req_to_tx_req(concept_id, grpc_concept_method_req)
        response = self._communicator.send_receive(tx_request)
        return response.conceptMethod_res.response

    def run_concept_iter_method(self, concept_id, grpc_concept_iter_method_req):
        return map(lambda res: res.conceptMethod_iter_res.response, Iterator(self._communicator, RequestBuilder.start_iterating_concept_method(concept_id, grpc_concept_iter_method_req)))

    def explanation(self, explainable):
        """ Retrieve the explanation of a Concept Map from the server """
        tx_request = RequestBuilder.explanation(explainable)
        response = self._communicator.send_receive(tx_request)
        return ResponseReader.ResponseReader.create_explanation(self, response.explanation_res)


class Iterator(six.Iterator):
    def __init__(self, communicator, iter_req):
        self._id = 0
        self._communicator = communicator
        self._iter_req = iter_req
        self._buffer = []
        self._start_iterating()
        self._receive_batch()

    def _start_iterating(self):
        self._communicator.send_only(RequestBuilder.iter_req_to_tx_req(self._iter_req))
        self._state = 'ITERATING'

    def _request_batch(self):
        self._communicator.send_only(RequestBuilder.iter_req_to_tx_req(
            RequestBuilder.continue_iterating(self._id, self._iter_req.options)))

    def _receive_batch(self):
        while True:
            transaction_res = self._communicator.receive()
            iter_res = transaction_res.iter_res
            which_one = iter_res.WhichOneof('res')
            if which_one == 'done':
                self._state = 'DONE'
                return
            elif which_one == 'iteratorId':
                self._id = iter_res.iteratorId
                return
            else:
                self._buffer.append(iter_res)

    def __iter__(self):
        return self

    def __next__(self):
        # Pop until buffer is empty
        if len(self._buffer) > 0:
            return self._buffer.pop(0)

        if self._state == 'ITERATING':
            self._request_batch()
        elif self._state == 'DONE':
            raise StopIteration()
        self._receive_batch()
        return self.__next__()


class Communicator(six.Iterator):
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
        except Exception as e: # specialize into different gRPC exceptions?
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
            with self._queue.mutex: # probably don't even need the mutex
                self._queue.queue.clear()
            self._queue.put(None)
            self._closed = True
            # force exhaust the iterator so `onCompleted()` is called on the server
            try:
                next(self._response_iterator)
            except StopIteration:
                pass
