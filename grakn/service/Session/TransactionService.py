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
from six.moves import map

from grakn.service.Session.util.RequestBuilder import RequestBuilder
import grakn.service.Session.util.ResponseReader as ResponseReader # for circular import issue
from grakn.service.Session.util import enums
from grakn.service.Session.util.Communicator import Communicator
from grakn.exception.GraknError import GraknError


class TransactionService(object):

    def __init__(self, session_id, tx_type, transaction_endpoint):
        self.session_id = session_id
        self.tx_type = tx_type.value

        self._communicator = Communicator(transaction_endpoint)

        # open the transaction with an 'open' message
        open_req = RequestBuilder.open_tx(session_id, tx_type)
        self._communicator.single_request(open_req)
    __init__.__annotations__ = {'tx_type': enums.TxType}

    # --- Passthrough targets ---
    # targets of top level Transaction class

    def query(self, query, infer=True, batch_size=50):
        return ResponseReader.ResponseReader\
            .get_query_results(self, Iterator(self._communicator,
                                              RequestBuilder.start_iterating_query(query, infer, batch_size)))
    query.__annotations__ = {'query': str}

    def commit(self):
        request = RequestBuilder.commit()
        self._communicator.single_request(request)

    def close(self):
        self._communicator.close()

    def is_closed(self):
        return self._communicator._closed

    def get_concept(self, concept_id):
        request = RequestBuilder.get_concept(concept_id)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.get_concept(self, response.getConcept_res)
    get_concept.__annotations__ = {'concept_id': str}

    def get_schema_concept(self, label):
        request = RequestBuilder.get_schema_concept(label)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.get_schema_concept(self, response.getSchemaConcept_res)
    get_schema_concept.__annotations__ = {'label': str}

    def get_attributes_by_value(self, attribute_value, data_type):
        request = RequestBuilder.start_iterating_get_attributes_by_value(attribute_value, data_type)
        iterator = Iterator(self._communicator, request)
        return ResponseReader.ResponseReader.get_attributes_by_value(self, iterator)
    get_attributes_by_value.__annotations__ = {'data_type': enums.DataType}

    def put_entity_type(self, label):
        request = RequestBuilder.put_entity_type(label)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.put_entity_type(self, response.putEntityType_res)
    put_entity_type.__annotations__ = {'label': str}

    def put_relation_type(self, label):
        request = RequestBuilder.put_relation_type(label)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.put_relation_type(self, response.putRelationType_res)
    put_relation_type.__annotations__ = {'label': str}

    def put_attribute_type(self, label, data_type):
        request = RequestBuilder.put_attribute_type(label, data_type)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.put_attribute_type(self, response.putAttributeType_res)
    put_attribute_type.__annotations__ = {'label': str, 'data_type': enums.DataType}

    def put_role(self, label):
        request = RequestBuilder.put_role(label)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.put_role(self, response.putRole_res)
    put_role.__annotations__ = {'label': str}

    def put_rule(self, label, when, then):
        request = RequestBuilder.put_rule(label, when, then)
        response = self._communicator.single_request(request)
        return ResponseReader.ResponseReader.put_rule(self, response.putRule_res)
    put_rule.__annotations__ = {'label': str, 'when': str, 'then': str}

    # --- Transaction Messages ---

    def run_concept_method(self, concept_id, grpc_concept_method_req):
        # wrap method_req into a transaction message
        tx_request = RequestBuilder.concept_method_req_to_tx_req(concept_id, grpc_concept_method_req)
        response = self._communicator.single_request(tx_request)
        return response.conceptMethod_res.response

    def run_concept_iter_method(self, concept_id, grpc_concept_iter_method_req):
        return map(lambda res: res.conceptMethod_iter_res.response,
                   Iterator(self._communicator,
                            RequestBuilder.start_iterating_concept_method(concept_id,grpc_concept_iter_method_req)))

    def explanation(self, explainable):
        """ Retrieve the explanation of a Concept Map from the server """
        tx_request = RequestBuilder.explanation(explainable)
        response = self._communicator.single_request(tx_request)
        return ResponseReader.ResponseReader.create_explanation(self, response.explanation_res)


end_of_batch_results = {'done', 'iteratorId'}


def end_of_batch(res):
    res_type = res.iter_res.WhichOneof('res')
    return res_type == 'done' or res_type == 'iteratorId'


class Iterator(six.Iterator):
    def __init__(self, communicator, iter_req):
        self._communicator = communicator
        self._iter_req = iter_req
        self._response_iterator = self._communicator.iteration_request(
            RequestBuilder.iter_req_to_tx_req(self._iter_req),
            end_of_batch)
        self._done = False

    def __iter__(self):
        return self

    def _request_next_batch(self, iter_id):
        self._response_iterator = self._communicator.iteration_request(
            RequestBuilder.iter_req_to_tx_req(
                RequestBuilder.continue_iterating(iter_id, self._iter_req.options)),
            end_of_batch)

    def __next__(self):
        if self._done:
            raise GraknError('Iterator was already iterated.')

        try:
            response = next(self._response_iterator)
        except StopIteration:
            raise GraknError('Internal client/protocol error,'
                             ' did not receive an expected "done" or "iteratorId" message.'
                             '\n\n Please ensure client version is supported by server version.')

        iter_res = response.iter_res
        res_type = iter_res.WhichOneof('res')
        if res_type == 'done':
            self._done = True
            raise StopIteration
        elif res_type == 'iteratorId':
            self._request_next_batch(iter_res.iteratorId)
            return next(self)
        else:
            return iter_res
