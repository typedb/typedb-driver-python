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

from typing import Callable, List

import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException


class Stream:

    _CONTINUE = "continue"
    _DONE = "done"

    def __init__(self, transaction, request_id: str, transform_response: Callable[[transaction_proto.Transaction.Res], List] = lambda res: None):
        self._transaction = transaction
        self._request_id = request_id
        self._transform_response = transform_response
        self._current_iterator = None

    def __iter__(self):
        return self

    def __next__(self):
        if self._current_iterator is not None:
            try:
                return next(self._current_iterator)
            except StopIteration:
                self._current_iterator = None

        res = self._transaction._fetch(self._request_id)
        res_case = res.WhichOneof("res")
        if res_case == Stream._CONTINUE:
            continue_req = transaction_proto.Transaction.Req()
            continue_req.id = self._request_id
            setattr(continue_req, "continue", True)
            self._transaction._request_iterator.put(continue_req)
            return next(self)
        elif res_case == Stream._DONE:
            raise StopIteration()
        elif res_case is None:
            raise GraknClientException("The required field 'res' of type 'transaction_proto.Transaction.Res' was not set.")
        else:
            self._current_iterator = iter(self._transform_response(res))
            return next(self)
