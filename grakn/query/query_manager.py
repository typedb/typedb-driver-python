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

import graknprotocol.protobuf.query_pb2 as query_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn import grakn_proto_builder
from grakn.concept.answer import concept_map
from grakn.options import GraknOptions


class QueryManager(object):

    def __init__(self, transaction):
        self._transaction = transaction

    def match(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        match_req = query_proto.Graql.Match.Req()
        match_req.query = query
        request.match_req.CopyFrom(match_req)
        return map(lambda answer_proto: concept_map._of(answer_proto), self._iterate_query(request, lambda res: res.query_res.match_res.answers, options))

    def insert(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        insert_req = query_proto.Graql.Insert.Req()
        insert_req.query = query
        request.insert_req.CopyFrom(insert_req)
        return map(lambda answer_proto: concept_map._of(answer_proto), self._iterate_query(request, lambda res: res.query_res.insert_res.answers, options))

    def delete(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        delete_req = query_proto.Graql.Delete.Req()
        delete_req.query = query
        request.delete_req.CopyFrom(delete_req)
        self._run_query(request, options)

    def define(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        define_req = query_proto.Graql.Define.Req()
        define_req.query = query
        request.define_req.CopyFrom(define_req)
        self._run_query(request, options)

    def undefine(self, query: str, options=GraknOptions()):
        request = query_proto.Query.Req()
        undefine_req = query_proto.Graql.Undefine.Req()
        undefine_req.query = query
        request.undefine_req.CopyFrom(undefine_req)
        self._run_query(request, options)

    def _run_query(self, query_req: query_proto.Query.Req, options: GraknOptions):
        req = transaction_proto.Transaction.Req()
        query_req.options.CopyFrom(grakn_proto_builder.options(options))
        req.query_req.CopyFrom(query_req)
        # Using stream makes this request asynchronous.
        self._transaction._stream(req)

    def _iterate_query(self, query_req: query_proto.Query.Req, response_reader: Callable[[transaction_proto.Transaction.Res], List], options: GraknOptions):
        req = transaction_proto.Transaction.Req()
        query_req.options.CopyFrom(grakn_proto_builder.options(options))
        req.query_req.CopyFrom(query_req)
        return self._transaction._stream(req, response_reader)
