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

import grakn_protocol.protobuf.query_pb2 as query_proto
import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn import grakn_proto_builder
from grakn.concept.answer import concept_map, concept_map_group, numeric, numeric_group
from grakn.options import GraknOptions


class QueryManager:

    def __init__(self, transaction):
        self._transaction = transaction

    def match(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        match_req = query_proto.Query.Match.Req()
        match_req.query = query
        request.match_req.CopyFrom(match_req)
        return map(lambda answer_proto: concept_map._of(answer_proto), self._iterate_query(request, lambda res: res.query_res.match_res.answers, options))

    def match_aggregate(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        match_aggregate_req = query_proto.Query.MatchAggregate.Req()
        match_aggregate_req.query = query
        request.match_aggregate_req.CopyFrom(match_aggregate_req)
        return self._iterate_query(request, lambda res: [numeric._of(res.query_res.match_aggregate_res.answer)], options)

    def match_group(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        match_group_req = query_proto.Query.MatchGroup.Req()
        match_group_req.query = query
        request.match_group_req.CopyFrom(match_group_req)
        return map(
            lambda cmg_proto: concept_map_group._of(cmg_proto),
            self._iterate_query(request, lambda res: res.query_res.match_group_res.answers, options)
        )

    def match_group_aggregate(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        match_group_aggregate_req = query_proto.Query.MatchGroupAggregate.Req()
        match_group_aggregate_req.query = query
        request.match_group_aggregate_req.CopyFrom(match_group_aggregate_req)
        return map(
            lambda numeric_group_proto: numeric_group._of(numeric_group_proto),
            self._iterate_query(request, lambda res: res.query_res.match_group_aggregate_res.answers, options)
        )

    def insert(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        insert_req = query_proto.Query.Insert.Req()
        insert_req.query = query
        request.insert_req.CopyFrom(insert_req)
        return map(lambda answer_proto: concept_map._of(answer_proto), self._iterate_query(request, lambda res: res.query_res.insert_res.answers, options))

    def delete(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        delete_req = query_proto.Query.Delete.Req()
        delete_req.query = query
        request.delete_req.CopyFrom(delete_req)
        return self._iterate_query(request, lambda res: [], options)

    def update(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        update_req = query_proto.Query.Update.Req()
        update_req.query = query
        request.update_req.CopyFrom(update_req)
        return map(lambda answer_proto: concept_map._of(answer_proto), self._iterate_query(request, lambda res: res.query_res.update_res.answers, options))

    def define(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        define_req = query_proto.Query.Define.Req()
        define_req.query = query
        request.define_req.CopyFrom(define_req)
        return self._iterate_query(request, lambda res: [], options)

    def undefine(self, query: str, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        request = query_proto.Query.Req()
        undefine_req = query_proto.Query.Undefine.Req()
        undefine_req.query = query
        request.undefine_req.CopyFrom(undefine_req)
        return self._iterate_query(request, lambda res: [], options)

    def _iterate_query(self, query_req: query_proto.Query.Req, response_reader: Callable[[transaction_proto.Transaction.Res], List], options: GraknOptions):
        req = transaction_proto.Transaction.Req()
        query_req.options.CopyFrom(grakn_proto_builder.options(options))
        req.query_req.CopyFrom(query_req)
        return self._transaction._stream(req, response_reader)
