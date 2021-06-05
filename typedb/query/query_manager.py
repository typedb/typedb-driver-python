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

from typing import TYPE_CHECKING, Union, Iterator

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.answer.concept_map_group import ConceptMapGroup
from typedb.api.answer.numeric import Numeric
from typedb.api.answer.numeric_group import NumericGroup
from typedb.api.logic.explanation import Explanation
from typedb.api.connection.options import TypeDBOptions
from typedb.api.query.future import QueryFuture
from typedb.api.query.query_manager import QueryManager
from typedb.common.rpc.request_builder import query_manager_match_req, query_manager_match_aggregate_req, \
    query_manager_match_group_req, query_manager_match_group_aggregate_req, query_manager_insert_req, \
    query_manager_delete_req, query_manager_update_req, query_manager_define_req, query_manager_undefine_req, \
    query_manager_explain_req
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.concept.answer.concept_map_group import _ConceptMapGroup
from typedb.concept.answer.numeric import _Numeric
from typedb.concept.answer.numeric_group import _NumericGroup
from typedb.logic.explanation import _Explanation

if TYPE_CHECKING:
    from typedb.api.connection.transaction import _TypeDBTransactionExtended, TypeDBTransaction


class _QueryManager(QueryManager):

    def __init__(self, transaction_ext: Union["_TypeDBTransactionExtended", "TypeDBTransaction"]):
        self._transaction_ext = transaction_ext

    def match(self, query: str, options: TypeDBOptions = None) -> Iterator[ConceptMap]:
        if not options:
            options = TypeDBOptions.core()
        return (_ConceptMap.of(cm) for rp in self.stream(query_manager_match_req(query, options.proto())) for cm in rp.match_res_part.answers)

    def match_aggregate(self, query: str, options: TypeDBOptions = None) -> QueryFuture[Numeric]:
        if not options:
            options = TypeDBOptions.core()
        return self.query(query_manager_match_aggregate_req(query, options.proto())).map(lambda res: _Numeric.of(res.match_aggregate_res.answer))

    def match_group(self, query: str, options: TypeDBOptions = None) -> Iterator[ConceptMapGroup]:
        if not options:
            options = TypeDBOptions.core()
        return (_ConceptMapGroup.of(cmg) for rp in self.stream(query_manager_match_group_req(query, options.proto()))
                for cmg in rp.match_group_res_part.answers)

    def match_group_aggregate(self, query: str, options: TypeDBOptions = None) -> Iterator[NumericGroup]:
        if not options:
            options = TypeDBOptions.core()
        return (_NumericGroup.of(ng) for rp in self.stream(query_manager_match_group_aggregate_req(query, options.proto()))
                for ng in rp.match_group_aggregate_res_part.answers)

    def insert(self, query: str, options: TypeDBOptions = None) -> Iterator[ConceptMap]:
        if not options:
            options = TypeDBOptions.core()
        return (_ConceptMap.of(cm) for rp in self.stream(query_manager_insert_req(query, options.proto())) for cm in rp.insert_res_part.answers)

    def delete(self, query: str, options: TypeDBOptions = None) -> QueryFuture:
        if not options:
            options = TypeDBOptions.core()
        return self.query_void(query_manager_delete_req(query, options.proto()))

    def update(self, query: str, options: TypeDBOptions = None) -> Iterator[ConceptMap]:
        if not options:
            options = TypeDBOptions.core()
        return (_ConceptMap.of(cm) for rp in self.stream(query_manager_update_req(query, options.proto())) for cm in rp.update_res_part.answers)

    def explain(self, explainable: ConceptMap.Explainable, options: TypeDBOptions = None) -> Iterator[Explanation]:
        if not options:
            options = TypeDBOptions.core()
        return (_Explanation.of(ex) for rp in self.stream(query_manager_explain_req(explainable.explainable_id(), options.proto())) for ex in rp.explain_res_part.explanations)

    def define(self, query: str, options: TypeDBOptions = None) -> QueryFuture:
        if not options:
            options = TypeDBOptions.core()
        return self.query_void(query_manager_define_req(query, options.proto()))

    def undefine(self, query: str, options: TypeDBOptions = None) -> QueryFuture:
        if not options:
            options = TypeDBOptions.core()
        return self.query_void(query_manager_undefine_req(query, options.proto()))

    def query_void(self, req: transaction_proto.Transaction.Req):
        return self._transaction_ext.run_query(req)

    def query(self, req: transaction_proto.Transaction.Req):
        return self._transaction_ext.run_query(req).map(lambda res: res.query_manager_res)

    def stream(self, req: transaction_proto.Transaction.Req):
        return (rp.query_manager_res_part for rp in self._transaction_ext.stream(req))
