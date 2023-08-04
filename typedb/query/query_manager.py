#
# Copyright (C) 2022 Vaticle
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

from __future__ import annotations
from typing import TYPE_CHECKING, Iterator, Optional

from typedb.api.connection.options import TypeDBOptions
from typedb.api.query.query_manager import QueryManager
from typedb.common.exception import TypeDBClientException, TRANSACTION_CLOSED, MISSING_QUERY
from typedb.common.streamer import Streamer
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.concept.answer.concept_map_group import _ConceptMapGroup
from typedb.concept.answer.numeric import _Numeric
from typedb.concept.answer.numeric_group import _NumericGroup
from typedb.logic.explanation import _Explanation
from typedb.typedb_client_python import query_match, concept_map_iterator_next, query_match_group, \
    concept_map_group_iterator_next, query_insert, query_update, query_explain, explanation_iterator_next, \
    query_match_aggregate, numeric_group_iterator_next, query_match_group_aggregate, query_delete, query_define, \
    query_undefine

if TYPE_CHECKING:
    from typedb.api.answer.concept_map import ConceptMap
    from typedb.api.answer.concept_map_group import ConceptMapGroup
    from typedb.api.answer.numeric import Numeric
    from typedb.api.answer.numeric_group import NumericGroup
    from typedb.api.logic.explanation import Explanation
    from typedb.typedb_client_python import Transaction as NativeTransaction


class _QueryManager(QueryManager):

    def __init__(self, transaction: NativeTransaction):
        self._transaction = transaction

    def match(self, query: str, options: Optional[TypeDBOptions] = None) -> Iterator[ConceptMap]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return map(_ConceptMap, Streamer(query_match(self._transaction, query, options.native_object),
                                         concept_map_iterator_next))

    def match_aggregate(self, query: str, options: Optional[TypeDBOptions] = None) -> Numeric:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return _Numeric(query_match_aggregate(self._transaction, query, options.native_object))

    def match_group(self, query: str, options: Optional[TypeDBOptions] = None) -> Iterator[ConceptMapGroup]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return map(_ConceptMapGroup, Streamer(query_match_group(self._transaction, query, options.native_object),
                                              concept_map_group_iterator_next))

    def match_group_aggregate(self, query: str, options: Optional[TypeDBOptions] = None) -> Iterator[NumericGroup]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return map(_NumericGroup, Streamer(query_match_group_aggregate(self._transaction, query, options.native_object),
                                           numeric_group_iterator_next))

    def insert(self, query: str, options: Optional[TypeDBOptions] = None) -> Iterator[ConceptMap]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return map(_ConceptMap, Streamer(query_insert(self._transaction, query, options.native_object),
                                         concept_map_iterator_next))

    def delete(self, query: str, options: Optional[TypeDBOptions] = None) -> None:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return query_delete(self._transaction, query, options.native_object)

    def update(self, query: str, options: Optional[TypeDBOptions] = None) -> Iterator[ConceptMap]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return map(_ConceptMap, Streamer(query_update(self._transaction, query, options.native_object),
                                         concept_map_iterator_next))

    def define(self, query: str, options: TypeDBOptions = None) -> None:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return query_define(self._transaction, query, options.native_object)

    def undefine(self, query: str, options: TypeDBOptions = None) -> None:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not query:
            raise TypeDBClientException(MISSING_QUERY)
        if not options:
            options = TypeDBOptions()
        return query_undefine(self._transaction, query, options.native_object)

    def explain(self, explainable: ConceptMap.Explainable, options: Optional[TypeDBOptions] = None
                ) -> Iterator[Explanation]:
        if not self._transaction.thisown:
            raise TypeDBClientException(TRANSACTION_CLOSED)
        if not options:
            options = TypeDBOptions()
        return map(_Explanation, Streamer(query_explain(self._transaction, explainable.id(), options.native_object),
                                          explanation_iterator_next))
