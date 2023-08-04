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
from typing import Mapping, Iterator, TYPE_CHECKING

from typedb.api.answer.concept_map import ConceptMap
from typedb.common.exception import TypeDBClientException, VARIABLE_DOES_NOT_EXIST, NONEXISTENT_EXPLAINABLE_CONCEPT, \
    NONEXISTENT_EXPLAINABLE_OWNERSHIP, MISSING_VARIABLE
from typedb.common.streamer import Streamer
from typedb.concept.concept import _Concept

from typedb.typedb_client_python import concept_map_get_variables, string_iterator_next, concept_map_get_values, \
    concept_iterator_next, concept_map_get, concept_map_get_explainables, concept_map_to_string, concept_map_equals, \
    explainables_get_relation, explainables_get_attribute, explainables_get_ownership, \
    explainables_get_relations_keys, explainables_get_attributes_keys, explainables_get_ownerships_keys, \
    string_pair_iterator_next, explainables_to_string, explainables_equals, explainable_get_conjunction, \
    explainable_get_id

if TYPE_CHECKING:
    from typedb.api.concept.concept import Concept
    from typedb.typedb_client_python import ConceptMap as NativeConceptMap, Explainables as NativeExplainables, \
        Explainable as NativeExplainable


class _ConceptMap(ConceptMap):

    def __init__(self, concept_map: NativeConceptMap):
        self._native_object = concept_map

    @property
    def native_object(self):
        return self._native_object

    def variables(self) -> Iterator[str]:
        return Streamer(concept_map_get_variables(self.native_object), string_iterator_next)

    def concepts(self) -> Iterator[Concept]:
        return map(_Concept.of, Streamer(concept_map_get_values(self.native_object), concept_iterator_next))

    def get(self, variable: str) -> Concept:
        if not variable:
            raise TypeDBClientException(MISSING_VARIABLE)
        concept = concept_map_get(self.native_object, variable)
        if not concept:
            raise TypeDBClientException.of(VARIABLE_DOES_NOT_EXIST, variable)
        return _Concept.of(concept)

    def explainables(self) -> ConceptMap.Explainables:
        return _ConceptMap.Explainables(concept_map_get_explainables(self.native_object))

    def __str__(self):
        return concept_map_to_string(self.native_object)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(other) != type(self):
            return False
        return concept_map_equals(self.native_object, other.native_object)

    def __hash__(self):
        return hash((tuple(self.variables()), tuple(self.concepts())))

    class Explainables(ConceptMap.Explainables):

        def __init__(self, explainables: NativeExplainables):
            self._native_object = explainables

        @property
        def native_object(self):
            return self._native_object

        def relation(self, variable: str) -> ConceptMap.Explainable:
            if not variable:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_relation(self.native_object, variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return _ConceptMap.Explainable(explainable)

        def attribute(self, variable: str) -> ConceptMap.Explainable:
            if not variable:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_attribute(self.native_object, variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return _ConceptMap.Explainable(explainable)

        def ownership(self, owner: str, attribute: str) -> ConceptMap.Explainable:
            if not owner or not attribute:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_ownership(self.native_object, owner, attribute)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_OWNERSHIP, (owner, attribute))
            return _ConceptMap.Explainable(explainable)

        def relations(self) -> Mapping[str, ConceptMap.Explainable]:
            return {key: self.relation(key) for key in Streamer(explainables_get_relations_keys(self.native_object),
                                                                string_iterator_next)}

        def attributes(self) -> Mapping[str, ConceptMap.Explainable]:
            return {key: self.attribute(key) for key in Streamer(explainables_get_attributes_keys(self.native_object),
                                                                 string_iterator_next)}

        def ownerships(self) -> Mapping[tuple[str, str], ConceptMap.Explainable]:
            return {key: self.ownership(*key) for key in Streamer(explainables_get_ownerships_keys(self.native_object),
                                                                  string_pair_iterator_next)}

        def __str__(self):
            return explainables_to_string(self.native_object)

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return explainables_equals(self.native_object, other.native_object)

        def __hash__(self):
            return hash((tuple(self.relations()), tuple(self.attributes()), tuple(self.ownerships())))

    class Explainable(ConceptMap.Explainable):

        def __init__(self, explainable: NativeExplainable):
            self._native_object = explainable

        @property
        def native_object(self):
            return self._native_object

        def conjunction(self) -> str:
            return explainable_get_conjunction(self.native_object)

        def id(self) -> int:
            return explainable_get_id(self.native_object)

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return self.id() == other.id()

        def __hash__(self):
            return hash(self.id())
