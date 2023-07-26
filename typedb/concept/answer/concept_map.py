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
from typing import Mapping, Dict, Tuple, Iterator

import typedb_protocol.common.answer_pb2 as answer_proto

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.concept.concept import Concept
from typedb.common.exception import TypeDBClientException, VARIABLE_DOES_NOT_EXIST, NONEXISTENT_EXPLAINABLE_CONCEPT, \
    NONEXISTENT_EXPLAINABLE_OWNERSHIP, MISSING_VARIABLE
from typedb.common.streamer import Streamer
from typedb.concept.concept import _Concept
from typedb.concept.proto import concept_proto_reader

from typedb.typedb_client_python import ConceptMap as NativeConceptMap, concept_map_get_variables, string_iterator_next, \
    concept_map_get_values, concept_iterator_next, concept_map_get, concept_map_get_explainables, concept_map_to_string, \
    concept_map_equals, Explainables as NativeExplainables, Explainable as NativeExplainable, explainables_get_relation, \
    explainables_get_attribute, \
    explainables_get_ownership, explainables_get_relations_keys, explainables_get_attributes_keys, \
    explainables_get_ownerships_keys, string_pair_iterator_next, explainables_to_string, explainables_equals, \
    explainable_get_conjunction, explainable_get_id


class _ConceptMap(ConceptMap):

    def __init__(self, concept_map: NativeConceptMap):
        self._concept_map = concept_map

    # @staticmethod
    # def of(res: answer_proto.ConceptMap) -> "_ConceptMap":
    #     variable_map = {}
    #     for res_var in res.map:
    #         variable_map[res_var] = concept_proto_reader.concept(res.map[res_var])
    #     return _ConceptMap(variable_map, _ConceptMap.Explainables.of(res.explainables))

    def variables(self) -> Iterator[str]:
        return Streamer(concept_map_get_variables(self._concept_map), string_iterator_next)

    def concepts(self) -> Iterator[Concept]:
        return map(_Concept.of, Streamer(concept_map_get_values(self._concept_map), concept_iterator_next))

    def get(self, variable: str) -> Concept:
        if not variable:
            raise TypeDBClientException(MISSING_VARIABLE)
        concept = concept_map_get(self._concept_map, variable)
        if not concept:
            raise TypeDBClientException.of(VARIABLE_DOES_NOT_EXIST, variable)
        return _Concept(concept)

    def explainables(self) -> ConceptMap.Explainables:
        return _ConceptMap.Explainables(concept_map_get_explainables(self._concept_map))

    def __str__(self):
        return concept_map_to_string(self._concept_map)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(other) != type(self):
            return False
        return concept_map_equals(self._concept_map, other._concept_map)

    def __hash__(self):
        return hash((tuple(self.variables()), tuple(self.concepts())))

    class Explainables(ConceptMap.Explainables):

        def __init__(self, explainables: NativeExplainables):
            self._explainables = explainables

        # @staticmethod
        # def of(explainables: answer_proto.Explainables):
        #     relations: Dict[str, ConceptMap.Explainable] = {}
        #     for [var, explainable] in explainables.relations.items():
        #         relations[var] = _ConceptMap.Explainable.of(explainable)
        #     attributes: Dict[str, ConceptMap.Explainable] = {}
        #     for [var, explainable] in explainables.attributes.items():
        #         attributes[var] = _ConceptMap.Explainable.of(explainable)
        #     ownerships: Dict[Tuple[str, str], ConceptMap.Explainable] = {}
        #     for [var, owned_map] in explainables.ownerships.items():
        #         for [owned, explainable] in owned_map.owned.items():
        #             ownerships[(var, owned)] = _ConceptMap.Explainable.of(explainable)
        #     return _ConceptMap.Explainables(relations, attributes, ownerships)

        def relation(self, variable: str) -> ConceptMap.Explainable:
            if not variable:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_relation(self._explainables, variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return _ConceptMap.Explainable(explainable)

        def attribute(self, variable: str) -> ConceptMap.Explainable:
            if not variable:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_attribute(self._explainables, variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return _ConceptMap.Explainable(explainable)

        def ownership(self, owner: str, attribute: str) -> ConceptMap.Explainable:
            if not owner or not attribute:
                raise TypeDBClientException(MISSING_VARIABLE)
            explainable = explainables_get_ownership(self._explainables, owner, attribute)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_OWNERSHIP, (owner, attribute))
            return _ConceptMap.Explainable(explainable)

        def relations(self) -> Mapping[str, ConceptMap.Explainable]:
            return {key: self.relation(key) for key in Streamer(explainables_get_relations_keys(self._explainables),
                                                                string_iterator_next)}

        def attributes(self) -> Mapping[str, ConceptMap.Explainable]:
            return {key: self.attribute(key) for key in Streamer(explainables_get_attributes_keys(self._explainables),
                                                                 string_iterator_next)}

        def ownerships(self) -> Mapping[Tuple[str, str], ConceptMap.Explainable]:
            return {key: self.ownership(*key) for key in Streamer(explainables_get_ownerships_keys(self._explainables),
                                                                  string_pair_iterator_next)}

        def __str__(self):
            return explainables_to_string(self._explainables)

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return explainables_equals(self._explainables, other._explainables)

        def __hash__(self):
            return hash((tuple(self.relations()), tuple(self.attributes()), tuple(self.ownerships())))

    class Explainable(ConceptMap.Explainable):

        def __init__(self, explainable: NativeExplainable):
            self._explainable = explainable

        # @staticmethod
        # def of(explainable: answer_proto.Explainable):
        #     return _ConceptMap.Explainable(explainable.conjunction, explainable.id)

        def conjunction(self) -> str:
            return explainable_get_conjunction(self._explainable)

        def id(self) -> int:
            return explainable_get_id(self._explainable)

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return self.id() == other.id()

        def __hash__(self):
            return hash(self.id())
