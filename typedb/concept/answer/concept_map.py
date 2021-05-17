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

from typing import Mapping, Dict, Tuple

import typedb_protocol.common.answer_pb2 as answer_proto

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.concept.concept import Concept
from typedb.common.exception import TypeDBClientException, VARIABLE_DOES_NOT_EXIST, NONEXISTENT_EXPLAINABLE_CONCEPT, \
    NONEXISTENT_EXPLAINABLE_OWNERSHIP
from typedb.concept.proto import concept_proto_reader


class _ConceptMap(ConceptMap):

    def __init__(self, mapping: Mapping[str, Concept], explainables: ConceptMap.Explainables = None):
        self._map = mapping
        self._explainables = explainables

    @staticmethod
    def of(res: answer_proto.ConceptMap) -> "_ConceptMap":
        variable_map = {}
        for res_var in res.map:
            variable_map[res_var] = concept_proto_reader.concept(res.map[res_var])
        return _ConceptMap(variable_map, _ConceptMap.Explainables.of(res.explainables))

    def map(self):
        return self._map

    def concepts(self):
        return self._map.values()

    def get(self, variable: str):
        concept = self._map[variable]
        if not concept:
            raise TypeDBClientException.of(VARIABLE_DOES_NOT_EXIST, variable)
        return concept

    def explainables(self) -> ConceptMap.Explainables:
        return self._explainables

    def __str__(self):
        return "".join(map(lambda var: "[" + var + "/" + str(self._map[var]) + "]", sorted(self._map.keys())))

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(other) != type(self):
            return False
        return other._map == self._map

    def __hash__(self):
        return hash(self._map)

    class Explainables(ConceptMap.Explainables):

        def __init__(self, relations: Mapping[str, ConceptMap.Explainable] = None, attributes: Mapping[str, ConceptMap.Explainable] = None, ownerships: Mapping[Tuple[str, str], ConceptMap.Explainable] = None):
            self._relations = relations
            self._attributes = attributes
            self._ownerships = ownerships

        @staticmethod
        def of(explainables: answer_proto.Explainables):
            relations: Dict[str, ConceptMap.Explainable] = {}
            for [var, explainable] in explainables.relations.items():
                relations[var] = _ConceptMap.Explainable.of(explainable)
            attributes: Dict[str, ConceptMap.Explainable] = {}
            for [var, explainable] in explainables.attributes.items():
                attributes[var] = _ConceptMap.Explainable.of(explainable)
            ownerships: Dict[Tuple[str, str], ConceptMap.Explainable] = {}
            for [var, owned_map] in explainables.ownerships.items():
                for [owned, explainable] in owned_map.owned.items():
                    ownerships[(var, owned)] = _ConceptMap.Explainable.of(explainable)
            return _ConceptMap.Explainables(relations, attributes, ownerships)

        def relation(self, variable: str) -> "ConceptMap.Explainable":
            explainable = self._relations.get(variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return explainable

        def attribute(self, variable: str) -> "ConceptMap.Explainable":
            explainable = self._attributes.get(variable)
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_CONCEPT, variable)
            return explainable

        def ownership(self, owner: str, attribute: str) -> "ConceptMap.Explainable":
            explainable = self._ownerships.get((owner, attribute))
            if not explainable:
                raise TypeDBClientException.of(NONEXISTENT_EXPLAINABLE_OWNERSHIP, (owner, attribute))
            return explainable

        def relations(self) -> Mapping[str, "ConceptMap.Explainable"]:
            return self._relations

        def attributes(self) -> Mapping[str, "ConceptMap.Explainable"]:
            return self._attributes

        def ownerships(self) -> Mapping[Tuple[str, str], "ConceptMap.Explainable"]:
            return self._ownerships

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return self._relations == other._relations and self._attributes == other._attributes and self._ownerships == other._ownerships

        def __hash__(self):
            return hash((self._relations, self._attributes, self._ownerships))

    class Explainable(ConceptMap.Explainable):

        def __init__(self, conjunction: str, explainable_id: int):
            self._conjunction = conjunction
            self._explainable_id = explainable_id

        @staticmethod
        def of(explainable: answer_proto.Explainable):
            return _ConceptMap.Explainable(explainable.conjunction, explainable.id)

        def conjunction(self) -> str:
            return self._conjunction

        def explainable_id(self) -> int:
            return self._explainable_id

        def __eq__(self, other):
            if other is self:
                return True
            if not other or type(other) != type(self):
                return False
            return self._explainable_id

        def __hash__(self):
            return hash(self._explainable_id)
