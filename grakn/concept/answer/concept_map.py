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

from typing import Mapping

import grakn_protocol.common.answer_pb2 as answer_proto

from grakn.api.answer.concept_map import ConceptMap
from grakn.api.concept.concept import Concept
from grakn.common.exception import GraknClientException, VARIABLE_DOES_NOT_EXIST
from grakn.concept.proto import concept_proto_reader


class _ConceptMap(ConceptMap):

    def __init__(self, mapping: Mapping[str, Concept]):
        self._map = mapping

    @staticmethod
    def of(concept_map_proto: answer_proto.ConceptMap) -> "_ConceptMap":
        variable_map = {}
        for res_var in concept_map_proto.map:
            variable_map[res_var] = concept_proto_reader.concept(concept_map_proto.map[res_var])
        return _ConceptMap(variable_map)

    def map(self):
        return self._map

    def concepts(self):
        return self._map.values()

    def get(self, variable: str):
        concept = self._map[variable]
        if not concept:
            raise GraknClientException.of(VARIABLE_DOES_NOT_EXIST, variable)
        return concept

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
