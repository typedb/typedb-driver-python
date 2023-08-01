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
from typing import Iterator

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.answer.concept_map_group import ConceptMapGroup
from typedb.api.concept.concept import Concept
from typedb.common.streamer import Streamer
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.concept.concept import _Concept

from typedb.typedb_client_python import ConceptMapGroup as NativeConceptMapGroup, concept_map_group_get_owner, \
    concept_map_group_get_concept_maps, concept_map_iterator_next, concept_map_group_to_string, concept_map_group_equals


class _ConceptMapGroup(ConceptMapGroup):

    def __init__(self, concept_map_group: NativeConceptMapGroup):
        self._concept_map_group = concept_map_group

    # @staticmethod
    # def of(cm_group: answer_proto.ConceptMapGroup) -> "_ConceptMapGroup":
    #     owner = concept_proto_reader.concept(cm_group.owner)
    #     concept_maps = list(map(lambda cm: _ConceptMap.of(cm), cm_group.concept_maps))
    #     return _ConceptMapGroup(owner, concept_maps)

    def owner(self) -> Concept:
        return _Concept.of(concept_map_group_get_owner(self._concept_map_group))

    def concept_maps(self) -> Iterator[ConceptMap]:
        return map(_ConceptMap, Streamer(concept_map_group_get_concept_maps(self._concept_map_group),
                                         concept_map_iterator_next))

    def __str__(self):
        return concept_map_group_to_string(self._concept_map_group)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(other) != type(self):
            return False
        return concept_map_group_equals(self._concept_map_group, other._concept_map_group)

    def __hash__(self):
        return hash((self.owner(), tuple(self.concept_maps())))
