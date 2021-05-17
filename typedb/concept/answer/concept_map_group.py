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

import typedb_protocol.common.answer_pb2 as answer_proto

from typedb.api.answer.concept_map_group import ConceptMapGroup
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.concept.proto import concept_proto_reader


class _ConceptMapGroup(ConceptMapGroup):

    def __init__(self, owner, concept_maps):
        self._owner = owner
        self._concept_maps = concept_maps

    @staticmethod
    def of(cm_group: answer_proto.ConceptMapGroup) -> "_ConceptMapGroup":
        owner = concept_proto_reader.concept(cm_group.owner)
        concept_maps = list(map(lambda cm: _ConceptMap.of(cm), cm_group.concept_maps))
        return _ConceptMapGroup(owner, concept_maps)

    def owner(self):
        return self._owner

    def concept_maps(self):
        return self._concept_maps

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(other) != type(self):
            return False
        return other._owner == self._owner and other._concept_maps == self._concept_maps

    def __hash__(self):
        return hash((self._owner, self._concept_maps))
