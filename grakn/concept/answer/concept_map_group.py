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

import grakn_protocol.protobuf.answer_pb2 as answer_proto
from grakn.concept.answer import concept_map
from grakn.concept.proto.concept_proto_reader import concept

class ConceptMapGroup:
    def __init__(self, owner, concept_maps):
        self._owner = owner
        self._concept_maps = concept_maps

    def owner(self):
        return self._owner

    def concept_maps(self):
        return self._concept_maps


def _of(concept_map_group_proto: answer_proto.ConceptMapGroup):
    return ConceptMapGroup(concept(concept_map_group_proto.owner), map(lambda cm: concept_map._of(cm), concept_map_group_proto.concept_maps))
