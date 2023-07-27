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

from typedb.api.concept.thing.entity import Entity
from typedb.concept.thing.thing import _Thing
from typedb.concept.type import entity_type

from typedb.typedb_client_python import entity_get_type


class _Entity(Entity, _Thing):

    # def __init__(self, iid: str, is_inferred: bool, entity_type: EntityType):
    #     super(_Entity, self).__init__(iid, is_inferred)
    #     self._type = entity_type

    # @staticmethod
    # def of(thing_proto: concept_proto.Thing):
    #     return _Entity(concept_proto_reader.iid(thing_proto.iid), thing_proto.inferred, concept_proto_reader.type_(thing_proto.type))

    def get_type(self) -> entity_type._EntityType:
        return entity_type._EntityType(entity_get_type(self._concept))

    # def as_entity(self) -> "Entity":
    #     return self

    # def as_entity(self) -> Entity:
    #     return self
