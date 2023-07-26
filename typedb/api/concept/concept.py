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
from datetime import datetime
import enum
from abc import ABC, abstractmethod
from typing import Mapping, Union, TYPE_CHECKING

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.value.value import Value
from typedb.common.exception import TypeDBClientException, INVALID_CONCEPT_CASTING

if TYPE_CHECKING:
    from typedb.api.concept.thing.attribute import Attribute
    from typedb.api.concept.thing.entity import Entity
    from typedb.api.concept.thing.relation import Relation
    from typedb.api.concept.thing.thing import Thing
    from typedb.api.concept.type.attribute_type import AttributeType
    from typedb.api.concept.type.entity_type import EntityType
    from typedb.api.concept.type.relation_type import RelationType
    from typedb.api.concept.type.role_type import RoleType
    from typedb.api.concept.type.thing_type import ThingType
    from typedb.api.concept.type.type import Type
    from typedb.api.connection.transaction import Transaction


class Concept(ABC):

    def is_type(self) -> bool:
        return False

    def is_thing_type(self) -> bool:
        return False

    def is_entity_type(self) -> bool:
        return False

    def is_attribute_type(self) -> bool:
        return False

    def is_relation_type(self) -> bool:
        return False

    def is_role_type(self) -> bool:
        return False

    def is_thing(self) -> bool:
        return False

    def is_entity(self) -> bool:
        return False

    def is_attribute(self) -> bool:
        return False

    def is_relation(self) -> bool:
        return False

    def is_value(self) -> bool:
        return False

    def as_type(self) -> Type:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Type))

    def as_thing_type(self) -> ThingType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, ThingType))

    def as_entity_type(self) -> EntityType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, EntityType))

    def as_attribute_type(self) -> AttributeType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, AttributeType))

    def as_relation_type(self) -> RelationType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, RelationType))

    def as_role_type(self) -> RoleType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, RoleType))

    def as_thing(self) -> Thing:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Thing))

    def as_entity(self) -> Entity:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Entity))

    def as_attribute(self) -> Attribute:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Attribute))

    def as_relation(self) -> Relation:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Relation))

    def as_value(self) -> Value:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, Value))

    @abstractmethod
    def to_json(self) -> Mapping[str, Union[str, int, float, bool, datetime]]:
        pass


# class ValueType(enum.Enum):
#     OBJECT = 0
#     BOOLEAN = 1
#     LONG = 2
#     DOUBLE = 3
#     STRING = 4
#     DATETIME = 5
#
#     def proto(self) -> concept_proto.ValueType:
#         return concept_proto.ValueType.Value(self.name)
#
#     def __str__(self):
#         return self.name.lower()


# class RemoteConcept(Concept, ABC):
#
#     @abstractmethod
#     def delete(self) -> None:
#         pass
#
#     @abstractmethod
#     def is_deleted(self) -> bool:
#         pass
#
#     def as_type(self) -> "RemoteType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "Type"))
#
#     def as_thing_type(self) -> "RemoteThingType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "ThingType"))
#
#     def as_entity_type(self) -> "RemoteEntityType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "EntityType"))
#
#     def as_attribute_type(self) -> "RemoteAttributeType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "AttributeType"))
#
#     def as_relation_type(self) -> "RemoteRelationType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "RelationType"))
#
#     def as_role_type(self) -> "RemoteRoleType":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "RoleType"))
#
#     def as_thing(self) -> "RemoteThing":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "Thing"))
#
#     def as_entity(self) -> "RemoteEntity":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "Entity"))
#
#     def as_attribute(self) -> "RemoteAttribute":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "Attribute"))
#
#     def as_relation(self) -> "RemoteRelation":
#         raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, "Relation"))
