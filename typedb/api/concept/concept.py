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
from abc import ABC, abstractmethod
from typing import Mapping, Union

from typedb.common.exception import TypeDBClientException, INVALID_CONCEPT_CASTING
from typedb.api.concept import thing, type, value


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

    def as_type(self) -> type.type.Type:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, type.type.Type))

    def as_thing_type(self) -> type.thing_type.ThingType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, type.thing_type.ThingType))

    def as_entity_type(self) -> type.entity_type.EntityType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, type.entity_type.EntityType))

    def as_attribute_type(self) -> type.attribute_type.AttributeType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__,
                                                                 type.attribute_type.AttributeType))

    def as_relation_type(self) -> type.relation_type.RelationType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__,
                                                                 type.relation_type.RelationType))

    def as_role_type(self) -> type.role_type.RoleType:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, type.role_type.RoleType))

    def as_thing(self) -> thing.thing.Thing:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, thing.thing.Thing))

    def as_entity(self) -> thing.entity.Entity:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, thing.entity.Entity))

    def as_attribute(self) -> thing.attribute.Attribute:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, thing.attribute.Attribute))

    def as_relation(self) -> thing.relation.Relation:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, thing.relation.Relation))

    def as_value(self) -> value.value.Value:
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, value.value.Value))

    @abstractmethod
    def to_json(self) -> Mapping[str, Union[str, int, float, bool, datetime]]:
        pass
