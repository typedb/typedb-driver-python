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
from abc import ABC, abstractmethod
from enum import Enum

from typedb.api.concept.concept import Concept
from typedb.common.exception import TypeDBClientException, UNEXPECTED_NATIVE_VALUE
from typedb.concept.concept_manager import _ConceptManager
from typedb.concept.thing.attribute import _Attribute
from typedb.concept.thing.entity import _Entity
from typedb.concept.thing.relation import _Relation
from typedb.concept.type.attribute_type import _AttributeType
from typedb.concept.type.entity_type import _EntityType
from typedb.concept.type.relation_type import _RelationType
from typedb.concept.type.role_type import _RoleType
from typedb.concept.type.thing_type import _ThingType, _Root
from typedb.concept.value.value import _Value
from typedb.connection.transaction import _TransactionImpl

from typedb.typedb_client_python import Concept as NativeConcept, Transaction as NativeTransaction, Transitive, \
    Explicit, concept_is_entity_type, concept_is_relation_type, concept_is_attribute_type, concept_is_root_thing_type, \
    concept_is_entity, concept_is_relation, concept_is_attribute, concept_is_value, concept_is_role_type, \
    concept_to_string, concept_equals


class Transitivity(Enum):
    Transitive = Transitive
    Explicit = Explicit


class _Concept(Concept, ABC):

    def __init__(self, concept: NativeConcept):
        self._concept = concept

    @staticmethod
    def native_transaction(transaction: _TransactionImpl) -> NativeTransaction:
        return transaction.concepts().native_transaction()

    def native_object(self):
        return self._concept

    @staticmethod
    def of(concept: NativeConcept) -> _Concept:
        if concept_is_entity_type(concept):
            return _EntityType(concept)
        if concept_is_relation_type(concept):
            return _RelationType(concept)
        if concept_is_attribute_type(concept):
            return _AttributeType(concept)
        if concept_is_root_thing_type(concept):
            return _Root(concept)
        if concept_is_entity(concept):
            return _Entity(concept)
        if concept_is_relation(concept):
            return _Relation(concept)
        if concept_is_attribute(concept):
            return _Attribute(concept)
        if concept_is_value(concept):
            return _Value(concept)
        if concept_is_role_type(concept):
            return _RoleType(concept)
        raise TypeDBClientException(UNEXPECTED_NATIVE_VALUE)

    def __str__(self):
        return concept_to_string(self._concept)

    def __eq__(self, other):
        return other and isinstance(other, _Concept) and concept_equals(self._concept, other._concept)

    @abstractmethod
    def __hash__(self):
        pass


# def is_remote(self):
    #     return False


# class _RemoteConcept(RemoteConcept, ABC):
#
#     def is_remote(self):
#         return True
