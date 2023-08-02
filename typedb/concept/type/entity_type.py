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
from typing import Iterator, Optional, TYPE_CHECKING

from typedb.api.concept.type.entity_type import EntityType
from typedb.api.connection.transaction import Transaction
from typedb.common.label import Label
from typedb.common.streamer import Streamer
from typedb.common.transitivity import Transitivity
from typedb.concept.thing import entity
from typedb.concept.type import thing_type

from typedb.typedb_client_python import Concept, entity_type_create, entity_type_get_subtypes, \
    entity_type_get_instances, entity_type_get_supertypes, entity_type_get_supertype, entity_type_set_supertype, \
    concept_iterator_next

if TYPE_CHECKING:
    pass


class _EntityType(EntityType, thing_type._ThingType):

    # def __init__(self, concept: Concept):
    #     super(_ThingType).__init__(concept)

    # @staticmethod
    # def of(type_proto: concept_proto.Type):
    #     return _EntityType(Label.of(type_proto.label), type_proto.is_root, type_proto.is_abstract)

    # def as_remote(self, transaction):
    #     return _RemoteEntityType(transaction, self.get_label(), self.is_root(), self.is_abstract())

    # def as_entity_type(self) -> "EntityType":
    #     return self

    def create(self, transaction: Transaction) -> entity._Entity:
        return entity._Entity(entity_type_create(self.native_transaction(transaction), self._concept))

    def set_supertype(self, transaction: Transaction, super_entity_type: EntityType) -> None:
        entity_type_set_supertype(self.native_transaction(transaction), self._concept,
                                  super_entity_type.native_object())

    def get_supertype(self, transaction: Transaction) -> Optional[_EntityType]:
        if res := entity_type_get_supertype(self.native_transaction(transaction), self._concept):
            return _EntityType(res)
        return None

    def get_supertypes(self, transaction: Transaction) -> Iterator[_EntityType]:
        return (_EntityType(item) for item in
                Streamer(entity_type_get_supertypes(self.native_transaction(transaction), self._concept), concept_iterator_next))

    def get_subtypes(self, transaction: Transaction) -> Iterator[_EntityType]:
        return (_EntityType(item) for item in
                Streamer(entity_type_get_subtypes(self.native_transaction(transaction), self._concept, Transitivity.Transitive.value), concept_iterator_next))

    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator[_EntityType]:
        return (_EntityType(item) for item in
                Streamer(entity_type_get_subtypes(self.native_transaction(transaction), self._concept, Transitivity.Explicit.value), concept_iterator_next))

    def get_instances(self, transaction: Transaction) -> Iterator[entity._Entity]:
        return (entity._Entity(item) for item in
                Streamer(entity_type_get_instances(self.native_transaction(transaction), self._concept, Transitivity.Transitive.value), concept_iterator_next))

    def get_instances_explicit(self, transaction: Transaction) -> Iterator[entity._Entity]:
        return (entity._Entity(item) for item in
                Streamer(entity_type_get_instances(self.native_transaction(transaction), self._concept, Transitivity.Explicit.value), concept_iterator_next))

# class _RemoteEntityType(_RemoteThingType, RemoteEntityType):
#
#     def as_remote(self, transaction):
#         return _RemoteEntityType(transaction, self.get_label(), self.is_root(), self.is_abstract())
#
#     def as_entity_type(self) -> "RemoteEntityType":
#         return self
#
#     def create(self):
#         return _Entity.of(self.execute(entity_type_create_req(self.get_label())).entity_type_create_res.entity)
