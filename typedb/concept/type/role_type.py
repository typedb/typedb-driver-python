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

from typedb.api.concept.type.role_type import RoleType
from typedb.api.connection.transaction import Transaction
from typedb.common.label import Label
from typedb.common.streamer import Streamer
from typedb.common.transitivity import Transitivity
from typedb.concept.thing import relation
from typedb.concept.thing.thing import _Thing
from typedb.concept.type import relation_type
from typedb.concept.type.thing_type import _ThingType
from typedb.concept.type.type import _Type

from typedb.typedb_client_python import role_type_is_root, role_type_is_abstract, role_type_get_scope, \
    role_type_get_name, role_type_delete, role_type_is_deleted, role_type_set_label, role_type_get_supertype, \
    role_type_get_supertypes, role_type_get_subtypes, role_type_get_relation_instances, role_type_get_player_instances, \
    role_type_get_relation_type, role_type_get_relation_types, role_type_get_player_types, concept_iterator_next

if TYPE_CHECKING:
    pass


class _RoleType(_Type, RoleType):

    def is_root(self) -> bool:
        return role_type_is_root(self.native_object)

    def is_abstract(self) -> bool:
        return role_type_is_abstract(self.native_object)

    def get_label(self) -> Label:
        return Label.of(role_type_get_scope(self.native_object), role_type_get_name(self.native_object))

    def delete(self, transaction: Transaction) -> None:
        role_type_delete(transaction.native_object, self.native_object)

    def is_deleted(self, transaction: Transaction) -> bool:
        return role_type_is_deleted(transaction.native_object, self.native_object)

    def set_label(self, transaction: Transaction, new_label: Label) -> None:
        role_type_set_label(transaction.native_object, self.native_object, new_label)

    def get_supertype(self, transaction: Transaction) -> Optional[_RoleType]:
        if res := role_type_get_supertype(transaction.native_object, self.native_object):
            return _RoleType(res)
        return None

    def get_supertypes(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (_RoleType(item) for item in
                Streamer(role_type_get_supertypes(transaction.native_object, self.native_object), concept_iterator_next))

    def get_subtypes(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (_RoleType(item) for item in
                Streamer(role_type_get_subtypes(transaction.native_object, self.native_object, Transitivity.TRANSITIVE.value), concept_iterator_next))

    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (_RoleType(item) for item in
                Streamer(role_type_get_subtypes(transaction.native_object, self.native_object, Transitivity.Explicit.value), concept_iterator_next))

    def get_relation_type(self, transaction: Transaction) -> relation_type._RelationType:
        return relation_type._RelationType(role_type_get_relation_type(transaction.native_object, self.native_object))

    def get_relation_types(self, transaction: Transaction) -> Iterator[relation_type._RelationType]:
        return (relation_type._RelationType(item) for item in
                Streamer(role_type_get_relation_types(transaction.native_object, self.native_object), concept_iterator_next))

    def get_player_types(self, transaction: Transaction) -> Iterator[_ThingType]:
        return (_ThingType.of(item) for item in Streamer(role_type_get_player_types(transaction.native_object,
                                                                                    self.native_object, Transitivity.TRANSITIVE.value), concept_iterator_next))

    def get_player_types_explicit(self, transaction: Transaction) -> Iterator[_ThingType]:
        return (_ThingType.of(item) for item in Streamer(role_type_get_player_types(transaction.native_object,
                                                                                    self.native_object, Transitivity.Explicit.value), concept_iterator_next))

    def get_relation_instances(self, transaction: Transaction) -> Iterator[relation._Relation]:
        return (relation._Relation(item) for item in Streamer(role_type_get_relation_instances(transaction.native_object,
                                                                                               self.native_object, Transitivity.TRANSITIVE.value), concept_iterator_next))

    def get_relation_instances_explicit(self, transaction: Transaction) -> Iterator[relation._Relation]:
        return (relation._Relation(item) for item in Streamer(role_type_get_relation_instances(transaction.native_object,
                                                                                               self.native_object, Transitivity.Explicit.value), concept_iterator_next))

    def get_player_instances(self, transaction: Transaction) -> Iterator[_Thing]:
        return (_Thing(item) for item in Streamer(role_type_get_player_instances(transaction.native_object,
                                                                                 self.native_object, Transitivity.TRANSITIVE.value), concept_iterator_next))

    def get_player_instances_explicit(self, transaction: Transaction) -> Iterator[_Thing]:
        return (_Thing(item) for item in Streamer(role_type_get_player_instances(transaction.native_object,
                                                                                 self.native_object, Transitivity.Explicit.value), concept_iterator_next))
