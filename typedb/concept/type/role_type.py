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
from typing import Iterator, Optional

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.type.role_type import RoleType
from typedb.api.connection.transaction import Transaction
from typedb.common.label import Label
from typedb.common.rpc.request_builder import role_type_get_relation_types_req, role_type_get_player_types_req, \
    role_type_get_player_types_explicit_req, role_type_get_relation_instances_req, \
    role_type_get_relation_instances_explicit_req, role_type_get_player_instances_req, \
    role_type_get_player_instances_explicit_req
from typedb.concept.concept import Transitivity
from typedb.concept.proto import concept_proto_reader
from typedb.concept.thing.relation import _Relation
from typedb.concept.thing.thing import _Thing
from typedb.concept.type.relation_type import _RelationType
from typedb.concept.type.thing_type import _ThingType
from typedb.concept.type.type import _Type

from typedb.typedb_client_python import role_type_is_root, role_type_is_abstract, role_type_get_scope, \
    role_type_get_name, role_type_delete, role_type_is_deleted, role_type_set_label, role_type_get_supertype, \
    role_type_get_supertypes, role_type_get_subtypes, role_type_get_relation_instances, role_type_get_player_instances, \
    role_type_get_relation_type, role_type_get_relation_types, role_type_get_player_types


class _RoleType(_Type, RoleType):

    def is_root(self) -> bool:
        return role_type_is_root(self._concept)

    def is_abstract(self) -> bool:
        return role_type_is_abstract(self._concept)

    def get_label(self) -> Label:
        return Label.of(role_type_get_scope(self._concept), role_type_get_name(self._concept))

    # @staticmethod
    # def of(type_proto: concept_proto.Type):
    #     return _RoleType(Label.of(type_proto.scope, type_proto.label), type_proto.is_root, type_proto.is_abstract)

    # def as_role_type(self) -> "RoleType":
    #     return self

    def delete(self, transaction: Transaction) -> None:
        role_type_delete(self.native_transaction(transaction), self._concept)

    def is_deleted(self, transaction: Transaction) -> bool:
        return role_type_is_deleted(self.native_transaction(transaction), self._concept)

    def set_label(self, transaction: Transaction, new_label: Label) -> None:
        role_type_set_label(self.native_transaction(transaction), self._concept, new_label)

    def get_supertype(self, transaction: Transaction) -> Optional["_RoleType"]:
        if res := role_type_get_supertype(self.native_transaction(transaction), self._concept):
            return _RoleType(res)
        return None

    def get_supertypes(self, transaction: Transaction) -> Iterator["_RoleType"]:
        return (_RoleType(item) for item in
                role_type_get_supertypes(self.native_transaction(transaction), self._concept))

    def get_subtypes(self, transaction: Transaction) -> Iterator["_RoleType"]:
        return (_RoleType(item) for item in
                role_type_get_subtypes(self.native_transaction(transaction), self._concept, Transitivity.Transitive))

    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator["_RoleType"]:
        return (_RoleType(item) for item in
                role_type_get_subtypes(self.native_transaction(transaction), self._concept, Transitivity.Explicit))

    def get_relation_type(self, transaction: Transaction) -> "_RelationType":
        return _RelationType(role_type_get_relation_type(self.native_transaction(transaction), self._concept))

    def get_relation_types(self, transaction: Transaction) -> Iterator["_RelationType"]:
        return (_RelationType(item) for item in
                role_type_get_relation_types(self.native_transaction(transaction), self._concept))

    def get_player_types(self, transaction: Transaction) -> Iterator["_ThingType"]:
        return (_ThingType(item) for item in role_type_get_player_types(self.native_transaction(transaction),
                                                                        self._concept, Transitivity.Transitive))

    def get_player_types_explicit(self, transaction: Transaction) -> Iterator["_ThingType"]:
        return (_ThingType(item) for item in role_type_get_player_types(self.native_transaction(transaction),
                                                                        self._concept, Transitivity.Explicit))

    def get_relation_instances(self, transaction: Transaction) -> Iterator["_Relation"]:
        return (_Relation(item) for item in role_type_get_relation_instances(self.native_transaction(transaction),
                                                                             self._concept, Transitivity.Transitive))

    def get_relation_instances_explicit(self, transaction: Transaction) -> Iterator["_Relation"]:
        return (_Relation(item) for item in role_type_get_relation_instances(self.native_transaction(transaction),
                                                                             self._concept, Transitivity.Explicit))

    def get_player_instances(self, transaction: Transaction) -> Iterator["Thing"]:
        return (_Thing(item) for item in role_type_get_player_instances(self.native_transaction(transaction),
                                                                           self._concept, Transitivity.Transitive))

    def get_player_instances_explicit(self, transaction: Transaction) -> Iterator["Thing"]:
        return (_Thing(item) for item in role_type_get_player_instances(self.native_transaction(transaction),
                                                                        self._concept, Transitivity.Explicit))
