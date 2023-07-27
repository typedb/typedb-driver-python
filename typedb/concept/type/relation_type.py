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

from typing import Iterator, Optional, Union

from typedb.api.concept.type.relation_type import RelationType
from typedb.api.connection.transaction import Transaction
# from typedb.common.label import Label
from typedb.common.transitivity import Transitivity
from typedb.concept.thing import relation
from typedb.concept.type.role_type import _RoleType
from typedb.concept.type.thing_type import _ThingType

from typedb.typedb_client_python import relation_type_create, relation_type_set_supertype, \
    relation_type_get_relates_for_role_label, relation_type_get_relates, relation_type_get_relates_overridden, \
    relation_type_set_relates, relation_type_unset_relates, relation_type_get_supertype, relation_type_get_supertypes, \
    relation_type_get_subtypes, relation_type_get_instances


class _RelationType(RelationType, _ThingType):

    # @staticmethod
    # def of(type_proto: concept_proto.Type):
    #     return _RelationType(Label.of(type_proto.label), type_proto.is_root, type_proto.is_abstract)

    # def as_relation_type(self) -> "RelationType":
    #     return self

    # def as_relation_type(self) -> "RemoteRelationType":
    #     return self

    def create(self, transaction: Transaction) -> relation._Relation:
        return relation._Relation(relation_type_create(self.native_transaction(transaction), self._concept))

    def get_instances(self, transaction: Transaction) -> Iterator[relation._Relation]:
        return (relation._Relation(item) for item in relation_type_get_instances(self.native_transaction(transaction),
                                                                        self._concept, Transitivity.Transitive))

    def get_instances_explicit(self, transaction: Transaction) -> Iterator[relation._Relation]:
        return (relation._Relation(item) for item in relation_type_get_instances(self.native_transaction(transaction),
                                                                        self._concept, Transitivity.Explicit))

    def get_relates(self, transaction: Transaction, role_label: Optional[str] = None) \
            -> Union[Optional[_RoleType], Iterator[_RoleType]]:
        if role_label:
            if res := relation_type_get_relates_for_role_label(self.native_transaction(transaction), self._concept, role_label):
                return _RoleType(res)
            return None
        return (_RoleType(item) for item in relation_type_get_relates(self.native_transaction(transaction),
                                                                      self._concept, Transitivity.Transitive))

    def get_relates_explicit(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (_RoleType(item) for item in relation_type_get_relates(self.native_transaction(transaction),
                                                                      self._concept, Transitivity.Explicit))

    def get_relates_overridden(self, transaction: Transaction, role_label: str) -> Optional[_RoleType]:
        if res := relation_type_get_relates_overridden(self.native_transaction(transaction), self._concept, role_label):
            return _RoleType(res)
        return None

    def set_relates(self, transaction: Transaction, role_label: str, overridden_label: Optional[str] = None) -> None:
        relation_type_set_relates(self.native_transaction(transaction), self._concept, role_label, overridden_label)

    def unset_relates(self, transaction: Transaction, role_label: str) -> None:
        relation_type_unset_relates(self.native_transaction(transaction), self._concept, role_label)

    def get_subtype(self, transaction: Transaction) -> Iterator[_RelationType]:
        pass

    def get_subtypes(self, transaction: Transaction) -> Iterator[_RelationType]:
        return (_RelationType(item) for item in relation_type_get_subtypes(self.native_transaction(transaction),
                                                                           self._concept, Transitivity.Transitive))

    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator[_RelationType]:
        return (_RelationType(item) for item in relation_type_get_subtypes(self.native_transaction(transaction),
                                                                           self._concept, Transitivity.Explicit))

    def get_supertype(self, transaction: Transaction) -> Optional[_RelationType]:
        if res := relation_type_get_supertype(self.native_transaction(transaction), self._concept):
            return _RelationType(res)
        return None

    def get_supertypes(self, transaction: Transaction) -> Iterator[_RelationType]:
        return (_RelationType(item) for item in relation_type_get_supertypes(self.native_transaction(transaction),
                                                                             self._concept))

    def set_supertype(self, transaction: Transaction, super_relation_type: RelationType) -> None:
        relation_type_set_supertype(self.native_transaction(transaction), self._concept, super_relation_type.native_object())
