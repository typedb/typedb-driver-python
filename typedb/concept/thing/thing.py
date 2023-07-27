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
from abc import ABC
from typing import Union, TYPE_CHECKING, Iterator

from typedb.api.concept.thing.attribute import Attribute
from typedb.api.concept.thing.thing import Thing
from typedb.api.concept.type.attribute_type import AttributeType
from typedb.api.concept.type.role_type import RoleType
from typedb.common.exception import TypeDBClientException, MISSING_IID, MISSING_TRANSACTION, \
    GET_HAS_WITH_MULTIPLE_FILTERS
from typedb.concept.concept import _Concept
import typedb.concept.thing as thing
# from typedb.concept.thing.attribute import _Attribute
# from typedb.concept.thing.relation import _Relation
# from typedb.concept.type.role_type import _RoleType

from typedb.api.concept.type.thing_type import Annotation

if TYPE_CHECKING:
    from typedb.api.connection.transaction import Transaction
    from typedb.concept.thing.attribute import _Attribute
    from typedb.concept.thing.relation import _Relation
    from typedb.concept.type.role_type import _RoleType

from typedb.typedb_client_python import thing_get_iid, thing_get_is_inferred, thing_get_has, \
    Annotation as NativeAnnotation, Concept, thing_get_relations, thing_get_playing, thing_set_has, thing_unset_has, \
    thing_delete, thing_is_deleted


class _Thing(Thing, _Concept, ABC):

    def get_iid(self) -> str:
        return thing_get_iid(self._concept)

    def is_inferred(self) -> bool:
        return thing_get_is_inferred(self._concept)

    # def as_thing(self) -> Thing:
    #     return self

    def get_has(self, transaction: Transaction, *, attribute_type=None, attribute_types: list[AttributeType] = (), annotations: set[Annotation] = frozenset()) -> Iterator[_Attribute]:
        if [bool(attribute_type), bool(attribute_types), bool(annotations)].count(True) > 1:
            raise TypeDBClientException.of(GET_HAS_WITH_MULTIPLE_FILTERS)
        if attribute_type:
            attribute_types = [attribute_type]
        native_annotations = [NativeAnnotation(anno.native_object()) for anno in annotations]
        return (thing.attribute._Attribute(item) for item in thing_get_has(self.native_transaction(transaction), self._concept,
                                                           attribute_types, native_annotations))

    def get_relations(self, transaction: Transaction, *role_types: RoleType) -> Iterator[_Relation]:
        native_role_types = [Concept(rt.native_object()) for rt in role_types]
        return (thing.relation._Relation(item) for item in thing_get_relations(self.native_transaction(transaction), self._concept,
                                                                 native_role_types))

    def get_playing(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (thing.role_type._RoleType(rt) for rt in thing_get_playing(self.native_transaction(transaction), self._concept))

    def set_has(self, transaction: Transaction, attribute: Attribute) -> None:
        thing_set_has(self.native_transaction(transaction), self._concept, attribute.native_object())

    def unset_has(self, transaction: Transaction, attribute: Attribute) -> None:
        thing_unset_has(self.native_transaction(transaction), self._concept, attribute.native_object())

    def delete(self, transaction: Transaction) -> None:
        thing_delete(self.native_transaction(transaction), self._concept)

    def is_deleted(self, transaction: Transaction) -> bool:
        thing_is_deleted(self.native_transaction(transaction), self._concept)

    def __str__(self):
        return "%s[%s:%s]" % (type(self).__name__, self.get_type().get_label(), self.get_iid())

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_iid() == other.get_iid()

    def __hash__(self):
        return hash(self.get_iid())

