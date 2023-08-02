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
from abc import abstractmethod, ABC
from itertools import chain
from typing import Optional, Iterator, TYPE_CHECKING

from typedb.api.concept.type.attribute_type import AttributeType
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.thing_type import ThingType, Annotation
from typedb.api.concept.value.value import ValueType
from typedb.common.exception import TypeDBClientException, UNEXPECTED_NATIVE_VALUE
from typedb.common.label import Label
from typedb.common.streamer import Streamer
from typedb.common.transitivity import Transitivity
from typedb.concept.thing.thing import _Thing
# from typedb.concept.type.attribute_type import _AttributeType
# from typedb.concept.type.entity_type import _EntityType
# from typedb.concept.type.relation_type import _RelationType
# from typedb.concept.type.role_type import _RoleType
from typedb.concept.type.type import _Type

from typedb.typedb_client_python import Concept as NativeConcept, concept_is_entity_type, concept_is_relation_type, \
    concept_is_attribute_type, concept_is_root_thing_type, thing_type_is_root, thing_type_is_abstract, \
    thing_type_get_label, thing_type_delete, thing_type_is_deleted, thing_type_set_label, thing_type_set_abstract, \
    thing_type_unset_abstract, thing_type_set_plays, thing_type_unset_plays, thing_type_set_owns, thing_type_get_owns, \
    thing_type_get_plays, thing_type_get_owns_overridden, thing_type_unset_owns, thing_type_get_syntax, \
    thing_type_get_plays_overridden, concept_iterator_next

if TYPE_CHECKING:
    from typedb.api.connection.transaction import Transaction
    # from typedb.connection.transaction import _Transaction


class _ThingType(ThingType, _Type, ABC):

    # def __init__(self, concept: NativeConcept):
    #     super(_Type, self).__init__(concept)

    @staticmethod
    def of(concept: NativeConcept):
        from typedb.concept.type import attribute_type, entity_type, relation_type

        if concept_is_entity_type(concept):
            return entity_type._EntityType(concept)
        elif concept_is_relation_type(concept):
            return relation_type._RelationType(concept)
        elif concept_is_attribute_type(concept):
            return attribute_type._AttributeType(concept)
        elif concept_is_root_thing_type(concept):
            return _Root(concept)
        raise TypeDBClientException.of(UNEXPECTED_NATIVE_VALUE)

    # def as_remote(self, transaction):
    #     return _RemoteThingType(transaction, self.get_label(), self.is_root(), self.is_abstract())

    def as_thing_type(self) -> ThingType:
        return self

    def is_root(self) -> bool:
        return thing_type_is_root(self._concept)

    def is_abstract(self) -> bool:
        return thing_type_is_abstract(self._concept)

    def get_label(self) -> Label:
        return Label.of(thing_type_get_label(self._concept))

    def delete(self, transaction: Transaction) -> None:
        thing_type_delete(self.native_transaction(transaction), self._concept)

    def is_deleted(self, transaction: Transaction) -> bool:
        return thing_type_is_deleted(transaction.concepts().native_transaction(), self._concept)

    def set_label(self, transaction: Transaction, new_label: Label) -> None:
        thing_type_set_label(transaction.concepts().native_transaction(), self._concept, new_label)

    @abstractmethod
    def get_instances(self, transaction: Transaction):
        pass

    @abstractmethod
    def get_instances_explicit(self, transaction: Transaction):
        pass

    def set_abstract(self, transaction: Transaction) -> None:
        thing_type_set_abstract(transaction.concepts().native_transaction(), self._concept)

    def unset_abstract(self, transaction: Transaction) -> None:
        thing_type_unset_abstract(transaction.concepts().native_transaction(), self._concept)

    def set_plays(self, transaction: Transaction,
                  role_type: RoleType, overridden_role_type: Optional[RoleType] = None) -> None:
        thing_type_set_plays(transaction.concepts().native_transaction(),
                             self._concept, role_type.native_object(),
                             overridden_role_type.native_object() if overridden_role_type else None)

    def unset_plays(self, transaction: Transaction, role_type: RoleType) -> None:
        thing_type_unset_plays(transaction.concepts().native_transaction(), self._concept, role_type.native_object())

    def set_owns(self, transaction: Transaction, attribute_type: AttributeType,
                 overridden_type: Optional[AttributeType] = None, annotations: Optional[set[Annotation]] = None) -> None:
        overridden_type_native = overridden_type.native_object() if overridden_type else None
        annotations_array = [anno.native_object() for anno in annotations] if annotations else []
        thing_type_set_owns(
            self.native_transaction(transaction),
            self._concept,
            attribute_type.native_object(),
            overridden_type_native,
            annotations_array,
        )

    def get_owns(self, transaction: Transaction, value_type: Optional[ValueType] = None,
                 transitivity: Transitivity = Transitivity.Transitive, annotations: Optional[set[Annotation]] = None
                 ) -> Iterator[AttributeType]:
        from typedb.concept.type import attribute_type

        return (attribute_type._AttributeType(item) for item in
                Streamer(thing_type_get_owns(
                    self.native_transaction(transaction),
                    self._concept,
                    value_type.native_object() if value_type else None,
                    transitivity.value,
                    [anno.native_object() for anno in annotations] if annotations else []
                ), concept_iterator_next))

    def get_owns_explicit(self, transaction: Transaction, value_type: Optional[ValueType] = None,
                          annotations: Optional[set[Annotation]] = None):
        return self.get_owns(transaction, value_type, Transitivity.Explicit, annotations)

    def get_plays(self, transaction: Transaction, transitivity: Transitivity = Transitivity.Transitive) -> Iterator[RoleType]:
        from typedb.concept.type import role_type

        return (role_type._RoleType(item) for item in
                Streamer(thing_type_get_plays(
                    self.native_transaction(transaction),
                    self._concept,
                    transitivity.value
                ), concept_iterator_next)
        )

    def get_plays_explicit(self, transaction: Transaction) -> Iterator[RoleType]:
        return self.get_plays(transaction, Transitivity.Explicit)

    def get_plays_overridden(self, transaction: Transaction, role_type: RoleType) -> Optional[RoleType]:
        from typedb.concept.type.role_type import _RoleType

        if res := thing_type_get_plays_overridden(self.native_transaction(transaction),
                self._concept, role_type.native_object()):
            return _RoleType(res)
        return None

    def get_owns_overridden(self, transaction: Transaction, attribute_type: AttributeType) -> Optional[AttributeType]:
        from typedb.concept.type.attribute_type import _AttributeType

        if res := thing_type_get_owns_overridden(self.native_transaction(transaction),
                                                 self._concept, attribute_type.native_object()):
            return _AttributeType(res)
        return None

    def unset_owns(self, transaction: Transaction, attribute_type: AttributeType) -> None:
        thing_type_unset_owns(self.native_transaction(transaction),
                              self._concept, attribute_type.native_object())

    def get_syntax(self, transaction: Transaction) -> str:
        return thing_type_get_syntax(self.native_transaction(transaction), self._concept,)


class _Root(_ThingType):

    ROOT_LABEL = Label.of("thing")

    def get_label(self) -> Label:
        return self.ROOT_LABEL

    def get_supertype(self, transaction: Transaction) -> Optional[_ThingType]:
        return None

    def get_supertypes(self, transaction: Transaction) -> Iterator[_ThingType]:
        return self,

    def get_subtypes(self, transaction: Transaction) -> Iterator[_ThingType]:
        return (_ThingType.of(item) for item in chain(
            (self, ), transaction.concepts().get_root_entity_type().get_subtypes(transaction),
            transaction.concepts().get_root_relation_type().get_subtypes(transaction),
            transaction.concepts().get_root_attribute_type().get_subtypes(transaction)))

    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator[_ThingType]:
        return (_ThingType.of(item) for item in (transaction.concepts().get_root_entity_type(),
                                              transaction.concepts().get_root_relation_type(),
                                              transaction.concepts().get_root_attribute_type()))

    def get_instances(self, transaction: Transaction) -> Iterator[_Thing]:
        return (_Thing(item) for item in chain(
            (self, ), transaction.concepts().get_root_entity_type().get_instances(transaction),
            transaction.concepts().get_root_relation_type().get_instances(transaction),
            transaction.concepts().get_root_attribute_type().get_instances(transaction)))

    def get_instances_explicit(self, transaction: Transaction) -> Iterator[_Thing]:
        return ()

# class _RemoteThingType(_RemoteType, RemoteThingType):
#
#     def as_remote(self, transaction):
#         return _RemoteThingType(transaction, self.get_label(), self.is_root(), self.is_abstract())
#
#     def as_thing_type(self) -> "RemoteThingType":
#         return self
#
#     def is_deleted(self) -> bool:
#         return not self._transaction_ext.concepts().get_thing_type(self.get_label().name())
#
#     def set_supertype(self, thing_type: ThingType):
#         self.execute(thing_type_set_supertype_req(self.get_label(), concept_proto_builder.thing_type(thing_type)))
#
#     def get_instances(self):
#         return (concept_proto_reader.thing(t) for rp in self.stream(thing_type_get_instances_req(self.get_label()))
#                 for t in rp.thing_type_get_instances_res_part.things)
#
#     def set_abstract(self):
#         self.execute(thing_type_set_abstract_req(self.get_label()))
#
#     def unset_abstract(self):
#         self.execute(thing_type_unset_abstract_req(self.get_label()))
#
#     def set_plays(self, role_type: RoleType, overridden_role_type: RoleType = None):
#         self.execute(thing_type_set_plays_req(self.get_label(), concept_proto_builder.role_type(role_type),
#                                               concept_proto_builder.role_type(overridden_role_type)))
#
#     def unset_plays(self, role_type: RoleType):
#         self.execute(thing_type_unset_plays_req(self.get_label(), concept_proto_builder.role_type(role_type)))
#
#     def get_plays(self):
#         return (concept_proto_reader.type_(t) for rp in self.stream(thing_type_get_plays_req(self.get_label()))
#                 for t in rp.thing_type_get_plays_res_part.role_types)
#
#     def get_plays_explicit(self):
#         return (concept_proto_reader.type_(t) for rp in self.stream(thing_type_get_plays_explicit_req(self.get_label()))
#                 for t in rp.thing_type_get_plays_explicit_res_part.role_types)
#
#     def get_plays_overridden(self, role_type: "RoleType"):
#         res = self.execute(thing_type_get_plays_overridden(
#             self.get_label(), concept_proto_builder.role_type(role_type)
#         )).thing_type_get_plays_overridden_res
#         return concept_proto_reader.type_(res.role_type) if res.HasField("role_type") else None
#
#     def set_owns(self, attribute_type: AttributeType, overridden_type: AttributeType = None,
#                  annotations: Set["Annotation"] = frozenset()):
#         self.execute(thing_type_set_owns_req(self.get_label(), concept_proto_builder.thing_type(attribute_type),
#                                              concept_proto_builder.thing_type(overridden_type),
#                                              [concept_proto_builder.annotation(a) for a in annotations]))
#
#     def unset_owns(self, attribute_type: AttributeType):
#         self.execute(thing_type_unset_owns_req(self.get_label(), concept_proto_builder.thing_type(attribute_type)))
#
#     def get_owns(self, value_type: ValueType = None, annotations: Set["Annotation"] = frozenset()):
#         return (concept_proto_reader.type_(t)
#                 for rp in self.stream(thing_type_get_owns_req(self.get_label(),
#                                                               value_type.proto() if value_type else None,
#                                                               [concept_proto_builder.annotation(a) for a in annotations]))
#                 for t in rp.thing_type_get_owns_res_part.attribute_types)
#
#     def get_owns_explicit(self, value_type: ValueType = None, annotations: Set["Annotation"] = frozenset()):
#         return (concept_proto_reader.type_(t)
#                 for rp in self.stream(thing_type_get_owns_explicit_req(self.get_label(),
#                                                                        value_type.proto() if value_type else None,
#                                                                        [concept_proto_builder.annotation(a) for a in annotations]))
#                 for t in rp.thing_type_get_owns_explicit_res_part.attribute_types)
#
#     def get_owns_overridden(self, attribute_type: "AttributeType"):
#         res = self.execute(thing_type_get_owns_overridden_req(
#             self.get_label(), concept_proto_builder.thing_type(attribute_type)
#         )).thing_type_get_owns_overridden_res
#         return concept_proto_reader.attribute_type(res.attribute_type) if res.HasField("attribute_type") else None
#
#     def get_syntax(self):
#         return self.execute(thing_type_get_syntax_req(self.get_label())).thing_type_get_syntax_res.syntax
