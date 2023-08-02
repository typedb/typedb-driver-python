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
from typing import TYPE_CHECKING, Iterator, Optional, Set

# from typedb.api.concept.concept import ValueType
from typedb.api.concept.thing.thing import Thing
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.type import Type
from typedb.api.concept.value.value import ValueType
from typedb.common.exception import TypeDBClientException, BAD_ANNOTATION
from typedb.common.transitivity import Transitivity

if TYPE_CHECKING:
    from typedb.api.concept.type.attribute_type import AttributeType
    from typedb.api.connection.transaction import TypeDBTransaction, Transaction

from typedb.typedb_client_python import Annotation as NativeAnnotation, annotation_new_key, annotation_new_unique, \
    annotation_is_key, annotation_is_unique, annotation_to_string, annotation_equals


class ThingType(Type, ABC):

    def is_thing_type(self) -> bool:
        return True

    @abstractmethod
    def get_supertype(self, transaction: Transaction) -> Optional[ThingType]:
        pass

    @abstractmethod
    def get_supertypes(self, transaction: Transaction) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def get_subtypes(self, transaction: Transaction) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def get_subtypes_explicit(self, transaction: Transaction) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def get_instances(self, transaction: Transaction) -> Iterator[Thing]:
        pass

    @abstractmethod
    def get_instances_explicit(self, transaction: Transaction) -> Iterator[Thing]:
        pass

    @abstractmethod
    def set_abstract(self, transaction: Transaction) -> None:
        pass

    @abstractmethod
    def unset_abstract(self, transaction: Transaction) -> None:
        pass

    @abstractmethod
    def set_plays(self, transaction: Transaction, role_type: RoleType,
                  overriden_type: Optional[RoleType] = None) -> None:
        pass

    @abstractmethod
    def unset_plays(self, transaction: Transaction, role_type: RoleType) -> None:
        pass

    @abstractmethod
    def set_owns(self, transaction: Transaction, attribute_type: AttributeType,
                 overridden_type: Optional[AttributeType] = None,
                 annotations: Optional[set[Annotation]] = None) -> None:
        pass

    @abstractmethod
    def get_owns(self, transaction: Transaction, value_type: Optional[ValueType] = None,
                 transitivity: Transitivity = Transitivity.Transitive, annotations: Optional[set[Annotation]] = None
                 ) -> Iterator[AttributeType]:
        pass

    @abstractmethod
    def get_owns_explicit(self, transaction: Transaction, value_type: Optional[ValueType] = None,
                          annotations: Optional[set[Annotation]] = None):
        pass

    @abstractmethod
    def get_plays(self, transaction: Transaction, transitivity: Transitivity = Transitivity.Transitive) -> Iterator[RoleType]:
        pass

    @abstractmethod
    def get_plays_explicit(self, transaction: Transaction) -> Iterator[RoleType]:
        pass

    @abstractmethod
    def get_plays_overridden(self, transaction: Transaction, role_type: RoleType) -> Optional[RoleType]:
        pass

    @abstractmethod
    def unset_owns(self, transaction: Transaction, attribute_type: AttributeType) -> None:
        pass

    @abstractmethod
    def get_syntax(self, transaction: Transaction) -> str:
        pass


class Annotation(object):

    def __init__(self, native_object: NativeAnnotation):
        self._object = native_object

    @abstractmethod
    def native_object(self):
        return self._object

    @staticmethod
    def key() -> Annotation:
        return Annotation(annotation_new_key())

    @staticmethod
    def unique() -> Annotation:
        return Annotation(annotation_new_unique())

    def is_key(self) -> bool:
        return annotation_is_key(self._object)

    def is_unique(self) -> bool:
        return annotation_is_unique(self._object)

    def name(self):
        return self._name

    @staticmethod
    def parse_annotation(annotation: str) -> Annotation:
        pass

    def __str__(self):
        return annotation_to_string(self._object)

    def __repr__(self):
        return f"Annotation({self._object})"

    def __hash__(self):
        return hash((self.is_key(), self.is_unique()))

    def __eq__(self, other):
        return isinstance(other, Annotation) and isinstance(self._object, NativeAnnotation) and \
            isinstance(other._object, NativeAnnotation) and annotation_equals(self._object, other._object)

# class Annotations(Annotation, Enum):
#     KEY = "key"
#     UNIQUE = "unique"
#
#     def __str__(self):
#         return "[annotation: " + self._name + "]"
#
#     @staticmethod
#     def parse_annotation(text) -> Annotation:
#         for annotation in Annotations:
#             if text == annotation.name():
#                 return annotation
#         raise TypeDBClientException.of(BAD_ANNOTATION, text)


# class RemoteThingType(RemoteType, ThingType, ABC):
#
#     @abstractmethod
#     def get_supertype(self) -> ThingType:
#         pass
#
#     @abstractmethod
#     def get_supertypes(self) -> Iterator[ThingType]:
#         pass
#
#     @abstractmethod
#     def get_subtypes(self) -> Iterator[ThingType]:
#         pass
#
#     @abstractmethod
#     def get_instances(self) -> Iterator["Thing"]:
#         pass
#
#     @abstractmethod
#     def set_abstract(self) -> None:
#         pass
#
#     @abstractmethod
#     def unset_abstract(self) -> None:
#         pass
#
#     @abstractmethod
#     def set_plays(self, role_type: RoleType, overridden_type: RoleType = None) -> None:
#         pass
#
#     @abstractmethod
#     def set_owns(self, attribute_type: "AttributeType", overridden_type: "AttributeType" = None,
#                  annotations: Set["Annotation"] = frozenset()) -> None:
#         pass
#
#     @abstractmethod
#     def get_plays(self) -> Iterator["RoleType"]:
#         pass
#
#     @abstractmethod
#     def get_plays_explicit(self) -> Iterator["RoleType"]:
#         pass
#
#     @abstractmethod
#     def get_plays_overridden(self, role_type: "RoleType") -> Optional["RoleType"]:
#         pass
#
#     @abstractmethod
#     def get_owns(self, value_type: "ValueType" = None,
#                  annotations: Set["Annotation"] = frozenset()) -> Iterator["AttributeType"]:
#         pass
#
#     @abstractmethod
#     def get_owns_explicit(self, value_type: "ValueType" = None,
#                           annotations: Set["Annotation"] = frozenset()) -> Iterator["AttributeType"]:
#         pass
#
#     @abstractmethod
#     def get_owns_overridden(self, attribute_type: "AttributeType") -> Optional["AttributeType"]:
#         pass
#
#     @abstractmethod
#     def unset_plays(self, role_type: "RoleType") -> None:
#         pass
#
#     @abstractmethod
#     def unset_owns(self, attribute_type: "AttributeType") -> None:
#         pass
#
#     @abstractmethod
#     def get_syntax(self) -> str:
#         pass
