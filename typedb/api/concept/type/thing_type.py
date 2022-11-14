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
from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Iterator, Optional

from typedb.api.concept.thing.thing import Thing
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.type import Type, RemoteType

if TYPE_CHECKING:
    from typedb.api.concept.type.attribute_type import AttributeType
    from typedb.api.connection.transaction import TypeDBTransaction


class ThingType(Type, ABC):

    def is_thing_type(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteThingType":
        pass


class RemoteThingType(RemoteType, ThingType, ABC):

    @abstractmethod
    def get_supertype(self) -> ThingType:
        pass

    @abstractmethod
    def get_supertypes(self) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["Thing"]:
        pass

    @abstractmethod
    def set_abstract(self) -> None:
        pass

    @abstractmethod
    def unset_abstract(self) -> None:
        pass

    @abstractmethod
    def set_plays(self, role_type: RoleType, overridden_type: RoleType = None) -> None:
        pass

    @abstractmethod
    def set_owns(self, attribute_type: "AttributeType", overridden_type: "AttributeType" = None, is_key: bool = False) -> None:
        pass

    @abstractmethod
    def get_plays(self) -> Iterator["RoleType"]:
        pass

    @abstractmethod
    def get_plays_explicit(self) -> Iterator["RoleType"]:
        pass

    @abstractmethod
    def get_plays_overridden(self, role_type: "RoleType") -> Optional["RoleType"]:
        pass

    @abstractmethod
    def get_owns(self, value_type: "AttributeType.ValueType" = None, keys_only: bool = False) -> Iterator["AttributeType"]:
        pass

    @abstractmethod
    def get_owns_explicit(self, value_type: "AttributeType.ValueType" = None, keys_only: bool = False) -> Iterator["AttributeType"]:
        pass

    @abstractmethod
    def get_owns_overridden(self, attribute_type: "AttributeType") -> Optional["AttributeType"]:
        pass

    @abstractmethod
    def unset_plays(self, role_type: "RoleType") -> None:
        pass

    @abstractmethod
    def unset_owns(self, attribute_type: "AttributeType") -> None:
        pass

    @abstractmethod
    def get_syntax(self) -> str:
        pass
