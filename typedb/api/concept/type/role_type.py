#
# Copyright (C) 2021 Vaticle
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
from typing import TYPE_CHECKING, Iterator

from typedb.api.concept.type.type import Type, RemoteType

if TYPE_CHECKING:
    from typedb.api.concept.type.relation_type import RelationType
    from typedb.api.concept.type.thing_type import ThingType
    from typedb.api.connection.transaction import TypeDBTransaction


class RoleType(Type, ABC):

    def is_role_type(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteRoleType":
        pass


class RemoteRoleType(RemoteType, RoleType, ABC):

    @abstractmethod
    def get_supertype(self) -> RoleType:
        pass

    @abstractmethod
    def get_supertypes(self) -> Iterator[RoleType]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[RoleType]:
        pass

    @abstractmethod
    def get_relation_type(self) -> "RelationType":
        pass

    @abstractmethod
    def get_relation_types(self) -> Iterator["RelationType"]:
        pass

    @abstractmethod
    def get_players(self) -> Iterator["ThingType"]:
        pass
