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
from typing import TYPE_CHECKING, Iterator, Union

from typedb.api.concept.thing.relation import Relation
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.thing_type import ThingType, RemoteThingType

if TYPE_CHECKING:
    from typedb.api.connection.transaction import TypeDBTransaction


class RelationType(ThingType, ABC):

    def is_relation_type(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteRelationType":
        pass


class RemoteRelationType(RemoteThingType, RelationType, ABC):

    @abstractmethod
    def create(self) -> "Relation":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["Relation"]:
        pass

    @abstractmethod
    def get_relates(self, role_label: str = None) -> Union[RoleType, Iterator[RoleType]]:
        pass

    @abstractmethod
    def set_relates(self, role_label: str, overridden_label: str = None) -> None:
        pass

    @abstractmethod
    def unset_relates(self, role_label: str) -> None:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[RelationType]:
        pass

    @abstractmethod
    def set_supertype(self, relation_type: RelationType) -> None:
        pass
