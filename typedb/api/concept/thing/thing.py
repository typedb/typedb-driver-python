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
from typing import List, TYPE_CHECKING, Iterator

from typedb.api.concept.concept import Concept, RemoteConcept

if TYPE_CHECKING:
    from typedb.api.concept.thing.attribute import Attribute
    from typedb.api.concept.type.attribute_type import AttributeType
    from typedb.api.concept.type.role_type import RoleType
    from typedb.api.concept.type.thing_type import ThingType
    from typedb.api.connection.transaction import TypeDBTransaction


class Thing(Concept, ABC):

    @abstractmethod
    def get_iid(self) -> str:
        pass

    @abstractmethod
    def get_type(self) -> "ThingType":
        pass

    @abstractmethod
    def is_inferred(self) -> bool:
        pass

    def is_thing(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteThing":
        pass


class RemoteThing(RemoteConcept, Thing, ABC):

    @abstractmethod
    def set_has(self, attribute: "Attribute") -> None:
        pass

    @abstractmethod
    def unset_has(self, attribute: "Attribute") -> None:
        pass

    @abstractmethod
    def get_has(self, attribute_type: "AttributeType" = None, attribute_types: List["AttributeType"] = None, only_key: bool = False) -> Iterator["Attribute"]:
        pass

    @abstractmethod
    def get_relations(self, role_types: List["RoleType"] = None):
        pass

    @abstractmethod
    def get_playing(self) -> Iterator["RoleType"]:
        pass
