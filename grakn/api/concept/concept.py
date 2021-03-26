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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from grakn.api.transaction import GraknTransaction


class Concept(ABC):

    def is_type(self) -> bool:
        return False

    def is_thing_type(self) -> bool:
        return False

    def is_entity_type(self) -> bool:
        return False

    def is_attribute_type(self) -> bool:
        return False

    def is_relation_type(self) -> bool:
        return False

    def is_role_type(self) -> bool:
        return False

    def is_thing(self) -> bool:
        return False

    def is_entity(self) -> bool:
        return False

    def is_attribute(self) -> bool:
        return False

    def is_relation(self) -> bool:
        return False

    @abstractmethod
    def as_remote(self, transaction: "GraknTransaction") -> "RemoteConcept":
        pass

    @abstractmethod
    def is_remote(self) -> bool:
        pass


class RemoteConcept(Concept, ABC):

    @abstractmethod
    def delete(self) -> None:
        pass

    @abstractmethod
    def is_deleted(self) -> bool:
        pass
