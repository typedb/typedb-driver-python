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
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from typedb.api.connection.transaction import TypeDBTransaction


class Rule(ABC):

    @abstractmethod
    def get_label(self) -> str:
        pass

    @abstractmethod
    def get_when(self) -> str:
        pass

    @abstractmethod
    def get_then(self) -> str:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteRule":
        pass

    @abstractmethod
    def is_remote(self) -> bool:
        pass


class RemoteRule(Rule, ABC):

    @abstractmethod
    def set_label(self, label: str) -> None:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass

    @abstractmethod
    def is_deleted(self) -> bool:
        pass
