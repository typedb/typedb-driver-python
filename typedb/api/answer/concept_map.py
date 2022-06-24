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
from typing import Mapping, Iterable, Tuple

from typedb.api.concept.concept import Concept


class ConceptMap(ABC):

    @abstractmethod
    def map(self) -> Mapping[str, Concept]:
        pass

    @abstractmethod
    def concepts(self) -> Iterable[Concept]:
        pass

    @abstractmethod
    def get(self, variable: str) -> Concept:
        pass

    @abstractmethod
    def explainables(self) -> "ConceptMap.Explainables":
        pass

    class Explainables(ABC):

        @abstractmethod
        def relation(self, variable: str) -> "ConceptMap.Explainable":
            pass

        @abstractmethod
        def attribute(self, variable: str) -> "ConceptMap.Explainable":
            pass

        @abstractmethod
        def ownership(self, owner: str, attribute: str) -> "ConceptMap.Explainable":
            pass

        @abstractmethod
        def relations(self) -> Mapping[str, "ConceptMap.Explainable"]:
            pass

        @abstractmethod
        def attributes(self) -> Mapping[str, "ConceptMap.Explainable"]:
            pass

        @abstractmethod
        def ownerships(self) -> Mapping[Tuple[str, str], "ConceptMap.Explainable"]:
            pass

    class Explainable(ABC):

        @abstractmethod
        def conjunction(self) -> str:
            pass

        @abstractmethod
        def explainable_id(self) -> int:
            pass
