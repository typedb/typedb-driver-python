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
from typing import Iterator

from grakn.api.answer.concept_map import ConceptMap
from grakn.api.answer.concept_map_group import ConceptMapGroup
from grakn.api.answer.numeric import Numeric
from grakn.api.answer.numeric_group import NumericGroup
from grakn.api.logic.explanation import Explanation
from grakn.api.options import GraknOptions
from grakn.api.query.future import QueryFuture


class QueryManager(ABC):

    @abstractmethod
    def match(self, query: str, options: GraknOptions = None) -> Iterator[ConceptMap]:
        pass

    @abstractmethod
    def match_aggregate(self, query: str, options: GraknOptions = None) -> QueryFuture[Numeric]:
        pass

    @abstractmethod
    def match_group(self, query: str, options: GraknOptions = None) -> Iterator[ConceptMapGroup]:
        pass

    @abstractmethod
    def match_group_aggregate(self, query: str, options: GraknOptions = None) -> Iterator[NumericGroup]:
        pass

    @abstractmethod
    def insert(self, query: str, options: GraknOptions = None) -> Iterator[ConceptMap]:
        pass

    @abstractmethod
    def delete(self, query: str, options: GraknOptions = None) -> QueryFuture:
        pass

    @abstractmethod
    def update(self, query: str, options: GraknOptions = None) -> Iterator[ConceptMap]:
        pass

    @abstractmethod
    def explain(self, explainable: ConceptMap.Explainable, options: GraknOptions = None) -> Iterator[Explanation]:
        pass

    @abstractmethod
    def define(self, query: str, options: GraknOptions = None) -> QueryFuture:
        pass

    @abstractmethod
    def undefine(self, query: str, options: GraknOptions = None) -> QueryFuture:
        pass
