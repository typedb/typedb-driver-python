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
from concurrent.futures._base import Future
from typing import List, Union, Optional

import behave.runner
from behave.model import Table

from grakn.client import GraknClient
from grakn.concept.answer.concept_map import ConceptMap
from grakn.concept.answer.concept_map_group import ConceptMapGroup
from grakn.concept.answer.numeric import Numeric
from grakn.concept.answer.numeric_group import NumericGroup
from grakn.concept.concept import Concept
from grakn.concept.thing.attribute import Attribute, BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, \
    DateTimeAttribute
from grakn.concept.thing.entity import Entity
from grakn.concept.thing.relation import Relation
from grakn.concept.thing.thing import Thing
from grakn.concept.type.attribute_type import AttributeType, BooleanAttributeType, LongAttributeType, \
    DoubleAttributeType, StringAttributeType, DateTimeAttributeType
from grakn.concept.type.entity_type import EntityType
from grakn.concept.type.relation_type import RelationType
from grakn.concept.type.role_type import RoleType
from grakn.concept.type.thing_type import ThingType
from grakn.concept.type.type import Type
from grakn.rpc.session import Session
from grakn.rpc.transaction import Transaction


AttributeSubtype: Attribute = Union[BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, DateTimeAttribute]
ThingSubtype: Thing = Union[Entity, Relation, AttributeSubtype]
TypeSubtype: Type = Union[ThingType, EntityType, RelationType, RoleType, AttributeType, BooleanAttributeType, LongAttributeType, DoubleAttributeType, StringAttributeType, DateTimeAttributeType]
ConceptSubtype: Concept = Union[ThingSubtype, TypeSubtype]


class Config:
    """
    Type definitions for Config.

    This class should not be instantiated. The initialisation of the actual Config object occurs in environment.py.
    """
    def __init__(self):
        self.userdata = {}


class Context(behave.runner.Context):
    """
    Type definitions for Context.

    This class should not be instantiated. The initialisation of the actual Context object occurs in environment.py.
    """
    def __init__(self):
        self.table: Optional[Table] = None
        self.THREAD_POOL_SIZE = 0
        self.client: Optional[GraknClient] = None
        self.sessions: List[Session] = []
        self.sessions_to_transactions: dict[Session, List[Transaction]] = {}
        self.sessions_parallel: List[Future[Session]] = []
        self.sessions_parallel_to_transactions_parallel: dict[Future[Session], List[Transaction]] = {}
        self.things: dict[str, ThingSubtype] = {}
        self.answers: Optional[List[ConceptMap]] = None
        self.numeric_answer: Optional[Numeric] = None
        self.answer_groups: Optional[List[ConceptMapGroup]] = None
        self.numeric_answer_groups: Optional[List[NumericGroup]] = None
        self.config = Config()

    def tx(self) -> Transaction:
        return self.sessions_to_transactions[self.sessions[0]][0]

    def put(self, var: str, thing: ThingSubtype) -> None:
        pass

    def get(self, var: str) -> ThingSubtype:
        pass

    def clear_answers(self) -> None:
        pass
