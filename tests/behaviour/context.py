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
from concurrent.futures import Future
from typing import List, Union, Optional, Dict

import behave.runner
from behave.model import Table

from typedb.api.connection.client import TypeDBClient
from typedb.api.answer.concept_map import ConceptMap
from typedb.api.answer.concept_map_group import ConceptMapGroup
from typedb.api.answer.numeric import Numeric
from typedb.api.answer.numeric_group import NumericGroup
from typedb.api.concept.concept import Concept
from typedb.api.concept.thing.attribute import Attribute, BooleanAttribute, LongAttribute, DoubleAttribute, \
    StringAttribute, DateTimeAttribute
from typedb.api.concept.thing.entity import Entity
from typedb.api.concept.thing.relation import Relation
from typedb.api.concept.thing.thing import Thing
from typedb.api.concept.type.attribute_type import AttributeType, BooleanAttributeType, LongAttributeType, \
    DoubleAttributeType, StringAttributeType, DateTimeAttributeType
from typedb.api.concept.type.entity_type import EntityType
from typedb.api.concept.type.relation_type import RelationType
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.thing_type import ThingType
from typedb.api.concept.type.type import Type
from typedb.api.connection.session import TypeDBSession
from typedb.api.connection.transaction import TypeDBTransaction
from tests.behaviour.config.parameters import RootLabel

AttributeSubtype = Union[Attribute, BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, DateTimeAttribute]
ThingSubtype = Union[Thing, Entity, Relation, AttributeSubtype]
TypeSubtype = Union[Type, ThingType, EntityType, RelationType, RoleType, AttributeType, BooleanAttributeType, LongAttributeType, DoubleAttributeType, StringAttributeType, DateTimeAttributeType]
ConceptSubtype = Union[Concept, ThingSubtype, TypeSubtype]


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
        self.client: Optional[TypeDBClient] = None
        self.sessions: List[TypeDBSession] = []
        self.sessions_to_transactions: Dict[TypeDBSession, List[TypeDBTransaction]] = {}
        self.sessions_parallel: List[Future[TypeDBSession]] = []
        self.sessions_parallel_to_transactions_parallel: Dict[Future[TypeDBSession], List[TypeDBTransaction]] = {}
        self.things: Dict[str, ThingSubtype] = {}
        self.answers: Optional[List[ConceptMap]] = None
        self.numeric_answer: Optional[Numeric] = None
        self.answer_groups: Optional[List[ConceptMapGroup]] = None
        self.numeric_answer_groups: Optional[List[NumericGroup]] = None
        self.config = Config()

    def tx(self) -> TypeDBTransaction:
        return self.sessions_to_transactions[self.sessions[0]][0]

    def put(self, var: str, thing: ThingSubtype) -> None:
        pass

    def get(self, var: str) -> ThingSubtype:
        pass

    def get_thing_type(self, root_label: RootLabel, type_label: str) -> ThingType:
        pass

    def clear_answers(self) -> None:
        pass
