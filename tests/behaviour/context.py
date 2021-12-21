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

import behave.runner
from behave.model import Table

from typedb.client import *
from tests.behaviour.config.parameters import RootLabel


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
        self.session_options = TypeDBOptions = None
        self.transaction_options: TypeDBOptions = None
        self.things: Dict[str, Thing] = {}
        self.answers: Optional[List[ConceptMap]] = None
        self.numeric_answer: Optional[Numeric] = None
        self.answer_groups: Optional[List[ConceptMapGroup]] = None
        self.numeric_answer_groups: Optional[List[NumericGroup]] = None
        self.config = Config()
        self.option_setters = {
            "session-idle-timeout-millis": lambda option, value: option.set_session_idle_timeout_millis(int(value)),
            "transaction-timeout-millis": lambda option, value: option.set_transaction_timeout_millis(int(value)),
        }

    def tx(self) -> TypeDBTransaction:
        return self.sessions_to_transactions[self.sessions[0]][0]

    def put(self, var: str, thing: Thing) -> None:
        pass

    def get(self, var: str) -> Thing:
        pass

    def get_thing_type(self, root_label: RootLabel, type_label: str) -> ThingType:
        pass

    def clear_answers(self) -> None:
        pass
