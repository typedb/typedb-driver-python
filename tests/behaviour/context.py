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
from typing import List

import behave.runner

from grakn.client import GraknClient
from grakn.concept.thing.thing import Thing
from grakn.rpc.session import Session
from grakn.rpc.transaction import Transaction


class Context(behave.runner.Context):
    """
    Type definitions for Context.

    This class should not be instantiated. The initialisation of the actual Context object occurs in environment.py.
    """
    def __init__(self):
        self.THREAD_POOL_SIZE = 0
        self.client: GraknClient = None
        self.sessions: List[Session] = []
        self.sessions_to_transactions: dict[Session, List[Transaction]] = {}
        self.sessions_parallel: List[Future[Session]] = []
        self.sessions_parallel_to_transactions_parallel: dict[Future[Session], List[Transaction]] = {}
        self.things: dict[str, Thing] = {}

    def tx(self) -> Transaction:
        return self.sessions_to_transactions[self.sessions[0]][0]
