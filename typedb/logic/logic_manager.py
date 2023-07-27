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

from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from typedb.api.logic.logic_manager import LogicManager
from typedb.api.logic.rule import Rule
from typedb.common.exception import TypeDBClientException, MISSING_LABEL
from typedb.common.streamer import Streamer
from typedb.logic.rule import _Rule

if TYPE_CHECKING:
    from typedb.typedb_client_python import Transaction, logic_manager_get_rule, logic_manager_get_rules, \
    rule_iterator_next, logic_manager_put_rule


class _LogicManager(LogicManager):

    def __init__(self, transaction: Transaction):
        self._transaction = transaction

    def native_transaction(self):
        return self._transaction

    def get_rule(self, label: str) -> Optional[Rule]:
        if not label:
            raise TypeDBClientException(MISSING_LABEL)
        if rule := logic_manager_get_rule(self._transaction, label):
            return _Rule(rule)
        return None

    def get_rules(self):
        return map(_Rule, Streamer(logic_manager_get_rules(self._transaction), rule_iterator_next))

    def put_rule(self, label: str, when: str, then: str):
        if not label:
            raise TypeDBClientException(MISSING_LABEL)
        return _Rule(logic_manager_put_rule(self._transaction, label, when, then))
