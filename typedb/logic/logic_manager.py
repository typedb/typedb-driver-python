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

from typedb.native_client_wrapper import logic_manager_get_rule, logic_manager_get_rules, rule_iterator_next, \
    logic_manager_put_rule

from typedb.api.logic.logic_manager import LogicManager
from typedb.api.logic.rule import Rule
from typedb.common.exception import TypeDBClientExceptionExt, MISSING_LABEL, TRANSACTION_CLOSED
from typedb.common.iterator_wrapper import IteratorWrapper
from typedb.common.native_object_mixin import NativeObjectMixin
from typedb.logic.rule import _Rule

if TYPE_CHECKING:
    from typedb.native_client_wrapper import Transaction as NativeTransaction


def _not_blank_label(label: str) -> str:
    if not label or label.isspace():
        raise TypeDBClientExceptionExt.of(MISSING_LABEL)
    return label


class _LogicManager(LogicManager, NativeObjectMixin):

    def __init__(self, transaction: NativeTransaction):
        self._transaction = transaction

    @property
    def _native_object(self) -> NativeTransaction:
        return self._transaction

    @property
    def _native_object_not_owned_exception(self) -> TypeDBClientExceptionExt:
        return TypeDBClientExceptionExt.of(TRANSACTION_CLOSED)

    @property
    def _native_transaction(self) -> NativeTransaction:
        return self.native_object

    def get_rule(self, label: str) -> Optional[Rule]:
        if rule := logic_manager_get_rule(self._transaction, _not_blank_label(label)):
            return _Rule(rule)
        return None

    def get_rules(self):
        return map(_Rule, IteratorWrapper(logic_manager_get_rules(self._transaction), rule_iterator_next))

    def put_rule(self, label: str, when: str, then: str):
        return _Rule(logic_manager_put_rule(self._transaction, _not_blank_label(label), when, then))
