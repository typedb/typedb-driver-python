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
from typing import TYPE_CHECKING

from typedb.api.logic.rule import Rule
from typedb.common.exception import TypeDBClientException, MISSING_LABEL

from typedb.typedb_client_python import Rule as NativeRule, rule_get_when, rule_get_then, rule_get_label, \
    rule_set_label, rule_delete, rule_is_deleted, rule_to_string

if TYPE_CHECKING:
    from typedb.api.connection.transaction import Transaction


class _Rule(Rule):

    def __init__(self, rule: NativeRule):
        self._rule = rule
        self._when = rule_get_when(self._rule)
        self._then = rule_get_then(self._rule)

    # @staticmethod
    # def of(rule_proto: logic_proto.Rule):
    #     return _Rule(rule_proto.label, rule_proto.when, rule_proto.then)

    def get_label(self) -> str:
        return rule_get_label(self._rule)

    def set_label(self, transaction: Transaction, new_label: str) -> None:
        if not new_label:
            raise TypeDBClientException(MISSING_LABEL)
        rule_set_label(transaction.logic, self._rule, new_label)

    def get_when(self) -> str:
        return self._when

    def get_then(self) -> str:
        return self._then

    def delete(self, transaction: Transaction) -> None:
        rule_delete(transaction.logic, self._rule)

    def is_deleted(self, transaction: Transaction) -> bool:
        return rule_is_deleted(transaction.logic, self._rule)

    def __str__(self):
        return rule_to_string(self._rule)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return hash(self.get_label())
