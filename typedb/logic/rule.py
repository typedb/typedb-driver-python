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
from typing import TYPE_CHECKING, Union

import typedb_protocol.common.logic_pb2 as logic_proto

from typedb.api.logic.rule import Rule, RemoteRule
from typedb.common.exception import TypeDBClientException, MISSING_LABEL, MISSING_TRANSACTION
from typedb.common.rpc.request_builder import rule_set_label_req, rule_delete_req

if TYPE_CHECKING:
    from typedb.api.connection.transaction import _TypeDBTransactionExtended, TypeDBTransaction


class _Rule(Rule):

    def __init__(self, label: str, when: str, then: str):
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        self._label = label
        self._when = when
        self._then = then
        self._hash = hash(label)

    @staticmethod
    def of(rule_proto: logic_proto.Rule):
        return _Rule(rule_proto.label, rule_proto.when, rule_proto.then)

    def get_label(self):
        return self._label

    def get_when(self):
        return self._when

    def get_then(self):
        return self._then

    def as_remote(self, transaction):
        return _RemoteRule(transaction, self.get_label(), self.get_when(), self.get_then())

    def is_remote(self):
        return False

    def __str__(self):
        return type(self).__name__ + "[label: %s]" % self.get_label()

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash


class _RemoteRule(RemoteRule):

    def __init__(self, transaction_ext: Union["_TypeDBTransactionExtended", "TypeDBTransaction"], label: str, when: str, then: str):
        if not transaction_ext:
            raise TypeDBClientException.of(MISSING_TRANSACTION)
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        self._transaction_ext = transaction_ext
        self._label = label
        self._when = when
        self._then = then
        self._hash = hash((transaction_ext, label))

    def get_label(self):
        return self._label

    def get_when(self):
        return self._when

    def get_then(self):
        return self._then

    def set_label(self, new_label: str):
        self._transaction_ext.execute(rule_set_label_req(self._label, new_label))
        self._label = new_label

    def delete(self):
        self._transaction_ext.execute(rule_delete_req(self._label))

    def is_deleted(self):
        return not self._transaction_ext.logic().get_rule(self.get_label())

    def as_remote(self, transaction):
        return _RemoteRule(transaction, self.get_label(), self.get_when(), self.get_then())

    def is_remote(self):
        return True

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._transaction_ext is other._transaction_ext and self.get_label() == other.get_label()

    def __hash__(self):
        return super(RemoteRule, self).__hash__()
