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

import grakn_protocol.protobuf.logic_pb2 as logic_proto
import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException


class Rule:

    def __init__(self, label: str, when: str, then: str):
        if not label:
            raise GraknClientException("Label must be a non-empty string.")
        self._label = label
        self._when = when
        self._then = then
        self._hash = hash(label)

    @staticmethod
    def _of(rule_proto: logic_proto.Rule):
        return Rule(rule_proto.label, rule_proto.when, rule_proto.then)

    def get_label(self):
        return self._label

    def get_when(self):
        return self._when

    def get_then(self):
        return self._then

    def as_remote(self, transaction):
        return RemoteRule(transaction, self.get_label(), self.get_when(), self.get_then())

    def is_remote(self):
        return False

    def __str__(self):
        return type(self).__name__ + "[label:" + self.get_label() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash


class RemoteRule(Rule):

    def __init__(self, transaction, label: str, when: str, then: str):
        super(RemoteRule, self).__init__(label, when, then)
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        self._transaction = transaction
        self._hash = hash((transaction, label))

    def set_label(self, label: str):
        req = logic_proto.Rule.Req()
        set_label_req = logic_proto.Rule.SetLabel.Req()
        set_label_req.label = label
        req.rule_set_label_req.CopyFrom(set_label_req)
        self._execute(req)
        self._label = label

    def delete(self):
        method = logic_proto.Rule.Req()
        method.rule_delete_req.CopyFrom(logic_proto.Rule.Delete.Req())
        self._execute(method)

    def is_deleted(self):
        return not self._transaction.logic().get_rule(self.get_label())

    def is_remote(self):
        return True

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._transaction is other._transaction and self.get_label() == other.get_label()

    def __hash__(self):
        return super(RemoteRule, self).__hash__()

    def _execute(self, method: logic_proto.Rule.Req):
        method.label = self.get_label()
        request = transaction_proto.Transaction.Req()
        request.rule_req.CopyFrom(method)
        return self._transaction._execute(request).rule_res
