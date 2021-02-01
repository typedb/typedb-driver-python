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
from typing import Callable, List

import grakn_protocol.protobuf.logic_pb2 as logic_proto
import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn.logic.rule import Rule


class LogicManager:

    def __init__(self, transaction):
        self._transaction = transaction

    def put_rule(self, label: str, when: str, then: str):
        req = logic_proto.LogicManager.Req()
        put_rule_req = logic_proto.LogicManager.PutRule.Req()
        put_rule_req.label = label
        put_rule_req.when = when
        put_rule_req.then = then
        req.put_rule_req.CopyFrom(put_rule_req)
        res = self._execute(req)
        return Rule._of(res.put_rule_res.rule)

    def get_rule(self, label: str):
        req = logic_proto.LogicManager.Req()
        get_rule_req = logic_proto.LogicManager.GetRule.Req()
        get_rule_req.label = label
        req.get_rule_req.CopyFrom(get_rule_req)

        response = self._execute(req)
        return Rule._of(response.get_rule_res.rule) if response.get_rule_res.WhichOneof("res") == "rule" else None

    def get_rules(self):
        method = logic_proto.LogicManager.Req()
        method.get_rules_req.CopyFrom(logic_proto.LogicManager.GetRules.Req())
        return self._rule_stream(method, lambda res: res.get_rules_res.rules)

    def _execute(self, request: logic_proto.LogicManager.Req):
        req = transaction_proto.Transaction.Req()
        req.logic_manager_req.CopyFrom(request)
        return self._transaction._execute(req).logic_manager_res

    def _rule_stream(self, method: logic_proto.LogicManager.Req, rule_list_getter: Callable[[logic_proto.LogicManager.Res], List[logic_proto.Rule]]):
        request = transaction_proto.Transaction.Req()
        request.logic_manager_req.CopyFrom(method)
        return map(Rule._of, self._transaction._stream(request, lambda res: rule_list_getter(res.logic_manager_res)))
