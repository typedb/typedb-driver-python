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
from typing import TYPE_CHECKING

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.logic.logic_manager import LogicManager
from typedb.common.rpc.request_builder import logic_manager_get_rule_req, logic_manager_get_rules_req, \
    logic_manager_put_rule_req
from typedb.logic.rule import _Rule

if TYPE_CHECKING:
    from typedb.api.connection.transaction import _TypeDBTransactionExtended


class _LogicManager(LogicManager):

    def __init__(self, transaction_ext: "_TypeDBTransactionExtended"):
        self._transaction_ext = transaction_ext

    def get_rule(self, label: str):
        res = self.execute(logic_manager_get_rule_req(label))
        return _Rule.of(res.get_rule_res.rule) if res.get_rule_res.WhichOneof("res") == "rule" else None

    def get_rules(self):
        return (_Rule.of(r) for rp in self.stream(logic_manager_get_rules_req()) for r in rp.get_rules_res_part.rules)

    def put_rule(self, label: str, when: str, then: str):
        return _Rule.of(self.execute(logic_manager_put_rule_req(label, when, then)).put_rule_res.rule)

    def execute(self, req: transaction_proto.Transaction.Req):
        return self._transaction_ext.execute(req).logic_manager_res

    def stream(self, req: transaction_proto.Transaction.Req):
        return map(lambda rp: rp.logic_manager_res_part, self._transaction_ext.stream(req))
