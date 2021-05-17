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
from typing import Mapping, Set

import typedb_protocol.common.logic_pb2 as logic_proto

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.logic.explanation import Explanation
from typedb.api.logic.rule import Rule
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.logic.rule import _Rule


def _var_mapping_of(var_mapping: Mapping[str, logic_proto.Explanation.VarList]):
    mapping = {}
    for from_ in var_mapping:
        tos = var_mapping[from_]
        mapping[from_] = set(tos.vars)
    return mapping


class _Explanation(Explanation):

    def __init__(self, rule: Rule, variable_mapping: Mapping[str, Set[str]], conclusion: ConceptMap, condition: ConceptMap):
        self._rule = rule
        self._variable_mapping = variable_mapping
        self._conclusion = conclusion
        self._condition = condition

    @staticmethod
    def of(explanation: logic_proto.Explanation):
        return _Explanation(_Rule.of(explanation.rule), _var_mapping_of(explanation.var_mapping),
                            _ConceptMap.of(explanation.conclusion), _ConceptMap.of(explanation.condition))

    def rule(self) -> Rule:
        return self._rule

    def variable_mapping(self) -> Mapping[str, Set[str]]:
        return self._variable_mapping

    def conclusion(self) -> ConceptMap:
        return self._conclusion

    def condition(self) -> ConceptMap:
        return self._condition

    def __str__(self):
        return "Explanation[rule: %s, variable_mapping: %s, then_answer: %s, when_answer: %s]" % (self._rule, self._variable_mapping, self._conclusion, self._condition)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._rule == other._rule and self._variable_mapping == other._variable_mapping and self._conclusion == other._conclusion and self._condition == other._condition

    def __hash__(self):
        return hash((self._rule, self._variable_mapping, self._conclusion, self._condition))
