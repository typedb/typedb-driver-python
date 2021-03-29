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

import grakn_protocol.common.logic_pb2 as logic_proto

from grakn.api.answer.concept_map import ConceptMap
from grakn.api.logic.explanation import Explanation
from grakn.api.logic.rule import Rule
from grakn.concept.answer.concept_map import _ConceptMap
from grakn.logic.rule import _Rule


def _var_mapping_of(var_mapping: Mapping[str, logic_proto.Explanation.VarsList]):
    mapping = {}
    for from_ in var_mapping:
        tos = var_mapping[from_]
        mapping[from_] = set(tos.vars_list)
    return mapping


class _Explanation(Explanation):

    def __init__(self, rule: Rule, variable_mapping: Mapping[str, Set[str]], then_answer: ConceptMap, when_answer: ConceptMap):
        self._rule = rule
        self._variable_mapping = variable_mapping
        self._then_answer = then_answer
        self._when_answer = when_answer

    @staticmethod
    def of(explanation: logic_proto.Explanation):
        return _Explanation(_Rule.of(explanation.rule), _var_mapping_of(explanation.var_mapping),
                            _ConceptMap.of(explanation.then_answer), _ConceptMap.of(explanation.when_answer))

    def rule(self) -> Rule:
        return self._rule

    def variable_mapping(self) -> Mapping[str, Set[str]]:
        return self._variable_mapping

    def then_answer(self) -> ConceptMap:
        return self._then_answer

    def when_answer(self) -> ConceptMap:
        return self._when_answer

    def __str__(self):
        return "Explanation[rule: %s, variable_mapping: %s, then_answer: %s, when_answer: %s]" % (self._rule, self._variable_mapping, self._then_answer, self._when_answer)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._rule == other._rule and self._variable_mapping == other._variable_mapping and self._then_answer == other._then_answer and self._when_answer == other._when_answer

    def __hash__(self):
        return hash((self._rule, self._variable_mapping, self._then_answer, self._when_answer))
