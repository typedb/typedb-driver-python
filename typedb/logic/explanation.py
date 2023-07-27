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

from typedb.api.answer.concept_map import ConceptMap
from typedb.api.logic.explanation import Explanation
from typedb.api.logic.rule import Rule
from typedb.common.exception import TypeDBClientException, MISSING_VARIABLE
from typedb.common.streamer import Streamer
from typedb.concept.answer.concept_map import _ConceptMap
from typedb.logic.rule import _Rule

from typedb.typedb_client_python import Explanation as NativeExplanation, explanation_get_rule, \
    explanation_get_conclusion, explanation_get_mapped_variables, explanation_get_condition, string_iterator_next, \
    explanation_get_mapping, explanation_to_string, explanation_equals


# def _var_mapping_of(var_mapping: Mapping[str, logic_proto.Explanation.VarList]):
#     mapping = {}
#     for from_ in var_mapping:
#         tos = var_mapping[from_]
#         mapping[from_] = set(tos.vars)
#     return mapping


class _Explanation(Explanation):

    def __init__(self, explanation: NativeExplanation):
        self._explanation = explanation

    # @staticmethod
    # def of(explanation: logic_proto.Explanation):
    #     return _Explanation(_Rule.of(explanation.rule), _var_mapping_of(explanation.var_mapping),
    #                         _ConceptMap.of(explanation.conclusion), _ConceptMap.of(explanation.condition))

    def rule(self) -> Rule:
        return _Rule(explanation_get_rule(self._explanation))

    def conclusion(self) -> ConceptMap:
        return _ConceptMap(explanation_get_conclusion(self._explanation))

    def condition(self) -> ConceptMap:
        return _ConceptMap(explanation_get_condition(self._explanation))

    def query_variables(self) -> set[str]:
        return set(Streamer(explanation_get_mapped_variables(self._explanation), string_iterator_next))

    def query_variable_mapping(self, var: str) -> set[str]:
        if not var:
            raise TypeDBClientException(MISSING_VARIABLE)
        return set(Streamer(explanation_get_mapping(self._explanation, var), string_iterator_next))

    def __str__(self):
        return explanation_to_string(self._explanation)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return explanation_equals(self._explanation, other._explanation)

    def __hash__(self):
        return hash((self.rule(), self.condition(), self.conclusion()))
