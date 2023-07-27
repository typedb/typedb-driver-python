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

from typedb.api.answer.numeric import Numeric
from typedb.api.answer.numeric_group import NumericGroup
from typedb.api.concept.concept import Concept
from typedb.concept.answer.numeric import _Numeric
from typedb.concept.concept import _Concept

from typedb.typedb_client_python import NumericGroup as NativeNumericGroup, numeric_group_get_owner, \
    numeric_group_get_numeric, numeric_group_to_string, numeric_group_equals


class _NumericGroup(NumericGroup):

    def __init__(self, numeric_group: NativeNumericGroup):
        self._numeric_group = numeric_group

    # @staticmethod
    # def of(numeric_group_proto: answer_proto.NumericGroup):
    #     return _NumericGroup(concept_proto_reader.concept(numeric_group_proto.owner), _Numeric.of(numeric_group_proto.number))

    def owner(self) -> Concept:
        return _Concept(numeric_group_get_owner(self._numeric_group))

    def numeric(self) -> Numeric:
        return _Numeric(numeric_group_get_numeric(self._numeric_group))

    def __str__(self):
        return numeric_group_to_string(self._numeric_group)

    def __eq__(self, other):
        return other and isinstance(other, NumericGroup) and \
            numeric_group_equals(self._numeric_group, self._numeric_group)

    def __hash__(self):
        return hash((self.owner(), self.numeric()))
