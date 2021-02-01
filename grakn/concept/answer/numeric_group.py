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

import grakn_protocol.protobuf.answer_pb2 as answer_proto
from grakn.concept.answer import numeric
from grakn.concept.proto.concept_proto_reader import concept

class NumericGroup:
    def __init__(self, owner, numeric):
        self._owner = owner
        self._numeric = numeric

    def owner(self):
        return self._owner

    def numeric(self):
        return self._numeric


def _of(numeric_group_proto: answer_proto.NumericGroup):
    return NumericGroup(concept(numeric_group_proto.owner), numeric._of(numeric_group_proto.number))
