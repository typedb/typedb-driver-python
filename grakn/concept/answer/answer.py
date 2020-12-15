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

import graknprotocol.protobuf.answer_pb2 as answer_proto

from grakn.common.exception import GraknClientException
from grakn.concept.answer import concept_map


class Answer(object):

    _CONCEPT_MAP = "concept_map"


def _of(proto_answer: answer_proto.Answer):
    answer_case = proto_answer.WhichOneof("answer")
    if answer_case == Answer._CONCEPT_MAP:
        return concept_map._of(proto_answer.concept_map)
    raise GraknClientException("The answer type " + answer_case + " was not recognised.")
