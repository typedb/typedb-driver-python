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

import typedb_protocol.common.answer_pb2 as answer_proto

from typedb.api.answer.numeric import Numeric
from typedb.common.exception import TypeDBClientException, BAD_ANSWER_TYPE, ILLEGAL_CAST


class _Numeric(Numeric):

    def __init__(self, int_value, float_value):
        self._int_value = int_value
        self._float_value = float_value

    @staticmethod
    def of(numeric_proto: answer_proto.Numeric):
        numeric_case = numeric_proto.WhichOneof("value")
        if numeric_case == "long_value":
            return _Numeric(numeric_proto.long_value, None)
        elif numeric_case == "double_value":
            return _Numeric(None, numeric_proto.double_value)
        elif numeric_case == "nan":
            return _Numeric(None, None)
        else:
            raise TypeDBClientException.of(BAD_ANSWER_TYPE, numeric_case)

    def is_int(self):
        return self._int_value is not None

    def is_float(self):
        return self._float_value is not None

    def is_nan(self):
        return not self.is_int() and not self.is_float()

    def as_float(self):
        if self.is_float():
            return self._float_value
        else:
            raise TypeDBClientException.of(ILLEGAL_CAST, "float")

    def as_int(self):
        if self.is_int():
            return self._int_value
        else:
            raise TypeDBClientException.of(ILLEGAL_CAST, "int")
