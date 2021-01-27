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

from grakn.common.exception import GraknClientException


class Numeric:
    def __init__(self, int_value, float_value):
        self._int_value = int_value
        self._float_value = float_value

    def is_int(self):
        return self._int_value is not None

    def is_float(self):
        return self._float_value is not None

    def is_nan(self):
        return not self.is_int() and not self.is_float()

    def as_float(self):
        if (self.is_float()):
            return self._float_value
        else:
            raise GraknClientException("Illegal casting operation to 'float'.")

    def as_int(self):
        if (self.is_int()):
            return self._int_value
        else:
            raise GraknClientException("Illegal casting operation to 'int'.")


def _of(numeric_proto: answer_proto.Numeric):
    numeric_case = numeric_proto.WhichOneof("value")
    if numeric_case == "long_value":
        return Numeric(numeric_proto.long_value, None)
    elif numeric_case == "double_value":
        return Numeric(None, numeric_proto.double_value)
    elif numeric_case == "nan":
        return Numeric(None, None)
    else:
        raise GraknClientException("The answer type '" + numeric_case + "' was not recognised.")
