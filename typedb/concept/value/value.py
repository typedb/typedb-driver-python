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
from abc import ABC
from datetime import datetime

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.concept import ValueType
from typedb.api.concept.value.value import Value, LongValue, BooleanValue, DoubleValue, StringValue, DateTimeValue
from typedb.concept.concept import _Concept
from typedb.concept.proto import concept_proto_reader


class _Value(Value, _Concept, ABC):

    def as_value(self) -> "Value":
        return self


class _BooleanValue(BooleanValue, _Value):

    def __init__(self, value: bool):
        super(_BooleanValue, self).__init__()
        self._value = value

    @staticmethod
    def of(value_proto: concept_proto.Value):
        return _BooleanValue(value_proto.value.boolean)

    def get_value(self):
        return self._value

    def get_value_type(self) -> "ValueType":
        return ValueType.BOOLEAN


class _LongValue(LongValue, _Value):

    def __init__(self, value: int):
        super(_LongValue, self).__init__()
        self._value = value

    @staticmethod
    def of(value_proto: concept_proto.Value):
        return _LongValue(value_proto.value.long)

    def get_value(self):
        return self._value

    def get_value_type(self) -> "ValueType":
        return ValueType.LONG


class _DoubleValue(DoubleValue, _Value):

    def __init__(self, value: float):
        super(_DoubleValue, self).__init__()
        self._value = value

    @staticmethod
    def of(value_proto: concept_proto.Value):
        return _DoubleValue(value_proto.value.double)

    def get_value(self):
        return self._value

    def get_value_type(self) -> "ValueType":
        return ValueType.DOUBLE


class _StringValue(StringValue, _Value):

    def __init__(self, value: str):
        super(_StringValue, self).__init__()
        self._value = value

    @staticmethod
    def of(value_proto: concept_proto.Value):
        return _StringValue(value_proto.value.string)

    def get_value(self):
        return self._value

    def get_value_type(self) -> "ValueType":
        return ValueType.STRING


class _DateTimeValue(DateTimeValue, _Value):

    def __init__(self, value: datetime):
        super(_DateTimeValue, self).__init__()
        self._value = value

    @staticmethod
    def of(value_proto: concept_proto.Value):
        return _DateTimeValue(datetime.utcfromtimestamp(float(value_proto.value.date_time) / 1000.0))

    def get_value(self):
        return self._value

    def get_value_type(self) -> "ValueType":
        return ValueType.DATETIME
