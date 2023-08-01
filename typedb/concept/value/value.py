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

from __future__ import annotations
from abc import ABC
from datetime import datetime, timezone
from functools import singledispatchmethod
from typing import Union

from typedb.api.concept.value.value import Value, ValueType
from typedb.common.exception import TypeDBClientException, UNEXPECTED_NATIVE_VALUE, ILLEGAL_STATE
from typedb.concept.concept import _Concept

from typedb.typedb_client_python import Concept, value_new_boolean, value_new_long, value_new_double, value_new_string, \
    value_new_date_time_from_millis, value_is_boolean, value_is_long, value_is_double, value_is_string, \
    value_is_date_time, value_get_boolean, value_get_long, value_get_double, value_get_string, \
    value_get_date_time_as_millis


class _Value(Value, _Concept, ABC):

    # def as_value(self) -> "Value":
    #     return self

    # @staticmethod
    @singledispatchmethod
    def of(value):
        raise TypeDBClientException.of(UNEXPECTED_NATIVE_VALUE)

    # @staticmethod
    @of.register
    def _(value: bool):
        return _Value(value_new_boolean(value))

    # @staticmethod
    @of.register
    def _(value: int):
        return _Value(value_new_long(value))

    # @staticmethod
    @of.register
    def _(value: float):
        return _Value(value_new_double(value))

    # @staticmethod
    @of.register
    def _(value: str):
        return _Value(value_new_string(value))

    # @staticmethod
    @of.register
    def _(value: datetime):
        return _Value(value_new_date_time_from_millis(value.astimezone(timezone.utc).timestamp() * 1000))

    def get_value_type(self) -> ValueType:
        if self.is_boolean():
            return ValueType.BOOLEAN
        if self.is_long():
            return ValueType.LONG
        if self.is_double():
            return ValueType.DOUBLE
        if self.is_string():
            return ValueType.STRING
        if self.is_datetime():
            return ValueType.DATETIME
        raise TypeDBClientException(ILLEGAL_STATE)

    def get_value(self) -> Union[bool, int, float, str, datetime]:
        if self.is_boolean():
            return self.as_boolean()
        if self.is_long():
            return self.as_long()
        if self.is_double():
            return self.as_double()
        if self.is_string():
            return self.as_string()
        if self.is_datetime():
            return self.as_datetime()
        raise TypeDBClientException(ILLEGAL_STATE)

    def is_boolean(self) -> bool:
        return value_is_boolean(self._concept)

    def is_long(self) -> bool:
        return value_is_long(self._concept)

    def is_double(self) -> bool:
        return value_is_double(self._concept)

    def is_string(self) -> bool:
        return value_is_string(self._concept)

    def is_datetime(self) -> bool:
        return value_is_date_time(self._concept)

    def as_boolean(self) -> bool:
        return value_get_boolean(self._concept)

    def as_long(self) -> int:
        return value_get_long(self._concept)

    def as_double(self) -> float:
        return value_get_double(self._concept)

    def as_string(self) -> str:
        return value_get_string(self._concept)

    def as_datetime(self) -> datetime:
        return datetime.utcfromtimestamp(value_get_date_time_as_millis(self._concept) / 1000)

    def __repr__(self):
        return f"{self.get_value_type()}({self.get_value()})"

    def __str__(self):
        return str(self.get_value())

    def __hash__(self):
        return hash(self.get_value())

# class _BooleanValue(BooleanValue, _Value):
#
#     def __init__(self, value: bool):
#         super(_BooleanValue, self).__init__()
#         self._value = value
#
#     @staticmethod
#     def of(value_proto: concept_proto.Value):
#         return _BooleanValue(value_proto.value.boolean)
#
#     def get_value(self):
#         return self._value
#
#     def get_value_type(self) -> "ValueType":
#         return ValueType.BOOLEAN
#
#
# class _LongValue(LongValue, _Value):
#
#     def __init__(self, value: int):
#         super(_LongValue, self).__init__()
#         self._value = value
#
#     @staticmethod
#     def of(value_proto: concept_proto.Value):
#         return _LongValue(value_proto.value.long)
#
#     def get_value(self):
#         return self._value
#
#     def get_value_type(self) -> "ValueType":
#         return ValueType.LONG
#
#
# class _DoubleValue(DoubleValue, _Value):
#
#     def __init__(self, value: float):
#         super(_DoubleValue, self).__init__()
#         self._value = value
#
#     @staticmethod
#     def of(value_proto: concept_proto.Value):
#         return _DoubleValue(value_proto.value.double)
#
#     def get_value(self):
#         return self._value
#
#     def get_value_type(self) -> "ValueType":
#         return ValueType.DOUBLE
#
#
# class _StringValue(StringValue, _Value):
#
#     def __init__(self, value: str):
#         super(_StringValue, self).__init__()
#         self._value = value
#
#     @staticmethod
#     def of(value_proto: concept_proto.Value):
#         return _StringValue(value_proto.value.string)
#
#     def get_value(self):
#         return self._value
#
#     def get_value_type(self) -> "ValueType":
#         return ValueType.STRING
#
#
# class _DateTimeValue(DateTimeValue, _Value):
#
#     def __init__(self, value: datetime):
#         super(_DateTimeValue, self).__init__()
#         self._value = value
#
#     @staticmethod
#     def of(value_proto: concept_proto.Value):
#         return _DateTimeValue(datetime.utcfromtimestamp(float(value_proto.value.date_time) / 1000.0))
#
#     def get_value(self):
#         return self._value
#
#     def get_value_type(self) -> "ValueType":
#         return ValueType.DATETIME
