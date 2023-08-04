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
from abc import ABC, abstractmethod
from datetime import datetime
from enum import Enum
from typing import Mapping, Union

from typedb.api.concept.concept import Concept
from typedb.common.exception import TypeDBClientException, UNEXPECTED_NATIVE_VALUE

from typedb.typedb_client_python import Object, Boolean, Long, Double, String, DateTime


class Value(Concept, ABC):

    @abstractmethod
    def get_value_type(self) -> ValueType:
        pass

    @abstractmethod
    def get_value(self) -> Union[bool, int, float, str, datetime]:
        pass

    def is_value(self) -> bool:
        return True

    def as_value(self) -> Value:
        return self

    def is_boolean(self) -> bool:
        pass

    def is_long(self) -> bool:
        pass

    def is_double(self) -> bool:
        pass

    def is_string(self) -> bool:
        pass

    def is_datetime(self) -> bool:
        pass

    def as_boolean(self) -> bool:
        pass

    def as_long(self) -> int:
        pass

    def as_double(self) -> float:
        pass

    def as_string(self) -> str:
        pass

    def as_datetime(self) -> datetime:
        pass

    def to_json(self) -> Mapping[str, Union[str, int, float, bool]]:
        return {
            "value_type": str(self.get_value_type()),
            "value": self.get_value() if not self.is_datetime() else self.get_value().isoformat(timespec='milliseconds')
        }


class _ValueType:

    def __init__(self, is_writable: bool, is_keyable: bool, native_object):
        self._is_writable = is_writable
        self._is_keyable = is_keyable
        self._native_object = native_object

    def is_writable(self) -> bool:
        return self._is_writable

    def is_keyable(self) -> bool:
        return self._is_keyable

    @property
    def native_object(self):
        return self._native_object


class ValueType(Enum):
    OBJECT = _ValueType(False, False, Object)
    BOOLEAN = _ValueType(True, False, Boolean)
    LONG = _ValueType(True, True, Long)
    DOUBLE = _ValueType(True, False, Double)
    STRING = _ValueType(True, True, String)
    DATETIME = _ValueType(True, True, DateTime)

    @property
    def native_object(self):
        return self.value.native_object

    def __str__(self):
        mapping = {ValueType.OBJECT: "object",
                   ValueType.BOOLEAN: "boolean",
                   ValueType.LONG: "long",
                   ValueType.DOUBLE: "double",
                   ValueType.STRING: "string",
                   ValueType.DATETIME: "datetime",
                   }
        return mapping[self]

    @staticmethod
    def of(value_type: Union[Object, Boolean, Long, Double, String, DateTime]) -> ValueType:
        for type_ in ValueType:
            if type_.native_object == value_type:
                return type_
        raise TypeDBClientException(UNEXPECTED_NATIVE_VALUE)
