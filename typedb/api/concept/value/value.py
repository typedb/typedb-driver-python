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
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Mapping, Union

from typedb.api.concept.concept import Concept, ValueType
from typedb.api.connection.transaction import TypeDBTransaction
from typedb.common.exception import TypeDBClientException, VALUE_HAS_NO_REMOTE



class Value(Concept, ABC):

    @abstractmethod
    def get_value_type(self) -> "ValueType":
        pass

    @abstractmethod
    def get_value(self) -> Union[bool, int, float, str, datetime]:
        pass

    def is_value(self):
        return True

    def is_boolean(self):
        return False

    def is_long(self):
        return False

    def is_double(self):
        return False

    def is_string(self):
        return False

    def is_datetime(self):
        return False

    def as_remote(self, transaction: "TypeDBTransaction"):
        raise TypeDBClientException.of(VALUE_HAS_NO_REMOTE)

    def to_json(self) -> Mapping[str, Union[str, int, float, bool]]:
        return {
            "value_type": str(self.get_value_type()),
            "value": self.get_value(),
        }


class BooleanValue(Value, ABC):

    def is_boolean(self) -> bool:
        return True

    @abstractmethod
    def get_value(self) -> bool:
        pass


class LongValue(Value, ABC):

    def is_long(self) -> bool:
        return True

    @abstractmethod
    def get_value(self) -> int:
        pass


class DoubleValue(Value, ABC):

    def is_double(self) -> bool:
        return True

    @abstractmethod
    def get_value(self) -> float:
        pass


class StringValue(Value, ABC):

    def is_string(self) -> bool:
        return True

    @abstractmethod
    def get_value(self) -> str:
        pass


class DateTimeValue(Value, ABC):

    def is_datetime(self) -> bool:
        return True

    @abstractmethod
    def get_value(self) -> datetime:
        pass

    def to_json(self) -> Mapping[str, Union[str, int, float, bool]]:
        return {
            "value_type": str(self.get_value_type()),
            "value": self.get_value().isoformat(timespec='milliseconds')
        }
