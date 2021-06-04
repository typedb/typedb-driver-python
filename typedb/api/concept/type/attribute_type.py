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
import enum
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Optional, TYPE_CHECKING, Iterator

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.thing.attribute import BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, \
    DateTimeAttribute, Attribute
from typedb.api.concept.type.thing_type import ThingType, RemoteThingType

if TYPE_CHECKING:
    from typedb.api.connection.transaction import TypeDBTransaction


class AttributeType(ThingType, ABC):

    def get_value_type(self) -> "ValueType":
        return AttributeType.ValueType.OBJECT

    def is_attribute_type(self) -> bool:
        return True

    def is_boolean(self) -> bool:
        return False

    def is_long(self) -> bool:
        return False

    def is_double(self) -> bool:
        return False

    def is_string(self) -> bool:
        return False

    def is_datetime(self) -> bool:
        return False

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteAttributeType":
        pass

    @abstractmethod
    def as_boolean(self) -> "BooleanAttributeType":
        pass

    @abstractmethod
    def as_long(self) -> "LongAttributeType":
        pass

    @abstractmethod
    def as_double(self) -> "DoubleAttributeType":
        pass

    @abstractmethod
    def as_string(self) -> "StringAttributeType":
        pass

    @abstractmethod
    def as_datetime(self) -> "DateTimeAttributeType":
        pass

    class ValueType(enum.Enum):
        OBJECT = 0
        BOOLEAN = 1
        LONG = 2
        DOUBLE = 3
        STRING = 4
        DATETIME = 5

        def is_writable(self) -> bool:
            return self is not AttributeType.ValueType.OBJECT

        def is_keyable(self) -> bool:
            return self in [AttributeType.ValueType.LONG, AttributeType.ValueType.STRING, AttributeType.ValueType.DATETIME]

        def proto(self) -> concept_proto.AttributeType.ValueType:
            return concept_proto.AttributeType.ValueType.Value(self.name)


class RemoteAttributeType(RemoteThingType, AttributeType, ABC):

    @abstractmethod
    def set_supertype(self, attribute_type: AttributeType) -> None:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[AttributeType]:
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["Attribute"]:
        pass

    @abstractmethod
    def get_owners(self, only_key: bool = False) -> Iterator[ThingType]:
        pass

    @abstractmethod
    def as_boolean(self) -> "RemoteBooleanAttributeType":
        pass

    @abstractmethod
    def as_long(self) -> "RemoteLongAttributeType":
        pass

    @abstractmethod
    def as_double(self) -> "RemoteDoubleAttributeType":
        pass

    @abstractmethod
    def as_string(self) -> "RemoteStringAttributeType":
        pass

    @abstractmethod
    def as_datetime(self) -> "RemoteDateTimeAttributeType":
        pass


class BooleanAttributeType(AttributeType, ABC):

    def get_value_type(self) -> AttributeType.ValueType:
        return AttributeType.ValueType.BOOLEAN

    def is_boolean(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteBooleanAttributeType":
        pass


class RemoteBooleanAttributeType(RemoteAttributeType, BooleanAttributeType, ABC):

    @abstractmethod
    def put(self, value: bool) -> "BooleanAttribute":
        pass

    @abstractmethod
    def get(self, value: bool) -> "BooleanAttribute":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["BooleanAttribute"]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[BooleanAttributeType]:
        pass

    @abstractmethod
    def set_supertype(self, attribute_type: BooleanAttributeType) -> None:
        pass


class LongAttributeType(AttributeType, ABC):

    def get_value_type(self) -> AttributeType.ValueType:
        return AttributeType.ValueType.LONG

    def is_long(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteLongAttributeType":
        pass


class RemoteLongAttributeType(RemoteAttributeType, LongAttributeType, ABC):

    @abstractmethod
    def put(self, value: int) -> "LongAttribute":
        pass

    @abstractmethod
    def get(self, value: int) -> "LongAttribute":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["LongAttribute"]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[LongAttributeType]:
        pass

    @abstractmethod
    def set_supertype(self, attribute_type: LongAttributeType) -> None:
        pass


class DoubleAttributeType(AttributeType, ABC):

    def get_value_type(self) -> AttributeType.ValueType:
        return AttributeType.ValueType.DOUBLE

    def is_double(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteDoubleAttributeType":
        pass


class RemoteDoubleAttributeType(RemoteAttributeType, DoubleAttributeType, ABC):

    @abstractmethod
    def put(self, value: float) -> "DoubleAttribute":
        pass

    @abstractmethod
    def get(self, value: float) -> "DoubleAttribute":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["DoubleAttribute"]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[DoubleAttributeType]:
        pass

    @abstractmethod
    def set_supertype(self, attribute_type: DoubleAttributeType) -> None:
        pass


class StringAttributeType(AttributeType, ABC):

    def get_value_type(self) -> AttributeType.ValueType:
        return AttributeType.ValueType.STRING

    def is_string(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteStringAttributeType":
        pass


class RemoteStringAttributeType(RemoteAttributeType, StringAttributeType, ABC):

    @abstractmethod
    def put(self, value: str) -> "StringAttribute":
        pass

    @abstractmethod
    def get(self, value: str) -> "StringAttribute":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["StringAttribute"]:
        pass

    @abstractmethod
    def get_regex(self) -> Optional[str]:
        pass

    @abstractmethod
    def set_regex(self, regex: Optional[str]):
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[StringAttributeType]:
        pass

    @abstractmethod
    def set_supertype(self, attribute_type: StringAttributeType) -> None:
        pass


class DateTimeAttributeType(AttributeType, ABC):

    def get_value_type(self) -> AttributeType.ValueType:
        return AttributeType.ValueType.DATETIME

    def is_datetime(self) -> bool:
        return True

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteDateTimeAttributeType":
        pass


class RemoteDateTimeAttributeType(RemoteAttributeType, DateTimeAttributeType, ABC):

    @abstractmethod
    def put(self, value: datetime) -> "DateTimeAttribute":
        pass

    @abstractmethod
    def get(self, value: datetime) -> "DateTimeAttribute":
        pass

    @abstractmethod
    def get_instances(self) -> Iterator["DateTimeAttribute"]:
        pass

    @abstractmethod
    def get_subtypes(self) -> Iterator[DateTimeAttributeType]:
        pass

    @abstractmethod
    def set_supertype(self, attribute_type: DateTimeAttributeType) -> None:
        pass
