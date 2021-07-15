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
from abc import ABC, abstractmethod
from datetime import datetime
from typing import TYPE_CHECKING, Iterator, Union

from typedb.api.concept.thing.thing import Thing, RemoteThing

if TYPE_CHECKING:
    from typedb.api.concept.type.attribute_type import AttributeType, BooleanAttributeType, LongAttributeType, \
        DoubleAttributeType, StringAttributeType, DateTimeAttributeType
    from typedb.api.concept.type.thing_type import ThingType
    from typedb.api.connection.transaction import TypeDBTransaction


class Attribute(Thing, ABC):

    @abstractmethod
    def get_type(self) -> "AttributeType":
        pass

    @abstractmethod
    def get_value(self) -> Union[bool, int, float, str, datetime]:
        pass

    def is_attribute(self):
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

    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteAttribute":
        pass


class RemoteAttribute(RemoteThing, Attribute, ABC):

    @abstractmethod
    def get_owners(self, owner_type: "ThingType" = None) -> Iterator[Thing]:
        pass


class BooleanAttribute(Attribute, ABC):

    def is_boolean(self) -> bool:
        return True

    @abstractmethod
    def get_type(self) -> "BooleanAttributeType":
        pass

    @abstractmethod
    def get_value(self) -> bool:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteBooleanAttribute":
        pass


class RemoteBooleanAttribute(RemoteAttribute, BooleanAttribute, ABC):
    pass


class LongAttribute(Attribute, ABC):

    def is_long(self) -> bool:
        return True

    @abstractmethod
    def get_type(self) -> "LongAttributeType":
        pass

    @abstractmethod
    def get_value(self) -> int:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteLongAttribute":
        pass


class RemoteLongAttribute(RemoteAttribute, LongAttribute, ABC):
    pass


class DoubleAttribute(Attribute, ABC):

    def is_double(self) -> bool:
        return True

    @abstractmethod
    def get_type(self) -> "DoubleAttributeType":
        pass

    @abstractmethod
    def get_value(self) -> float:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteDoubleAttribute":
        pass


class RemoteDoubleAttribute(RemoteAttribute, DoubleAttribute, ABC):
    pass


class StringAttribute(Attribute, ABC):

    def is_string(self) -> bool:
        return True

    @abstractmethod
    def get_type(self) -> "StringAttributeType":
        pass

    @abstractmethod
    def get_value(self) -> str:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteStringAttribute":
        pass


class RemoteStringAttribute(RemoteAttribute, StringAttribute, ABC):
    pass


class DateTimeAttribute(Attribute, ABC):

    def is_datetime(self) -> bool:
        return True

    @abstractmethod
    def get_type(self) -> "DateTimeAttributeType":
        pass

    @abstractmethod
    def get_value(self) -> datetime:
        pass

    @abstractmethod
    def as_remote(self, transaction: "TypeDBTransaction") -> "RemoteDateTimeAttribute":
        pass


class RemoteDateTimeAttribute(RemoteAttribute, DateTimeAttribute, ABC):
    pass
