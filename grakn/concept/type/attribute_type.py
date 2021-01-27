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

from datetime import datetime
from typing import Optional

import grakn_protocol.protobuf.concept_pb2 as concept_proto

from grakn.common.exception import GraknClientException
from grakn.concept.proto import concept_proto_builder, concept_proto_reader
from grakn.concept.type.thing_type import ThingType, RemoteThingType
from grakn.concept.type.value_type import ValueType


def is_keyable(value_type: ValueType):
    return value_type in [ValueType.LONG, ValueType.STRING, ValueType.DATETIME]


class AttributeType(ThingType):

    ROOT_LABEL = "attribute"

    def as_remote(self, transaction):
        return RemoteAttributeType(transaction, self.get_label(), self.is_root())

    def get_value_type(self):
        return ValueType.OBJECT

    def is_keyable(self):
        return is_keyable(self.get_value_type())

    def is_attribute_type(self):
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

    def as_boolean(self):
        if self.is_root():
            return BooleanAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + BooleanAttributeType.__name__)

    def as_long(self):
        if self.is_root():
            return LongAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + LongAttributeType.__name__)

    def as_double(self):
        if self.is_root():
            return DoubleAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + DoubleAttributeType.__name__)

    def as_string(self):
        if self.is_root():
            return StringAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + StringAttributeType.__name__)

    def as_datetime(self):
        if self.is_root():
            return DateTimeAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + DateTimeAttributeType.__name__)

    def __eq__(self, other):
        if other is self:
            return True
        # root "attribute" should always be equal to itself regardless of which value class it holds
        if not other or not isinstance(other, AttributeType):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return super(AttributeType, self).__hash__()


class RemoteAttributeType(RemoteThingType):

    ROOT_LABEL = "attribute"

    def get_value_type(self):
        return ValueType.OBJECT

    def is_keyable(self):
        return is_keyable(self.get_value_type())

    def as_remote(self, transaction):
        return RemoteAttributeType(transaction, self.get_label(), self.is_root())

    def get_subtypes(self):
        stream = super(RemoteAttributeType, self).get_subtypes()
        if self.is_root() and self.get_value_type() is not ValueType.OBJECT:
            return [subtype for subtype in stream if subtype.get_value_type() is self.get_value_type() or subtype.get_label() == self.get_label()]
        else:
            return stream

    def get_owners(self, only_key=False):
        method = concept_proto.Type.Req()
        get_owners_req = concept_proto.AttributeType.GetOwners.Req()
        get_owners_req.only_key = only_key
        method.attribute_type_get_owners_req.CopyFrom(get_owners_req)
        return self._type_stream(method, lambda res: res.attribute_type_get_owners_res.owners)

    def _put_internal(self, value_proto):
        method = concept_proto.Type.Req()
        put_req = concept_proto.AttributeType.Put.Req()
        put_req.value.CopyFrom(value_proto)
        method.attribute_type_put_req.CopyFrom(put_req)
        return concept_proto_reader.attribute(self._execute(method).attribute_type_put_res.attribute)

    def _get_internal(self, value_proto):
        method = concept_proto.Type.Req()
        get_req = concept_proto.AttributeType.Get.Req()
        get_req.value.CopyFrom(value_proto)
        method.attribute_type_get_req.CopyFrom(get_req)
        response = self._execute(method).attribute_type_get_res
        return concept_proto_reader.attribute(response.attribute) if response.WhichOneof("res") == "attribute" else None

    def is_attribute_type(self):
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

    def as_boolean(self):
        if self.is_root():
            return BooleanAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + BooleanAttributeType.__name__)

    def as_long(self):
        if self.is_root():
            return LongAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + LongAttributeType.__name__)

    def as_double(self):
        if self.is_root():
            return DoubleAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + DoubleAttributeType.__name__)

    def as_string(self):
        if self.is_root():
            return StringAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + StringAttributeType.__name__)

    def as_datetime(self):
        if self.is_root():
            return DateTimeAttributeType(self.ROOT_LABEL, is_root=True)
        raise GraknClientException("Invalid conversion to " + DateTimeAttributeType.__name__)

    def __eq__(self, other):
        if other is self:
            return True
        if not other or not isinstance(RemoteAttributeType, other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return super(RemoteAttributeType, self).__hash__()


class BooleanAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return BooleanAttributeType(type_proto.label, type_proto.root)

    def get_value_type(self):
        return ValueType.BOOLEAN

    def as_remote(self, transaction):
        return RemoteBooleanAttributeType(transaction, self.get_label(), self.is_root())

    def is_boolean(self):
        return True

    def as_boolean(self):
        return self


class RemoteBooleanAttributeType(RemoteAttributeType):

    def get_value_type(self):
        return ValueType.BOOLEAN

    def as_remote(self, transaction):
        return RemoteBooleanAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: bool):
        return self._put_internal(concept_proto_builder.boolean_attribute_value(value))

    def get(self, value: bool):
        return self._get_internal(concept_proto_builder.boolean_attribute_value(value))

    def is_boolean(self):
        return True

    def as_boolean(self):
        return self


class LongAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return LongAttributeType(type_proto.label, type_proto.root)

    def get_value_type(self):
        return ValueType.LONG

    def as_remote(self, transaction):
        return RemoteLongAttributeType(transaction, self.get_label(), self.is_root())

    def is_long(self):
        return True

    def as_long(self):
        return self


class RemoteLongAttributeType(RemoteAttributeType):

    def get_value_type(self):
        return ValueType.LONG

    def as_remote(self, transaction):
        return RemoteLongAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: int):
        return self._put_internal(concept_proto_builder.long_attribute_value(value))

    def get(self, value: int):
        return self._get_internal(concept_proto_builder.long_attribute_value(value))

    def is_long(self):
        return True

    def as_long(self):
        return self


class DoubleAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return DoubleAttributeType(type_proto.label, type_proto.root)

    def get_value_type(self):
        return ValueType.DOUBLE

    def as_remote(self, transaction):
        return RemoteDoubleAttributeType(transaction, self.get_label(), self.is_root())

    def is_double(self):
        return True

    def as_double(self):
        return self


class RemoteDoubleAttributeType(RemoteAttributeType):

    def get_value_type(self):
        return ValueType.DOUBLE

    def as_remote(self, transaction):
        return RemoteDoubleAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: float):
        return self._put_internal(concept_proto_builder.double_attribute_value(value))

    def get(self, value: float):
        return self._get_internal(concept_proto_builder.double_attribute_value(value))

    def is_double(self):
        return True

    def as_double(self):
        return self


class StringAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return StringAttributeType(type_proto.label, type_proto.root)

    def get_value_type(self):
        return ValueType.STRING

    def as_remote(self, transaction):
        return RemoteStringAttributeType(transaction, self.get_label(), self.is_root())

    def is_string(self):
        return True

    def as_string(self):
        return self


class RemoteStringAttributeType(RemoteAttributeType):

    def get_value_type(self):
        return ValueType.STRING

    def as_remote(self, transaction):
        return RemoteStringAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: str):
        return self._put_internal(concept_proto_builder.string_attribute_value(value))

    def get(self, value: str):
        return self._get_internal(concept_proto_builder.string_attribute_value(value))

    def get_regex(self):
        method = concept_proto.Type.Req()
        get_regex_req = concept_proto.AttributeType.GetRegex.Req()
        method.attribute_type_get_regex_req.CopyFrom(get_regex_req)
        regex = self._execute(method).attribute_type_get_regex_res.regex
        return None if len(regex) == 0 else regex

    def set_regex(self, regex: Optional[str]):
        if regex is None:
            regex = ""
        method = concept_proto.Type.Req()
        set_regex_req = concept_proto.AttributeType.SetRegex.Req()
        set_regex_req.regex = regex
        method.attribute_type_set_regex_req.CopyFrom(set_regex_req)
        self._execute(method)

    def is_string(self):
        return True

    def as_string(self):
        return self


class DateTimeAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return DateTimeAttributeType(type_proto.label, type_proto.root)

    def get_value_type(self):
        return ValueType.DATETIME

    def as_remote(self, transaction):
        return RemoteDateTimeAttributeType(transaction, self.get_label(), self.is_root())

    def is_datetime(self):
        return True

    def as_datetime(self):
        return self


class RemoteDateTimeAttributeType(RemoteAttributeType):

    def get_value_type(self):
        return ValueType.DATETIME

    def as_remote(self, transaction):
        return RemoteDateTimeAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: datetime):
        return self._put_internal(concept_proto_builder.datetime_attribute_value(value))

    def get(self, value: datetime):
        return self._get_internal(concept_proto_builder.datetime_attribute_value(value))

    def is_datetime(self):
        return True

    def as_datetime(self):
        return self
