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

from datetime import datetime
from typing import Optional, Iterator

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.type.attribute_type import AttributeType, RemoteAttributeType, BooleanAttributeType, \
    RemoteBooleanAttributeType, LongAttributeType, RemoteLongAttributeType, DoubleAttributeType, \
    RemoteDoubleAttributeType, StringAttributeType, RemoteStringAttributeType, DateTimeAttributeType, \
    RemoteDateTimeAttributeType
from typedb.common.exception import TypeDBClientException, INVALID_CONCEPT_CASTING
from typedb.common.label import Label
from typedb.common.rpc.request_builder import attribute_type_get_owners_req, attribute_type_put_req, \
    attribute_type_get_req, attribute_type_get_regex_req, attribute_type_set_regex_req
from typedb.concept.proto import concept_proto_builder, concept_proto_reader
from typedb.concept.type.thing_type import _ThingType, _RemoteThingType


class _AttributeType(AttributeType, _ThingType):

    ROOT_LABEL = Label.of("attribute")

    def as_remote(self, transaction):
        return _RemoteAttributeType(transaction, self.get_label(), self.is_root())

    def as_attribute_type(self) -> "AttributeType":
        return self

    def as_boolean(self):
        if self.is_root():
            return _BooleanAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, BooleanAttributeType.__name__))

    def as_long(self):
        if self.is_root():
            return _LongAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, LongAttributeType.__name__))

    def as_double(self):
        if self.is_root():
            return _DoubleAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, DoubleAttributeType.__name__))

    def as_string(self):
        if self.is_root():
            return _StringAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, StringAttributeType.__name__))

    def as_datetime(self):
        if self.is_root():
            return _DateTimeAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, DateTimeAttributeType.__name__))

    def __eq__(self, other):
        if other is self:
            return True
        # root "attribute" should always be equal to itself regardless of which value class it holds
        if not other or not isinstance(other, _AttributeType):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return super(_AttributeType, self).__hash__()


class _RemoteAttributeType(_RemoteThingType, RemoteAttributeType):

    ROOT_LABEL = Label.of("attribute")

    def as_remote(self, transaction):
        return _RemoteAttributeType(transaction, self.get_label(), self.is_root())

    def as_attribute_type(self) -> "RemoteAttributeType":
        return self

    def get_subtypes(self) -> Iterator[AttributeType]:
        stream = super(_RemoteAttributeType, self).get_subtypes()
        if self.is_root() and self.get_value_type() is not AttributeType.ValueType.OBJECT:
            return (subtype for subtype in stream if subtype.get_value_type() is self.get_value_type() or subtype.get_label() == self.get_label())
        else:
            return stream

    def get_owners(self, only_key: bool = False):
        return (concept_proto_reader.thing_type(tt) for rp in self.stream(attribute_type_get_owners_req(self.get_label(), only_key))
                for tt in rp.attribute_type_get_owners_res_part.owners)

    def put_internal(self, proto_value: concept_proto.Attribute.Value):
        res = self.execute(attribute_type_put_req(self.get_label(), proto_value)).attribute_type_put_res
        return concept_proto_reader.attribute(res.attribute)

    def get_internal(self, proto_value: concept_proto.Attribute.Value):
        res = self.execute(attribute_type_get_req(self.get_label(), proto_value)).attribute_type_get_res
        return concept_proto_reader.attribute(res.attribute) if res.WhichOneof("res") == "attribute" else None

    def is_attribute_type(self):
        return True

    def as_boolean(self):
        if self.is_root():
            return _BooleanAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, BooleanAttributeType.__name__))

    def as_long(self):
        if self.is_root():
            return _LongAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, LongAttributeType.__name__))

    def as_double(self):
        if self.is_root():
            return _DoubleAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, DoubleAttributeType.__name__))

    def as_string(self):
        if self.is_root():
            return _StringAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, StringAttributeType.__name__))

    def as_datetime(self):
        if self.is_root():
            return _DateTimeAttributeType(self.ROOT_LABEL, is_root=True)
        raise TypeDBClientException.of(INVALID_CONCEPT_CASTING, (self.__class__.__name__, DateTimeAttributeType.__name__))

    def __eq__(self, other):
        if other is self:
            return True
        if not other or not isinstance(_RemoteAttributeType, other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return super(_RemoteAttributeType, self).__hash__()


class _BooleanAttributeType(BooleanAttributeType, _AttributeType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _BooleanAttributeType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteBooleanAttributeType(transaction, self.get_label(), self.is_root())

    def as_boolean(self):
        return self


class _RemoteBooleanAttributeType(_RemoteAttributeType, RemoteBooleanAttributeType):

    def as_remote(self, transaction):
        return _RemoteBooleanAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: bool):
        return self.put_internal(concept_proto_builder.boolean_attribute_value(value))

    def get(self, value: bool):
        return self.get_internal(concept_proto_builder.boolean_attribute_value(value))

    def as_boolean(self):
        return self


class _LongAttributeType(LongAttributeType, _AttributeType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _LongAttributeType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteLongAttributeType(transaction, self.get_label(), self.is_root())

    def as_long(self):
        return self


class _RemoteLongAttributeType(_RemoteAttributeType, RemoteLongAttributeType):

    def as_remote(self, transaction):
        return _RemoteLongAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: int):
        return self.put_internal(concept_proto_builder.long_attribute_value(value))

    def get(self, value: int):
        return self.get_internal(concept_proto_builder.long_attribute_value(value))

    def as_long(self):
        return self


class _DoubleAttributeType(DoubleAttributeType, _AttributeType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _DoubleAttributeType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteDoubleAttributeType(transaction, self.get_label(), self.is_root())

    def as_double(self):
        return self


class _RemoteDoubleAttributeType(_RemoteAttributeType, RemoteDoubleAttributeType):

    def as_remote(self, transaction):
        return _RemoteDoubleAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: float):
        return self.put_internal(concept_proto_builder.double_attribute_value(value))

    def get(self, value: float):
        return self.get_internal(concept_proto_builder.double_attribute_value(value))

    def as_double(self):
        return self


class _StringAttributeType(StringAttributeType, _AttributeType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _StringAttributeType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteStringAttributeType(transaction, self.get_label(), self.is_root())

    def as_string(self):
        return self


class _RemoteStringAttributeType(_RemoteAttributeType, RemoteStringAttributeType):

    def as_remote(self, transaction):
        return _RemoteStringAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: str):
        return self.put_internal(concept_proto_builder.string_attribute_value(value))

    def get(self, value: str):
        return self.get_internal(concept_proto_builder.string_attribute_value(value))

    def get_regex(self):
        res = self.execute(attribute_type_get_regex_req(self.get_label()))
        regex = res.attribute_type_get_regex_res.regex
        return None if len(regex) == 0 else regex

    def set_regex(self, regex: Optional[str]):
        if regex is None:
            regex = ""
        self.execute(attribute_type_set_regex_req(self.get_label(), regex))

    def as_string(self):
        return self


class _DateTimeAttributeType(DateTimeAttributeType, _AttributeType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _DateTimeAttributeType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteDateTimeAttributeType(transaction, self.get_label(), self.is_root())

    def as_datetime(self):
        return self


class _RemoteDateTimeAttributeType(_RemoteAttributeType, RemoteDateTimeAttributeType):

    def as_remote(self, transaction):
        return _RemoteDateTimeAttributeType(transaction, self.get_label(), self.is_root())

    def put(self, value: datetime):
        return self.put_internal(concept_proto_builder.datetime_attribute_value(value))

    def get(self, value: datetime):
        return self.get_internal(concept_proto_builder.datetime_attribute_value(value))

    def as_datetime(self):
        return self
