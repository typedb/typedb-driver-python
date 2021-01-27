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
from typing import List

import grakn_protocol.protobuf.concept_pb2 as concept_proto

from grakn.common.exception import GraknClientException
from grakn.concept.type.value_type import ValueType


def iid(iid_: str):
    return bytes.fromhex(iid_.lstrip("0x"))


def thing(thing_):
    proto_thing = concept_proto.Thing()
    proto_thing.iid = iid(thing_.get_iid())
    proto_thing.encoding = thing_encoding(thing_)
    return proto_thing


def type_(_type):
    proto_type = concept_proto.Type()
    proto_type.label = _type.get_label()
    proto_type.encoding = type_encoding(_type)

    if _type.is_role_type():
        proto_type.scope = _type.get_scope()

    return proto_type


def types(types_: List):
    return map(lambda _type: type_(_type), types_)


def boolean_attribute_value(value: bool):
    value_proto = concept_proto.Attribute.Value()
    value_proto.boolean = value
    return value_proto


def long_attribute_value(value: int):
    value_proto = concept_proto.Attribute.Value()
    value_proto.long = value
    return value_proto


def double_attribute_value(value: float):
    value_proto = concept_proto.Attribute.Value()
    value_proto.double = value
    return value_proto


def string_attribute_value(value: str):
    value_proto = concept_proto.Attribute.Value()
    value_proto.string = value
    return value_proto


def datetime_attribute_value(value: datetime):
    value_proto = concept_proto.Attribute.Value()
    value_proto.date_time = int((value - datetime(1970, 1, 1)).total_seconds() * 1000)
    return value_proto


def value_type(value_type_: ValueType):
    if value_type_ == ValueType.BOOLEAN:
        return concept_proto.AttributeType.ValueType.Value("BOOLEAN")
    elif value_type_ == ValueType.LONG:
        return concept_proto.AttributeType.ValueType.Value("LONG")
    elif value_type_ == ValueType.DOUBLE:
        return concept_proto.AttributeType.ValueType.Value("DOUBLE")
    elif value_type_ == ValueType.STRING:
        return concept_proto.AttributeType.ValueType.Value("STRING")
    elif value_type_ == ValueType.DATETIME:
        return concept_proto.AttributeType.ValueType.Value("DATETIME")
    elif value_type_ == ValueType.OBJECT:
        return concept_proto.AttributeType.ValueType.Value("OBJECT")
    else:
        raise GraknClientException("Unrecognised value type: " + str(value_type_))


def thing_encoding(thing_):
    if thing_.is_entity():
        return concept_proto.Thing.Encoding.Value("ENTITY")
    elif thing_.is_relation():
        return concept_proto.Thing.Encoding.Value("RELATION")
    elif thing_.is_attribute():
        return concept_proto.Thing.Encoding.Value("ATTRIBUTE")
    else:
        raise GraknClientException("Unrecognised thing encoding: " + str(thing_))


def type_encoding(_type):
    if _type.is_entity_type():
        return concept_proto.Type.Encoding.Value("ENTITY_TYPE")
    elif _type.is_relation_type():
        return concept_proto.Type.Encoding.Value("RELATION_TYPE")
    elif _type.is_attribute_type():
        return concept_proto.Type.Encoding.Value("ATTRIBUTE_TYPE")
    elif _type.is_role_type():
        return concept_proto.Type.Encoding.Value("ROLE_TYPE")
    elif _type.is_thing_type():
        return concept_proto.Type.Encoding.Value("THING_TYPE")
    else:
        raise GraknClientException("Unrecognised type encoding: " + str(_type))
