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
from typing import List, Optional

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.thing_type import ThingType
from typedb.api.concept.type.type import Type
from typedb.common.exception import TypeDBClientException, BAD_ENCODING
from typedb.common.rpc.request_builder import proto_role_type, proto_thing_type, byte_string


def thing(thing_):
    proto_thing = concept_proto.Thing()
    proto_thing.iid = byte_string(thing_.get_iid())
    return proto_thing


def thing_type(tt: Optional[ThingType]):
    return proto_thing_type(tt.get_label(), encoding(tt)) if tt else None


def role_type(rt: Optional[RoleType]):
    return proto_role_type(rt.get_label(), encoding(rt)) if rt else None


def types(ts: Optional[List[Type]]):
    return map(lambda t: thing_type(t) if t.is_thing_type() else role_type(t), ts) if ts else None


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


def encoding(_type: Type):
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
        raise TypeDBClientException.of(BAD_ENCODING, _type)
