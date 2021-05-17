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

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.common.exception import TypeDBClientException, BAD_ENCODING, BAD_VALUE_TYPE
from typedb.common.label import Label
from typedb.concept.thing.attribute import _BooleanAttribute, _LongAttribute, _DoubleAttribute, _StringAttribute, \
    _DateTimeAttribute
from typedb.concept.thing.entity import _Entity
from typedb.concept.thing.relation import _Relation
from typedb.concept.type.attribute_type import _BooleanAttributeType, _LongAttributeType, _DoubleAttributeType, \
    _StringAttributeType, _DateTimeAttributeType, _AttributeType
from typedb.concept.type.entity_type import _EntityType
from typedb.concept.type.relation_type import _RelationType
from typedb.concept.type.role_type import _RoleType
from typedb.concept.type.thing_type import _ThingType


def iid(proto_iid: bytes):
    return "0x" + proto_iid.hex()


def concept(proto_concept: concept_proto.Concept):
    return thing(proto_concept.thing) if proto_concept.HasField("thing") else type_(proto_concept.type)


def thing(proto_thing: concept_proto.Thing):
    if proto_thing.type.encoding == concept_proto.Type.Encoding.Value("ENTITY_TYPE"):
        return _Entity.of(proto_thing)
    elif proto_thing.type.encoding == concept_proto.Type.Encoding.Value("RELATION_TYPE"):
        return _Relation.of(proto_thing)
    elif proto_thing.type.encoding == concept_proto.Type.Encoding.Value("ATTRIBUTE_TYPE"):
        return attribute(proto_thing)
    else:
        raise TypeDBClientException.of(BAD_ENCODING, proto_thing.type.encoding)


def attribute(proto_thing: concept_proto.Thing):
    if proto_thing.type.value_type == concept_proto.AttributeType.ValueType.Value("BOOLEAN"):
        return _BooleanAttribute.of(proto_thing)
    elif proto_thing.type.value_type == concept_proto.AttributeType.ValueType.Value("LONG"):
        return _LongAttribute.of(proto_thing)
    elif proto_thing.type.value_type == concept_proto.AttributeType.ValueType.Value("DOUBLE"):
        return _DoubleAttribute.of(proto_thing)
    elif proto_thing.type.value_type == concept_proto.AttributeType.ValueType.Value("STRING"):
        return _StringAttribute.of(proto_thing)
    elif proto_thing.type.value_type == concept_proto.AttributeType.ValueType.Value("DATETIME"):
        return _DateTimeAttribute.of(proto_thing)
    else:
        raise TypeDBClientException.of(BAD_VALUE_TYPE, proto_thing.type.value_type)


def type_(proto_type: concept_proto.Type):
    if proto_type.encoding == concept_proto.Type.Encoding.Value("ROLE_TYPE"):
        return _RoleType.of(proto_type)
    else:
        return thing_type(proto_type)


def thing_type(proto_type: concept_proto.Type):
    if proto_type.encoding == concept_proto.Type.Encoding.Value("ENTITY_TYPE"):
        return _EntityType.of(proto_type)
    elif proto_type.encoding == concept_proto.Type.Encoding.Value("RELATION_TYPE"):
        return _RelationType.of(proto_type)
    elif proto_type.encoding == concept_proto.Type.Encoding.Value("ATTRIBUTE_TYPE"):
        return attribute_type(proto_type)
    elif proto_type.encoding == concept_proto.Type.Encoding.Value("THING_TYPE"):
        return _ThingType(Label.of(proto_type.label), proto_type.root)
    else:
        raise TypeDBClientException.of(BAD_ENCODING, proto_type.encoding)


def attribute_type(proto_type: concept_proto.Type):
    if proto_type.value_type == concept_proto.AttributeType.ValueType.Value("BOOLEAN"):
        return _BooleanAttributeType.of(proto_type)
    elif proto_type.value_type == concept_proto.AttributeType.ValueType.Value("LONG"):
        return _LongAttributeType.of(proto_type)
    elif proto_type.value_type == concept_proto.AttributeType.ValueType.Value("DOUBLE"):
        return _DoubleAttributeType.of(proto_type)
    elif proto_type.value_type == concept_proto.AttributeType.ValueType.Value("STRING"):
        return _StringAttributeType.of(proto_type)
    elif proto_type.value_type == concept_proto.AttributeType.ValueType.Value("DATETIME"):
        return _DateTimeAttributeType.of(proto_type)
    elif proto_type.value_type == concept_proto.AttributeType.ValueType.Value("OBJECT"):
        return _AttributeType(Label.of(proto_type.label), proto_type.root)
    else:
        raise TypeDBClientException.of(BAD_VALUE_TYPE, proto_type.value_type)
