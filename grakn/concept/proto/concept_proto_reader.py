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

import grakn_protocol.protobuf.concept_pb2 as concept_proto

from grakn.common.exception import GraknClientException
from grakn.concept.answer.concept_map import ConceptMap
from grakn.concept.thing.attribute import BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, \
    DateTimeAttribute
from grakn.concept.thing.entity import Entity
from grakn.concept.thing.relation import Relation
from grakn.concept.type.attribute_type import BooleanAttributeType, LongAttributeType, DoubleAttributeType, \
    StringAttributeType, DateTimeAttributeType, AttributeType
from grakn.concept.type.entity_type import EntityType
from grakn.concept.type.relation_type import RelationType
from grakn.concept.type.role_type import RoleType
from grakn.concept.type.thing_type import ThingType


def iid(iid_proto: bytes):
    return "0x" + iid_proto.hex()


def concept(con_proto: concept_proto.Concept):
    if con_proto.HasField(ConceptMap._THING):
        concept = thing(con_proto.thing)
    else:
        concept = type_(con_proto.type)
    return concept


def thing(thing_proto: concept_proto.Thing):
    if thing_proto.encoding == concept_proto.Thing.Encoding.Value("ENTITY"):
        return Entity._of(thing_proto)
    elif thing_proto.encoding == concept_proto.Thing.Encoding.Value("RELATION"):
        return Relation._of(thing_proto)
    elif thing_proto.encoding == concept_proto.Thing.Encoding.Value("ATTRIBUTE"):
        return attribute(thing_proto)
    else:
        raise GraknClientException("The encoding " + thing_proto.encoding + " was not recognised.")


def attribute(thing_proto: concept_proto.Thing):
    if thing_proto.value_type == concept_proto.AttributeType.ValueType.Value("BOOLEAN"):
        return BooleanAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.Value("LONG"):
        return LongAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.Value("DOUBLE"):
        return DoubleAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.Value("STRING"):
        return StringAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.Value("DATETIME"):
        return DateTimeAttribute._of(thing_proto)
    else:
        raise GraknClientException("The value type " + str(thing_proto.value_type) + " was not recognised.")


def type_(type_proto: concept_proto.Type):
    if type_proto.encoding == concept_proto.Type.Encoding.Value("ROLE_TYPE"):
        return RoleType._of(type_proto)
    else:
        return thing_type(type_proto)


def thing_type(type_proto: concept_proto.Type):
    if type_proto.encoding == concept_proto.Type.Encoding.Value("ENTITY_TYPE"):
        return EntityType._of(type_proto)
    elif type_proto.encoding == concept_proto.Type.Encoding.Value("RELATION_TYPE"):
        return RelationType._of(type_proto)
    elif type_proto.encoding == concept_proto.Type.Encoding.Value("ATTRIBUTE_TYPE"):
        return attribute_type(type_proto)
    elif type_proto.encoding == concept_proto.Type.Encoding.Value("THING_TYPE"):
        return ThingType(type_proto.label, type_proto.root)
    else:
        raise GraknClientException("The encoding " + str(type_proto.encoding) + " was not recognised.")


def attribute_type(type_proto: concept_proto.Type):
    if type_proto.value_type == concept_proto.AttributeType.ValueType.Value("BOOLEAN"):
        return BooleanAttributeType._of(type_proto)
    elif type_proto.value_type == concept_proto.AttributeType.ValueType.Value("LONG"):
        return LongAttributeType._of(type_proto)
    elif type_proto.value_type == concept_proto.AttributeType.ValueType.Value("DOUBLE"):
        return DoubleAttributeType._of(type_proto)
    elif type_proto.value_type == concept_proto.AttributeType.ValueType.Value("STRING"):
        return StringAttributeType._of(type_proto)
    elif type_proto.value_type == concept_proto.AttributeType.ValueType.Value("DATETIME"):
        return DateTimeAttributeType._of(type_proto)
    elif type_proto.value_type == concept_proto.AttributeType.ValueType.Value("OBJECT"):
        return AttributeType(type_proto.label, type_proto.root)
    else:
        raise GraknClientException("The value type " + str(type_proto.value_type) + " was not recognised.")
