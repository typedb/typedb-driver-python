import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.common.exception import GraknClientException
from grakn.concept.thing.attribute import BooleanAttribute, LongAttribute, DoubleAttribute, StringAttribute, \
    DateTimeAttribute
from grakn.concept.thing.thing import Thing
from grakn.concept.type.attribute_type import BooleanAttributeType, LongAttributeType, DoubleAttributeType, \
    StringAttributeType, DateTimeAttributeType, AttributeType
from grakn.concept.type.entity_type import EntityType
from grakn.concept.type.relation_type import RelationType
from grakn.concept.type.role_type import RoleType
from grakn.concept.type.thing_type import ThingType


def thing(thing_proto: concept_proto.Thing):
    # TODO: implement this properly
    return Thing._of(thing_proto)


def attribute(thing_proto: concept_proto.Thing):
    if thing_proto.value_type == concept_proto.AttributeType.ValueType.BOOLEAN:
        return BooleanAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.LONG:
        return LongAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.DOUBLE:
        return DoubleAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.STRING:
        return StringAttribute._of(thing_proto)
    elif thing_proto.value_type == concept_proto.AttributeType.ValueType.DATETIME:
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
