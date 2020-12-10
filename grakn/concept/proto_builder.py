import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.common.exception import GraknClientException


def type_(_type):
    proto_type = concept_proto.Type()
    proto_type.label = _type.get_label()
    proto_type.encoding = type_encoding(_type)

    if _type.is_role_type():
        proto_type.scope = _type.get_scope()

    return proto_type


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
        raise GraknClientException("Unrecognised type encoding")
