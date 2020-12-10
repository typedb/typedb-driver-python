import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.type.thing_type import ThingType, RemoteThingType


class AttributeType(ThingType):

    def as_remote(self, transaction):
        return RemoteAttributeType(transaction, self._label, self._is_root)


class RemoteAttributeType(RemoteThingType):
    pass


class BooleanAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return BooleanAttributeType(type_proto.label, type_proto.root)


class RemoteBooleanAttributeType(RemoteAttributeType):
    pass


class LongAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return LongAttributeType(type_proto.label, type_proto.root)


class RemoteLongAttributeType(RemoteAttributeType):
    pass


class DoubleAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return DoubleAttributeType(type_proto.label, type_proto.root)


class RemoteDoubleAttributeType(RemoteAttributeType):
    pass


class StringAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return StringAttributeType(type_proto.label, type_proto.root)


class RemoteStringAttributeType(RemoteAttributeType):
    pass


class DateTimeAttributeType(AttributeType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return DateTimeAttributeType(type_proto.label, type_proto.root)


class RemoteDateTimeAttributeType(RemoteAttributeType):
    pass
