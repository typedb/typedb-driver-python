import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.type.thing_type import ThingType, RemoteThingType


class RelationType(ThingType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return RelationType(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteRelationType(transaction, self._label, self._is_root)


class RemoteRelationType(RemoteThingType):
    pass
