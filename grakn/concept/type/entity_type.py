import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept import proto_reader
from grakn.concept.type.thing_type import ThingType, RemoteThingType


class EntityType(ThingType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return EntityType(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteEntityType(transaction, self._label, self._is_root)


class RemoteEntityType(RemoteThingType):

    def create(self):
        method = concept_proto.Type.Req()
        create_req = concept_proto.EntityType.Create.Req()
        method.entity_type_create_req.CopyFrom(create_req)
        return proto_reader.thing(self._execute(method).entity_type_create_res.entity)
