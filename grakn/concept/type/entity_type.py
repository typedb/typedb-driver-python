import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.concept.thing.entity import Entity
from grakn.concept.type.thing_type import ThingType, RemoteThingType


class EntityType(ThingType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return EntityType(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteEntityType(transaction, self.get_label(), self.is_root())

    def is_entity_type(self):
        return True


class RemoteEntityType(RemoteThingType):

    def as_remote(self, transaction):
        return RemoteEntityType(transaction, self.get_label(), self.is_root())

    def create(self):
        method = concept_proto.Type.Req()
        create_req = concept_proto.EntityType.Create.Req()
        method.entity_type_create_req.CopyFrom(create_req)
        return Entity._of(self._execute(method).entity_type_create_res.entity)

    def is_entity_type(self):
        return True
