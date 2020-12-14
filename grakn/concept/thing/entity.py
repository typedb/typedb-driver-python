import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.concept.thing.thing import Thing, RemoteThing


class Entity(Thing):

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        return Entity(thing_proto.iid.hex())

    def as_remote(self, transaction):
        return RemoteEntity(transaction, self._iid)

    def is_entity(self):
        return True


class RemoteEntity(RemoteThing):

    def as_remote(self, transaction):
        return RemoteEntity(transaction, self._iid)

    def is_entity(self):
        return True
