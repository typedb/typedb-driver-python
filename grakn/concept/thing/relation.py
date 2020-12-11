import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.concept.thing.thing import Thing, RemoteThing


class Relation(Thing):

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        return Relation(thing_proto.iid.hex())


class RemoteRelation(RemoteThing):
    pass
