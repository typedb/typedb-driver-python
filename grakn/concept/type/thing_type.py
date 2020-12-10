import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.type.type import Type, RemoteType


class ThingType(Type):

    def as_remote(self, transaction):
        return RemoteThingType(transaction, self._label, self._is_root)


class RemoteThingType(RemoteType):

    def get_instances(self):
        method = concept_proto.Type.Req()
        get_instances_req = concept_proto.ThingType.GetInstances.Req()
        method.thing_type_get_instances_req.CopyFrom(get_instances_req)
        return self._thing_stream(method, lambda res: res.thing_type_get_instances_res.thing)
