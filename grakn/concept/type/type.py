import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException
from grakn.concept.concept import Concept, RemoteConcept
from grakn.concept.thing.thing import Thing


class Type(Concept):

    def __init__(self, label, is_root):
        if not label:
            raise GraknClientException("Label must be a non-empty string.")
        self._label = label
        self._is_root = is_root

    @staticmethod
    def of(type_proto):
        # TODO: implement this properly
        return Type(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteType(transaction, self._label, self._is_root)


class RemoteType(RemoteConcept):

    def __init__(self, transaction, label, is_root):
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        if not label:
            raise GraknClientException("Label must be a non-empty string.")
        self._transaction = transaction
        self._label = label
        self._is_root = is_root

    def get_supertypes(self):
        method = concept_proto.Type.Req()
        method.type_get_supertypes_req.CopyFrom(concept_proto.Type.GetSupertypes.Req())
        return self._type_stream(method, lambda res: res.type_get_supertypes_res.type)

    def _type_stream(self, method, type_list_getter):
        method.label = self._label
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return self._transaction._stream(request, lambda res: type_list_getter(res.type_res))

    def _thing_stream(self, method, thing_list_getter):
        method.label = self._label
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return self._transaction._stream(request, lambda res: thing_list_getter(res.type_res))

    def _execute(self, method):
        method.label = self._label
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return self._transaction._execute(request).type_res

    # TODO: this does NOT belong here!
    def create(self):
        method = concept_proto.Type.Req()
        create_req = concept_proto.EntityType.Create.Req()
        method.entity_type_create_req.CopyFrom(create_req)
        return Thing.of(self._execute(method).entity_type_create_res.entity)

    # TODO: this also doesn't belong here!
    def get_instances(self):
        method = concept_proto.Type.Req()
        get_instances_req = concept_proto.ThingType.GetInstances.Req()
        method.thing_type_get_instances_req.CopyFrom(get_instances_req)
        return self._thing_stream(method, lambda res: res.thing_type_get_instances_res.thing)
