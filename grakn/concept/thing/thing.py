import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException
from grakn.concept.concept import Concept, RemoteConcept


class Thing(Concept):

    def __init__(self, iid: str):
        if not iid:
            raise GraknClientException("IID must be a non-empty string.")
        self._iid = iid

    def as_remote(self, transaction):
        return RemoteThing(transaction, self._iid)

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        # TODO: implement this properly
        return Thing(thing_proto.iid.hex())

    def __str__(self):
        return type(self).__name__ + "[iid:" + str(self._iid) + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._iid == other._iid


class RemoteThing(RemoteConcept):

    def __init__(self, transaction, iid: str):
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        if not iid:
            raise GraknClientException("IID must be set.")
        self._transaction = transaction
        self._iid = iid

    # def get_supertypes(self):
    #     method = concept_proto.Type.Req()
    #     method.type_get_supertypes_req.CopyFrom(concept_proto.Type.GetSupertypes.Req())
    #     return self._type_stream(method, lambda res: res.type_get_supertypes_res.type)
    #
    # def _type_stream(self, method, type_list_getter):
    #     method.label = self._label
    #     request = transaction_proto.Transaction.Req()
    #     request.type_req.CopyFrom(method)
    #     return self._transaction._stream(request, lambda res: type_list_getter(res.type_res))

    def __str__(self):
        return type(self).__name__ + "[iid:" + str(self._iid) + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._iid == other._iid
