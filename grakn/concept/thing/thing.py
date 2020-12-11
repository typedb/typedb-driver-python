from typing import Callable, List

import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException
from grakn.concept import proto_reader
from grakn.concept.concept import Concept, RemoteConcept


class Thing(Concept):

    def __init__(self, iid: str):
        if not iid:
            raise GraknClientException("IID must be a non-empty string.")
        self._iid = iid

    def as_remote(self, transaction):
        return RemoteThing(transaction, self._iid)

    def get_iid(self):
        return self._iid

    def is_thing(self):
        return True

    def is_entity(self):
        return False

    def is_attribute(self):
        return False

    def is_relation(self):
        return False

    def __str__(self):
        return type(self).__name__ + "[iid:" + self.get_iid() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_iid() == other.get_iid()


class RemoteThing(RemoteConcept):

    def __init__(self, transaction, iid: str):
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        if not iid:
            raise GraknClientException("IID must be set.")
        self._transaction = transaction
        self._iid = iid

    def get_iid(self):
        return self._iid

    def get_type(self):
        method = concept_proto.Thing.Req()
        method.thing_get_type_req.CopyFrom(concept_proto.Thing.GetType.Req())
        return proto_reader.type_(self._execute(method).thing_get_type_res.thing_type)

    def is_inferred(self):
        req = concept_proto.Thing.Req()
        req.thing_is_inferred_req.CopyFrom(concept_proto.Thing.IsInferred.Req())
        return self._execute(req).thing_is_inferred_res.inferred

    def get_has(self, attribute_type=None, attribute_types: List = None, only_key=False):
        if [bool(attribute_type), bool(attribute_types), only_key].count(True) > 1:
            raise GraknClientException("Only one filter can be applied at a time to get_has."
                                       "The possible filters are: [attribute_type, attribute_types, only_key]")
        if attribute_type:
            attribute_types = [attribute_type]
        method = concept_proto.Thing.Req()
        get_has_req = concept_proto.Thing.GetHas.Req()
        if only_key:
            get_has_req.keys_only = only_key
        elif attribute_types:
            pass  # TODO
            # get_has_req.


    def _thing_stream(self, method: concept_proto.Thing.Req, thing_list_getter: Callable[[concept_proto.Thing.Res], List[concept_proto.Thing]]):
        method.iid = self.get_iid()
        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        return map(lambda thing_proto: proto_reader.thing(thing_proto), self._transaction._stream(request, lambda res: thing_list_getter(res.thing_res)))

    def _type_stream(self, method: concept_proto.Thing.Req, type_list_getter: Callable[[concept_proto.Thing.Res], List[concept_proto.Type]]):
        method.iid = self.get_iid()
        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        return map(lambda type_proto: proto_reader.type_(type_proto), self._transaction._stream(request, lambda res: type_list_getter(res.thing_res)))

    def _execute(self, method: concept_proto.Thing.Req):
        method.iid = self.get_iid()
        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        return self._transaction._execute(request).thing_res

    def __str__(self):
        return type(self).__name__ + "[iid:" + str(self._iid) + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._iid == other._iid
