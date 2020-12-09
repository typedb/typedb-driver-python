import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.type.type import Type


class ConceptManager(object):

    def __init__(self, transaction):
        self._transaction = transaction

    def put_entity_type(self, label):
        req = concept_proto.ConceptManager.Req()
        put_entity_type_req = concept_proto.ConceptManager.PutEntityType.Req()
        put_entity_type_req.label = label
        req.put_entity_type_req.CopyFrom(put_entity_type_req)
        res = self._execute(req)
        return Type.of(res.put_entity_type_res.entity_type)

    def get_entity_type(self, label):
        pass

    def get_type(self, label):
        req = concept_proto.ConceptManager.Req()
        get_type_req = concept_proto.ConceptManager.GetType.Req()
        get_type_req.label = label
        req.get_type_req.CopyFrom(get_type_req)

        response = self._execute(req)
        # TODO: if  .... .getResCase == ... TYPE
        return Type.of(response.get_type_res.type)

    def _execute(self, request):
        req = transaction_proto.Transaction.Req()
        req.concept_manager_req.CopyFrom(request)
        return self._transaction._execute(req).concept_manager_res
