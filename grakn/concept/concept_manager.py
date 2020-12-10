import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept import proto_reader
from grakn.concept.type.entity_type import EntityType
from grakn.concept.type.relation_type import RelationType


class ConceptManager(object):

    def __init__(self, transaction):
        self._transaction = transaction

    def get_root_thing_type(self):
        return self.get_type("thing")

    def get_root_entity_type(self):
        return self.get_entity_type("entity")

    def get_root_relation_type(self):
        return self.get_relation_type("relation")

    def get_root_attribute_type(self):
        return self.get_attribute_type("attribute")

    def put_entity_type(self, label: str):
        req = concept_proto.ConceptManager.Req()
        put_entity_type_req = concept_proto.ConceptManager.PutEntityType.Req()
        put_entity_type_req.label = label
        req.put_entity_type_req.CopyFrom(put_entity_type_req)
        res = self._execute(req)
        return EntityType._of(res.put_entity_type_res.entity_type)

    def get_entity_type(self, label: str):
        _type = self.get_type(label)
        return _type if _type.is_entity_type() else None

    def put_relation_type(self, label: str):
        req = concept_proto.ConceptManager.Req()
        put_relation_type_req = concept_proto.ConceptManager.PutRelationType.Req()
        put_relation_type_req.label = label
        req.put_relation_type_req.CopyFrom(put_relation_type_req)
        res = self._execute(req)
        return RelationType._of(res.put_relation_type_res.relation_type)

    def get_relation_type(self, label: str):
        _type = self.get_type(label)
        return _type if _type.is_relation_type() else None

    def put_attribute_type(self, label: str):
        req = concept_proto.ConceptManager.Req()
        put_attribute_type_req = concept_proto.ConceptManager.PutAttributeType.Req()
        put_attribute_type_req.label = label
        req.put_attribute_type_req.CopyFrom(put_attribute_type_req)
        res = self._execute(req)
        return proto_reader.attribute_type(res.put_attribute_type_res.attribute_type)

    def get_attribute_type(self, label: str):
        _type = self.get_type(label)
        return _type if _type.is_attribute_type() else None

    def get_thing(self, iid: str):
        req = concept_proto.ConceptManager.Req()
        get_thing_req = concept_proto.ConceptManager.GetThing.Req()
        get_thing_req.iid = iid
        req.get_thing_req.CopyFrom(get_thing_req)

        response = self._execute(req)
        return proto_reader.thing(response.get_thing_res.thing) if response.get_thing_res.WhichOneof("res") == "thing" else None

    def get_type(self, label: str):
        req = concept_proto.ConceptManager.Req()
        get_type_req = concept_proto.ConceptManager.GetType.Req()
        get_type_req.label = label
        req.get_type_req.CopyFrom(get_type_req)

        response = self._execute(req)
        return proto_reader.type_(response.get_type_res.type) if response.get_type_res.WhichOneof("res") == "type" else None

    def _execute(self, request: concept_proto.ConceptManager.Req):
        req = transaction_proto.Transaction.Req()
        req.concept_manager_req.CopyFrom(request)
        return self._transaction._execute(req).concept_manager_res
