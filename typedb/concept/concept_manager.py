#
# Copyright (C) 2022 Vaticle
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
from typing import Optional

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.concept.concept import ValueType
from typedb.api.concept.concept_manager import ConceptManager
from typedb.common.exception import TypeDBClientException, MISSING_LABEL, MISSING_IID
# from typedb.api.connection.transaction import Transaction
from typedb.common.rpc.request_builder import concept_manager_put_entity_type_req, \
    concept_manager_put_relation_type_req, \
    concept_manager_put_attribute_type_req, concept_manager_get_thing_type_req, concept_manager_get_thing_req
from typedb.concept.proto import concept_proto_reader
from typedb.concept.thing.attribute import _Attribute
from typedb.concept.thing.entity import _Entity
from typedb.concept.thing.relation import _Relation
from typedb.concept.type.attribute_type import _AttributeType
from typedb.concept.type.entity_type import _EntityType
from typedb.concept.type.relation_type import _RelationType

from typedb.typedb_client_python import Transaction, concepts_get_entity_type, concepts_get_relation_type, concepts_get_attribute_type, \
    concepts_put_entity_type, concepts_put_relation_type, concepts_put_attribute_type, concepts_get_entity, concepts_get_relation, concepts_get_attribute, concepts_get_schema_exceptions


class _ConceptManager(ConceptManager):

    def __init__(self, transaction: Transaction):
        self._transaction = transaction

    def native_transaction(self):
        return self._transaction

    # def get_root_thing_type(self):
    #     return self.get_thing_type("thing")

    def get_root_entity_type(self) -> _EntityType:
        return self.get_entity_type("entity")

    def get_root_relation_type(self) -> _RelationType:
        return self.get_relation_type("relation")

    def get_root_attribute_type(self) -> _AttributeType:
        return self.get_attribute_type("attribute")

    def get_entity_type(self, label: str) -> Optional[_EntityType]:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        if _type := concepts_get_entity_type(self._transaction, label):
            return _EntityType.of(_type)
        return None
        # return _type if _type and _type.is_entity_type() else None

    def get_relation_type(self, label: str) -> Optional[_RelationType]:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        if _type := concepts_get_relation_type(self._transaction, label):
            return _RelationType.of(_type)
        return None

    def get_attribute_type(self, label: str) -> Optional[_AttributeType]:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        if _type := concepts_get_attribute_type(self._transaction, label):
            return _AttributeType.of(_type)
        return None

    def put_entity_type(self, label: str) -> _EntityType:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        return _EntityType.of(concepts_put_entity_type(self._transaction, label))

    def put_relation_type(self, label: str) -> _RelationType:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        return _RelationType.of(concepts_put_relation_type(self._transaction, label))

    def put_attribute_type(self, label: str, value_type: ValueType) -> _AttributeType:
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        return _AttributeType.of(concepts_put_attribute_type(self._transaction, label, value_type))

    def get_entity(self, iid: str) -> Optional[_Entity]:
        if not iid:
            raise TypeDBClientException.of(MISSING_IID)
        if concept := concepts_get_entity(self._transaction, iid):
            return _Entity.of(concept)
        return None

    def get_relation(self, iid: str) -> Optional[_Relation]:
        if not iid:
            raise TypeDBClientException.of(MISSING_IID)
        if concept := concepts_get_relation(self._transaction, iid):
            return _Relation.of(concept)
        return None

    def get_attribute(self, iid: str) -> Optional[_Attribute]:
        if not iid:
            raise TypeDBClientException.of(MISSING_IID)
        if concept := concepts_get_attribute(self._transaction, iid):
            return _Attribute.of(concept)
        return None

    # def get_thing_type(self, label: str):
    #     res = self.execute(concept_manager_get_thing_type_req(label))
    #     return concept_proto_reader.thing_type(res.get_thing_type_res.thing_type) if res.get_thing_type_res.WhichOneof("res") == "thing_type" else None

    # def get_thing(self, iid: str):
    #     res = self.execute(concept_manager_get_thing_req(iid))
    #     return concept_proto_reader.thing(res.get_thing_res.thing) if res.get_thing_res.WhichOneof("res") == "thing" else None

    # def execute(self, req: transaction_proto.Transaction.Req):
    #     pass
    #     # return self._transaction_ext.execute(req).concept_manager_res
