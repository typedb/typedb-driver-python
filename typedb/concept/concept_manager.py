#
# Copyright (C) 2021 Vaticle
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

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.concept.concept_manager import ConceptManager
from typedb.api.concept.type.attribute_type import AttributeType
from typedb.api.connection.transaction import _TypeDBTransactionExtended
from typedb.common.rpc.request_builder import concept_manager_put_entity_type_req, concept_manager_put_relation_type_req, \
    concept_manager_put_attribute_type_req, concept_manager_get_thing_type_req, concept_manager_get_thing_req
from typedb.concept.proto import concept_proto_reader
from typedb.concept.type.entity_type import _EntityType
from typedb.concept.type.relation_type import _RelationType


class _ConceptManager(ConceptManager):

    def __init__(self, transaction_ext: _TypeDBTransactionExtended):
        self._transaction_ext = transaction_ext

    def get_root_thing_type(self):
        return self.get_thing_type("thing")

    def get_root_entity_type(self):
        return self.get_entity_type("entity")

    def get_root_relation_type(self):
        return self.get_relation_type("relation")

    def get_root_attribute_type(self):
        return self.get_attribute_type("attribute")

    def put_entity_type(self, label: str):
        return _EntityType.of(self.execute(concept_manager_put_entity_type_req(label)).put_entity_type_res.entity_type)

    def get_entity_type(self, label: str):
        _type = self.get_thing_type(label)
        return _type if _type and _type.is_entity_type() else None

    def put_relation_type(self, label: str):
        res = self.execute(concept_manager_put_relation_type_req(label))
        return _RelationType.of(res.put_relation_type_res.relation_type)

    def get_relation_type(self, label: str):
        _type = self.get_thing_type(label)
        return _type if _type and _type.is_relation_type() else None

    def put_attribute_type(self, label: str, value_type: AttributeType.ValueType):
        res = self.execute(concept_manager_put_attribute_type_req(label, value_type.proto()))
        return concept_proto_reader.attribute_type(res.put_attribute_type_res.attribute_type)

    def get_attribute_type(self, label: str):
        _type = self.get_thing_type(label)
        return _type if _type and _type.is_attribute_type() else None

    def get_thing_type(self, label: str):
        res = self.execute(concept_manager_get_thing_type_req(label))
        return concept_proto_reader.thing_type(res.get_thing_type_res.thing_type) if res.get_thing_type_res.WhichOneof("res") == "thing_type" else None

    def get_thing(self, iid: str):
        res = self.execute(concept_manager_get_thing_req(iid))
        return concept_proto_reader.thing(res.get_thing_res.thing) if res.get_thing_res.WhichOneof("res") == "thing" else None

    def execute(self, req: transaction_proto.Transaction.Req):
        return self._transaction_ext.execute(req).concept_manager_res
