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

from typing import Callable, List

import grakn_protocol.protobuf.concept_pb2 as concept_proto
import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn.common.exception import GraknClientException
from grakn.concept.proto import concept_proto_builder, concept_proto_reader
from grakn.concept.concept import Concept, RemoteConcept


class Type(Concept):

    def __init__(self, label: str, is_root: bool):
        if not label:
            raise GraknClientException("Label must be a non-empty string.")
        self._label = label
        self._is_root = is_root
        self._hash = hash(label)

    def get_label(self):
        return self._label

    def is_root(self):
        return self._is_root

    def is_type(self):
        return True

    def __str__(self):
        return type(self).__name__ + "[label:" + self.get_label() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash


class RemoteType(RemoteConcept):

    def __init__(self, transaction, label: str, is_root: bool):
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        if not label:
            raise GraknClientException("Label must be a non-empty string.")
        self._transaction = transaction
        self._label = label
        self._is_root = is_root
        self._hash = hash((self._transaction, label))

    def get_label(self):
        return self._label

    def is_root(self):
        return self._is_root

    def set_label(self, label: str):
        req = concept_proto.Type.Req()
        set_label_req = concept_proto.Type.SetLabel.Req()
        set_label_req.label = label
        req.type_set_label_req.CopyFrom(set_label_req)
        self._execute(req)
        self._label = label
        self._hash = hash((self._transaction, label))

    def is_abstract(self):
        req = concept_proto.Type.Req()
        req.type_is_abstract_req.CopyFrom(concept_proto.Type.IsAbstract.Req())
        res = self._execute(req)
        return res.type_is_abstract_res.abstract

    def is_type(self):
        return True

    def set_supertype(self, _type: Type):
        req = concept_proto.Type.Req()
        supertype_req = concept_proto.Type.SetSupertype.Req()
        supertype_req.type.CopyFrom(concept_proto_builder.type_(_type))
        req.type_set_supertype_req.CopyFrom(supertype_req)
        self._execute(req)

    def get_supertype(self):
        req = concept_proto.Type.Req()
        req.type_get_supertype_req.CopyFrom(concept_proto.Type.GetSupertype.Req())
        res = self._execute(req).type_get_supertype_res
        return concept_proto_reader.type_(res.type) if res.WhichOneof("res") == "type" else None

    def get_supertypes(self):
        method = concept_proto.Type.Req()
        method.type_get_supertypes_req.CopyFrom(concept_proto.Type.GetSupertypes.Req())
        return self._type_stream(method, lambda res: res.type_get_supertypes_res.types)

    def get_subtypes(self):
        method = concept_proto.Type.Req()
        method.type_get_subtypes_req.CopyFrom(concept_proto.Type.GetSubtypes.Req())
        return self._type_stream(method, lambda res: res.type_get_subtypes_res.types)

    def delete(self):
        method = concept_proto.Type.Req()
        method.type_delete_req.CopyFrom(concept_proto.Type.Delete.Req())
        self._execute(method)

    def is_deleted(self):
        return not self._transaction.concepts()._get_thing_type(self.get_label())

    def _type_stream(self, method: concept_proto.Type.Req, type_list_getter: Callable[[concept_proto.Type.Res], List[concept_proto.Type]]):
        method.label = self.get_label()
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return map(lambda type_proto: concept_proto_reader.type_(type_proto), self._transaction._stream(request, lambda res: type_list_getter(res.type_res)))

    def _thing_stream(self, method: concept_proto.Type.Req, thing_list_getter: Callable[[concept_proto.Type.Res], List[concept_proto.Thing]]):
        method.label = self.get_label()
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return map(lambda thing_proto: concept_proto_reader.thing(thing_proto), self._transaction._stream(request, lambda res: thing_list_getter(res.type_res)))

    def _execute(self, method: concept_proto.Type.Req):
        method.label = self.get_label()
        request = transaction_proto.Transaction.Req()
        request.type_req.CopyFrom(method)
        return self._transaction._execute(request).type_res

    def __str__(self):
        return type(self).__name__ + "[label:" + self.get_label() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._transaction is other._transaction and self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash
