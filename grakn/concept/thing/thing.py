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
from grakn.concept.proto import concept_proto_reader, concept_proto_builder
from grakn.concept.concept import Concept, RemoteConcept


class Thing(Concept):

    def __init__(self, iid: str):
        if not iid:
            raise GraknClientException("IID must be a non-empty string.")
        self._iid = iid
        self._hash = hash(iid)

    def get_iid(self):
        return self._iid

    def is_thing(self):
        return True

    def __str__(self):
        return type(self).__name__ + "[iid:" + self.get_iid() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_iid() == other.get_iid()

    def __hash__(self):
        return self._hash


class RemoteThing(RemoteConcept):

    def __init__(self, transaction, iid: str):
        if not transaction:
            raise GraknClientException("Transaction must be set.")
        if not iid:
            raise GraknClientException("IID must be set.")
        self._transaction = transaction
        self._iid = iid
        self._hash = hash(iid)

    def get_iid(self):
        return self._iid

    def get_type(self):
        method = concept_proto.Thing.Req()
        method.thing_get_type_req.CopyFrom(concept_proto.Thing.GetType.Req())
        return concept_proto_reader.type_(self._execute(method).thing_get_type_res.thing_type)

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
            get_has_req.attribute_types.extend(concept_proto_builder.types(attribute_types))
        method.thing_get_has_req.CopyFrom(get_has_req)
        return self._thing_stream(method, lambda res: res.thing_get_has_res.attributes)

    def get_plays(self):
        req = concept_proto.Thing.Req()
        req.thing_get_plays_req.CopyFrom(concept_proto.Thing.GetPlays.Req())
        return self._type_stream(req, lambda res: res.thing_get_plays_res.role_types)

    def get_relations(self, role_types: list = None):
        if not role_types:
            role_types = []
        method = concept_proto.Thing.Req()
        get_relations_req = concept_proto.Thing.GetRelations.Req()
        get_relations_req.role_types.extend(concept_proto_builder.types(role_types))
        method.thing_get_relations_req.CopyFrom(get_relations_req)
        return self._thing_stream(method, lambda res: res.thing_get_relations_res.relations)

    def set_has(self, attribute):
        method = concept_proto.Thing.Req()
        set_has_req = concept_proto.Thing.SetHas.Req()
        set_has_req.attribute.CopyFrom(concept_proto_builder.thing(attribute))
        method.thing_set_has_req.CopyFrom(set_has_req)
        self._execute(method)

    def unset_has(self, attribute):
        method = concept_proto.Thing.Req()
        unset_has_req = concept_proto.Thing.UnsetHas.Req()
        unset_has_req.attribute.CopyFrom(concept_proto_builder.thing(attribute))
        method.thing_unset_has_req.CopyFrom(unset_has_req)
        self._execute(method)

    def delete(self):
        method = concept_proto.Thing.Req()
        method.thing_delete_req.CopyFrom(concept_proto.Thing.Delete.Req())
        self._execute(method)

    def is_deleted(self):
        return not self._transaction.concepts().get_thing(self.get_iid())

    def is_thing(self):
        return True

    def _thing_stream(self, method: concept_proto.Thing.Req, thing_list_getter: Callable[[concept_proto.Thing.Res], List[concept_proto.Thing]]):
        method.iid = concept_proto_builder.iid(self.get_iid())
        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        return map(lambda thing_proto: concept_proto_reader.thing(thing_proto), self._transaction._stream(request, lambda res: thing_list_getter(res.thing_res)))

    def _type_stream(self, method: concept_proto.Thing.Req, type_list_getter: Callable[[concept_proto.Thing.Res], List[concept_proto.Type]]):
        method.iid = concept_proto_builder.iid(self.get_iid())
        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        return map(lambda type_proto: concept_proto_reader.type_(type_proto), self._transaction._stream(request, lambda res: type_list_getter(res.thing_res)))

    def _execute(self, method: concept_proto.Thing.Req):
        method.iid = concept_proto_builder.iid(self.get_iid())
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
        return self._transaction is other._transaction and self._iid == other._iid

    def __hash__(self):
        return self._hash
