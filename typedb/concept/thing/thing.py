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
from abc import ABC
from typing import List, Union, TYPE_CHECKING

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.concept.thing.attribute import Attribute
from typedb.api.concept.thing.thing import Thing, RemoteThing
from typedb.common.exception import TypeDBClientException, MISSING_IID, MISSING_TRANSACTION, GET_HAS_WITH_MULTIPLE_FILTERS
from typedb.common.rpc.request_builder import thing_get_has_req, thing_get_relations_req, \
    thing_get_playing_req, thing_set_has_req, thing_unset_has_req, thing_delete_req
from typedb.concept.concept import _Concept, _RemoteConcept
from typedb.concept.proto import concept_proto_reader, concept_proto_builder
from typedb.concept.type.role_type import _RoleType

if TYPE_CHECKING:
    from typedb.api.connection.transaction import _TypeDBTransactionExtended, TypeDBTransaction


class _Thing(Thing, _Concept, ABC):

    def __init__(self, iid: str, is_inferred: bool):
        if not iid:
            raise TypeDBClientException.of(MISSING_IID)
        self._iid = iid
        self._is_inferred = is_inferred

    def get_iid(self):
        return self._iid

    def is_inferred(self) -> bool:
        return self._is_inferred

    def as_thing(self) -> "Thing":
        return self

    def __str__(self):
        return "%s[%s:%s]" % (type(self).__name__, self.get_type().get_label(), self.get_iid())

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_iid() == other.get_iid()

    def __hash__(self):
        return hash(self._iid)


class _RemoteThing(_RemoteConcept, RemoteThing, ABC):

    def __init__(self, transaction: Union["_TypeDBTransactionExtended", "TypeDBTransaction"], iid: str, is_inferred: bool):
        if not transaction:
            raise TypeDBClientException.of(MISSING_TRANSACTION)
        if not iid:
            raise TypeDBClientException.of(MISSING_IID)
        self._transaction_ext = transaction
        self._iid = iid
        self._is_inferred = is_inferred
        self._hash = hash((self._transaction_ext, iid))

    def get_iid(self):
        return self._iid

    def is_inferred(self) -> bool:
        return self._is_inferred

    def as_thing(self) -> "RemoteThing":
        return self

    def get_has(self, attribute_type=None, attribute_types: List = None, only_key=False):
        if [bool(attribute_type), bool(attribute_types), only_key].count(True) > 1:
            raise TypeDBClientException.of(GET_HAS_WITH_MULTIPLE_FILTERS)
        if attribute_type:
            attribute_types = [attribute_type]
        return (concept_proto_reader.attribute(a) for rp in self.stream(thing_get_has_req(self.get_iid(), concept_proto_builder.types(attribute_types), only_key))
                for a in rp.thing_get_has_res_part.attributes)

    def get_relations(self, role_types: list = None):
        if not role_types:
            role_types = []
        return (concept_proto_reader.thing(r) for rp in self.stream(thing_get_relations_req(self.get_iid(), concept_proto_builder.types(role_types)))
                for r in rp.thing_get_relations_res_part.relations)

    def get_playing(self):
        return (_RoleType.of(rt) for rp in self.stream(thing_get_playing_req(self.get_iid()))
                for rt in rp.thing_get_playing_res_part.role_types)

    def set_has(self, attribute: Attribute):
        self.execute(thing_set_has_req(self.get_iid(), concept_proto_builder.thing(attribute)))

    def unset_has(self, attribute: Attribute):
        self.execute(thing_unset_has_req(self.get_iid(), concept_proto_builder.thing(attribute)))

    def delete(self):
        self.execute(thing_delete_req(self.get_iid()))

    def is_deleted(self):
        return not self._transaction_ext.concepts().get_thing(self.get_iid())

    def execute(self, request: transaction_proto.Transaction.Req):
        return self._transaction_ext.execute(request).thing_res

    def stream(self, request: transaction_proto.Transaction.Req):
        return (rp.thing_res_part for rp in self._transaction_ext.stream(request))

    def __str__(self):
        return type(self).__name__ + "[iid:" + str(self._iid) + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._transaction_ext is other._transaction_ext and self._iid == other._iid

    def __hash__(self):
        return self._hash
