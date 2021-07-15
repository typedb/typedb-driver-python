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
from typing import TYPE_CHECKING, Union

import typedb_protocol.common.transaction_pb2 as transaction_proto

from typedb.api.concept.type.type import Type, RemoteType
from typedb.common.exception import TypeDBClientException, MISSING_LABEL, MISSING_TRANSACTION
from typedb.common.label import Label
from typedb.common.rpc.request_builder import type_set_label_req, type_is_abstract_req, type_get_supertype_req, \
    type_get_supertypes_req, type_get_subtypes_req, type_delete_req
from typedb.concept.concept import _Concept, _RemoteConcept
from typedb.concept.proto import concept_proto_reader

if TYPE_CHECKING:
    from typedb.api.connection.transaction import TypeDBTransaction, _TypeDBTransactionExtended


class _Type(Type, _Concept, ABC):

    def __init__(self, label: Label, is_root: bool):
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        self._label = label
        self._is_root = is_root
        self._hash = hash(label)

    def get_label(self):
        return self._label

    def is_root(self):
        return self._is_root

    def as_type(self) -> "Type":
        return self

    def __str__(self):
        return type(self).__name__ + "[label: %s]" % self.get_label()

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash


class _RemoteType(RemoteType, _RemoteConcept, ABC):

    def __init__(self, transaction: Union["_TypeDBTransactionExtended", "TypeDBTransaction"], label: Label, is_root: bool):
        if not transaction:
            raise TypeDBClientException.of(MISSING_TRANSACTION)
        if not label:
            raise TypeDBClientException.of(MISSING_LABEL)
        self._transaction_ext = transaction
        self._label = label
        self._is_root = is_root
        self._hash = hash((self._transaction_ext, label))

    def get_label(self):
        return self._label

    def is_root(self):
        return self._is_root

    def as_type(self) -> "RemoteType":
        return self

    def set_label(self, new_label: str):
        self.execute(type_set_label_req(self.get_label(), new_label))
        self._label = new_label
        self._hash = hash((self._transaction_ext, new_label))

    def is_abstract(self):
        return self.execute(type_is_abstract_req(self.get_label())).type_is_abstract_res.abstract

    def get_supertype(self):
        res = self.execute(type_get_supertype_req(self.get_label())).type_get_supertype_res
        return concept_proto_reader.type_(res.type) if res.WhichOneof("res") == "type" else None

    def get_supertypes(self):
        return (concept_proto_reader.type_(t) for rp in self.stream(type_get_supertypes_req(self.get_label())) for t in rp.type_get_supertypes_res_part.types)

    def get_subtypes(self):
        return (concept_proto_reader.type_(t) for rp in self.stream(type_get_subtypes_req(self.get_label())) for t in rp.type_get_subtypes_res_part.types)

    def delete(self):
        self.execute(type_delete_req(self.get_label()))

    def execute(self, request: transaction_proto.Transaction.Req):
        return self._transaction_ext.execute(request).type_res

    def stream(self, request: transaction_proto.Transaction.Req):
        return (rp.type_res_part for rp in self._transaction_ext.stream(request))

    def __str__(self):
        return type(self).__name__ + "[label: %s]" % self.get_label()

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self._transaction_ext is other._transaction_ext and self.get_label() == other.get_label()

    def __hash__(self):
        return self._hash
