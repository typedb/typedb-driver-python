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

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.type.role_type import RoleType, RemoteRoleType
from typedb.common.label import Label
from typedb.common.rpc.request_builder import role_type_get_relation_types_req, role_type_get_player_types_req, \
    role_type_get_player_types_explicit_req, role_type_get_relation_instances_req, \
    role_type_get_relation_instances_explicit_req, role_type_get_player_instances_req, \
    role_type_get_player_instances_explicit_req
from typedb.concept.proto import concept_proto_reader
from typedb.concept.type.type import _Type, _RemoteType


class _RoleType(_Type, RoleType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _RoleType(Label.of(type_proto.scope, type_proto.label), type_proto.is_root, type_proto.is_abstract)

    def as_remote(self, transaction):
        return _RemoteRoleType(transaction, self.get_label(), self.is_root(), self.is_abstract())

    def as_role_type(self) -> "RoleType":
        return self


class _RemoteRoleType(_RemoteType, RemoteRoleType):

    def as_remote(self, transaction):
        return _RemoteRoleType(transaction, self.get_label(), self.is_root(), self.is_abstract())

    def as_role_type(self) -> "RemoteRoleType":
        return self

    def is_deleted(self) -> bool:
        return self.get_relation_type() is not None and self.get_relation_type().as_remote(self._transaction_ext).get_relates(self.get_label().name()) is not None

    def get_relation_type(self):
        return self._transaction_ext.concepts().get_relation_type(self.get_label().scope())

    def get_relation_types(self):
        return (concept_proto_reader.type_(rt) for rp in self.stream(role_type_get_relation_types_req(self.get_label()))
                for rt in rp.role_type_get_relation_types_res_part.relation_types)

    def get_player_types(self):
        return (concept_proto_reader.thing_type(tt) for rp in self.stream(role_type_get_player_types_req(self.get_label()))
                for tt in rp.role_type_get_player_types_res_part.thing_types)

    def get_player_types_explicit(self):
        return (concept_proto_reader.thing_type(tt) for rp in self.stream(role_type_get_player_types_explicit_req(self.get_label()))
                for tt in rp.role_type_get_player_types_explicit_res_part.thing_types)

    def get_relation_instances(self):
        return (concept_proto_reader.thing(t) for res in self.stream(role_type_get_relation_instances_req(self.get_label()))
                for t in res.role_type_get_relation_instances_res_part.relations)

    def get_relation_instances_explicit(self):
        return (concept_proto_reader.thing(t) for res in self.stream(role_type_get_relation_instances_explicit_req(self.get_label()))
                for t in res.role_type_get_relation_instances_explicit_res_part.relations)

    def get_player_instances(self):
        return (concept_proto_reader.thing(t) for res in self.stream(role_type_get_player_instances_req(self.get_label()))
                for t in res.role_type_get_player_instances_res_part.things)

    def get_player_instances_explicit(self):
        return (concept_proto_reader.thing(t) for res in self.stream(role_type_get_player_instances_explicit_req(self.get_label()))
                for t in res.role_type_get_player_instances_explicit_res_part.things)
