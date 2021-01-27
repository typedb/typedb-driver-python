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

import grakn_protocol.protobuf.concept_pb2 as concept_proto

from grakn.concept.proto import concept_proto_reader
from grakn.concept.thing.relation import Relation
from grakn.concept.type.thing_type import ThingType, RemoteThingType


class RelationType(ThingType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return RelationType(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteRelationType(transaction, self.get_label(), self.is_root())

    def is_relation_type(self):
        return True


class RemoteRelationType(RemoteThingType):

    def as_remote(self, transaction):
        return RemoteRelationType(transaction, self.get_label(), self.is_root())

    def create(self):
        method = concept_proto.Type.Req()
        create_req = concept_proto.RelationType.Create.Req()
        method.relation_type_create_req.CopyFrom(create_req)
        return Relation._of(self._execute(method).relation_type_create_res.relation)

    def get_relates(self, role_label: str = None):
        method = concept_proto.Type.Req()
        if role_label:
            get_relates_req = concept_proto.RelationType.GetRelatesForRoleLabel.Req()
            get_relates_req.label = role_label
            method.relation_type_get_relates_for_role_label_req.CopyFrom(get_relates_req)
            res = self._execute(method).relation_type_get_relates_for_role_label_res
            return concept_proto_reader.type_(res.role_type) if res.HasField("role_type") else None
        else:
            method.relation_type_get_relates_req.CopyFrom(concept_proto.RelationType.GetRelates.Req())
            return self._type_stream(method, lambda res: res.relation_type_get_relates_res.roles)

    def set_relates(self, role_label: str, overridden_label: str = None):
        method = concept_proto.Type.Req()
        set_relates_req = concept_proto.RelationType.SetRelates.Req()
        set_relates_req.label = role_label
        if overridden_label:
            set_relates_req.overridden_label = overridden_label
        method.relation_type_set_relates_req.CopyFrom(set_relates_req)
        self._execute(method)

    def unset_relates(self, role_label: str):
        method = concept_proto.Type.Req()
        unset_relates_req = concept_proto.RelationType.UnsetRelates.Req()
        unset_relates_req.label = role_label
        method.relation_type_unset_relates_req.CopyFrom(unset_relates_req)
        self._execute(method)

    def is_relation_type(self):
        return True
