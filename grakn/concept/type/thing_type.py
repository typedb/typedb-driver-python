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

from grakn.concept.proto import concept_proto_builder
from grakn.concept.type.type import Type, RemoteType


class ThingType(Type):

    def as_remote(self, transaction):
        return RemoteThingType(transaction, self.get_label(), self.is_root())

    def is_thing_type(self):
        return True


class RemoteThingType(RemoteType):

    def get_instances(self):
        method = concept_proto.Type.Req()
        get_instances_req = concept_proto.ThingType.GetInstances.Req()
        method.thing_type_get_instances_req.CopyFrom(get_instances_req)
        return self._thing_stream(method, lambda res: res.thing_type_get_instances_res.things)

    def set_abstract(self):
        req = concept_proto.Type.Req()
        req.thing_type_set_abstract_req.CopyFrom(concept_proto.ThingType.SetAbstract.Req())
        self._execute(req)

    def unset_abstract(self):
        req = concept_proto.Type.Req()
        req.thing_type_unset_abstract_req.CopyFrom(concept_proto.ThingType.UnsetAbstract.Req())
        self._execute(req)

    def set_plays(self, role, overridden_role=None):
        req = concept_proto.Type.Req()
        set_plays_req = concept_proto.ThingType.SetPlays.Req()
        set_plays_req.role.CopyFrom(concept_proto_builder.type_(role))
        if overridden_role:
            set_plays_req.overridden_role.CopyFrom(concept_proto_builder.type_(overridden_role))
        req.thing_type_set_plays_req.CopyFrom(set_plays_req)
        self._execute(req)

    def set_owns(self, attribute_type, overridden_type=None, is_key=False):
        req = concept_proto.Type.Req()
        set_owns_req = concept_proto.ThingType.SetOwns.Req()
        set_owns_req.attribute_type.CopyFrom(concept_proto_builder.type_(attribute_type))
        set_owns_req.is_key = is_key
        if overridden_type:
            set_owns_req.overridden_type.CopyFrom(concept_proto_builder.type_(overridden_type))
        req.thing_type_set_owns_req.CopyFrom(set_owns_req)
        self._execute(req)

    def get_plays(self):
        req = concept_proto.Type.Req()
        req.thing_type_get_plays_req.CopyFrom(concept_proto.ThingType.GetPlays.Req())
        return self._type_stream(req, lambda res: res.thing_type_get_plays_res.roles)

    def get_owns(self, value_type=None, keys_only=False):
        req = concept_proto.Type.Req()
        get_owns_req = concept_proto.ThingType.GetOwns.Req()
        get_owns_req.keys_only = keys_only
        if value_type:
            get_owns_req.value_type = concept_proto_builder.value_type(value_type)
        req.thing_type_get_owns_req.CopyFrom(get_owns_req)
        return self._type_stream(req, lambda res: res.thing_type_get_owns_res.attribute_types)

    def unset_plays(self, role):
        req = concept_proto.Type.Req()
        unset_plays_req = concept_proto.ThingType.UnsetPlays.Req()
        unset_plays_req.role.CopyFrom(concept_proto_builder.type_(role))
        req.thing_type_unset_plays_req.CopyFrom(unset_plays_req)
        self._execute(req)

    def unset_owns(self, attribute_type):
        req = concept_proto.Type.Req()
        unset_owns_req = concept_proto.ThingType.UnsetOwns.Req()
        unset_owns_req.attribute_type.CopyFrom(concept_proto_builder.type_(attribute_type))
        req.thing_type_unset_owns_req.CopyFrom(unset_owns_req)
        self._execute(req)

    def as_remote(self, transaction):
        return RemoteThingType(transaction, self.get_label(), self.is_root())

    def is_thing_type(self):
        return True
