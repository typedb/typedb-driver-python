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
from typing import Iterator

from typedb.api.concept.type.attribute_type import AttributeType
from typedb.api.concept.type.role_type import RoleType
from typedb.api.concept.type.thing_type import ThingType, RemoteThingType
from typedb.common.label import Label
from typedb.common.rpc.request_builder import thing_type_set_supertype_req, thing_type_get_instances_req, \
    thing_type_set_abstract_req, thing_type_unset_abstract_req, thing_type_set_plays_req, thing_type_set_owns_req, \
    thing_type_get_plays_req, thing_type_get_owns_req, thing_type_unset_plays_req, thing_type_unset_owns_req
from typedb.concept.proto import concept_proto_builder, concept_proto_reader
from typedb.concept.type.type import _Type, _RemoteType


class _ThingType(ThingType, _Type):

    def as_remote(self, transaction):
        return _RemoteThingType(transaction, self.get_label(), self.is_root())

    def as_thing_type(self) -> "ThingType":
        return self


class _RemoteThingType(_RemoteType, RemoteThingType):

    def as_remote(self, transaction):
        return _RemoteThingType(transaction, self.get_label(), self.is_root())

    def as_thing_type(self) -> "RemoteThingType":
        return self

    def is_deleted(self) -> bool:
        return not self._transaction_ext.concepts().get_thing_type(self.get_label().name())

    def set_supertype(self, thing_type: ThingType):
        self.execute(thing_type_set_supertype_req(self.get_label(), concept_proto_builder.thing_type(thing_type)))

    def get_instances(self):
        return (concept_proto_reader.thing(t) for rp in self.stream(thing_type_get_instances_req(self.get_label()))
                for t in rp.thing_type_get_instances_res_part.things)

    def set_abstract(self):
        self.execute(thing_type_set_abstract_req(self.get_label()))

    def unset_abstract(self):
        self.execute(thing_type_unset_abstract_req(self.get_label()))

    def set_plays(self, role_type: RoleType, overridden_role_type: RoleType = None):
        self.execute(thing_type_set_plays_req(self.get_label(), concept_proto_builder.role_type(role_type), concept_proto_builder.role_type(overridden_role_type)))

    def set_owns(self, attribute_type: AttributeType, overridden_type: AttributeType = None, is_key: bool = False):
        self.execute(thing_type_set_owns_req(self.get_label(), concept_proto_builder.thing_type(attribute_type), concept_proto_builder.thing_type(overridden_type), is_key))

    def get_plays(self):
        return (concept_proto_reader.type_(t) for rp in self.stream(thing_type_get_plays_req(self.get_label()))
                for t in rp.thing_type_get_plays_res_part.roles)

    def get_owns(self, value_type: AttributeType.ValueType = None, keys_only: bool = False):
        return (concept_proto_reader.type_(t) for rp in self.stream(thing_type_get_owns_req(self.get_label(), value_type.proto() if value_type else None, keys_only))
                for t in rp.thing_type_get_owns_res_part.attribute_types)

    def unset_plays(self, role_type: RoleType):
        self.execute(thing_type_unset_plays_req(self.get_label(), concept_proto_builder.role_type(role_type)))

    def unset_owns(self, attribute_type: AttributeType):
        self.execute(thing_type_unset_owns_req(self.get_label(), concept_proto_builder.thing_type(attribute_type)))
