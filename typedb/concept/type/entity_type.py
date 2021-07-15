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

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.type.entity_type import EntityType, RemoteEntityType
from typedb.common.label import Label
from typedb.common.rpc.request_builder import entity_type_create_req
from typedb.concept.thing.entity import _Entity
from typedb.concept.type.thing_type import _ThingType, _RemoteThingType


class _EntityType(EntityType, _ThingType):

    @staticmethod
    def of(type_proto: concept_proto.Type):
        return _EntityType(Label.of(type_proto.label), type_proto.root)

    def as_remote(self, transaction):
        return _RemoteEntityType(transaction, self.get_label(), self.is_root())

    def as_entity_type(self) -> "EntityType":
        return self


class _RemoteEntityType(_RemoteThingType, RemoteEntityType):

    def as_remote(self, transaction):
        return _RemoteEntityType(transaction, self.get_label(), self.is_root())

    def as_entity_type(self) -> "RemoteEntityType":
        return self

    def create(self):
        return _Entity.of(self.execute(entity_type_create_req(self.get_label())).entity_type_create_res.entity)
