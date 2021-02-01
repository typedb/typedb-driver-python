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

from grakn.concept.thing.entity import Entity
from grakn.concept.type.thing_type import ThingType, RemoteThingType


class EntityType(ThingType):

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return EntityType(type_proto.label, type_proto.root)

    def as_remote(self, transaction):
        return RemoteEntityType(transaction, self.get_label(), self.is_root())

    def is_entity_type(self):
        return True


class RemoteEntityType(RemoteThingType):

    def as_remote(self, transaction):
        return RemoteEntityType(transaction, self.get_label(), self.is_root())

    def create(self):
        method = concept_proto.Type.Req()
        create_req = concept_proto.EntityType.Create.Req()
        method.entity_type_create_req.CopyFrom(create_req)
        return Entity._of(self._execute(method).entity_type_create_res.entity)

    def is_entity_type(self):
        return True
