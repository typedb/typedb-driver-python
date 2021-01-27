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
from grakn.concept.thing.thing import Thing, RemoteThing


class Entity(Thing):

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        return Entity(concept_proto_reader.iid(thing_proto.iid))

    def as_remote(self, transaction):
        return RemoteEntity(transaction, self._iid)

    def is_entity(self):
        return True


class RemoteEntity(RemoteThing):

    def as_remote(self, transaction):
        return RemoteEntity(transaction, self._iid)

    def is_entity(self):
        return True
