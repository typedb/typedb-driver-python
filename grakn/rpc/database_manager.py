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

from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.database_pb2 as database_proto
from grpc import Channel


class DatabaseManager(object):

    def __init__(self, channel: Channel):
        self._grpc_stub = GraknStub(channel)

    def contains(self, name: str):
        request = database_proto.Database.Contains.Req()
        request.name = name
        return self._grpc_stub.database_contains(request).contains

    def create(self, name: str):
        request = database_proto.Database.Create.Req()
        request.name = name
        self._grpc_stub.database_create(request)

    def delete(self, name: str):
        request = database_proto.Database.Delete.Req()
        request.name = name
        self._grpc_stub.database_delete(request)

    def all(self):
        return list(self._grpc_stub.database_all(database_proto.Database.All.Req()).names)
