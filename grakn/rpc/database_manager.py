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
from abc import ABC, abstractmethod
from typing import List

import grakn_protocol.protobuf.database_pb2 as database_proto
from grakn_protocol.protobuf.grakn_pb2_grpc import GraknStub
from grpc import Channel, RpcError

from grakn.common.exception import GraknClientException


class DatabaseManager(ABC):

    @abstractmethod
    def contains(self, name: str) -> bool:
        pass

    @abstractmethod
    def create(self, name: str) -> None:
        pass

    @abstractmethod
    def delete(self, name: str) -> None:
        pass

    @abstractmethod
    def all(self) -> List[str]:
        pass


def _not_blank(name: str) -> str:
    if name in [None, ""] or name.isspace():
        raise GraknClientException("Database name must not be empty.")
    return name


class _RPCDatabaseManager:

    def __init__(self, channel: Channel):
        self._grpc_stub = GraknStub(channel)

    def contains(self, name: str) -> bool:
        request = database_proto.Database.Contains.Req()
        request.name = _not_blank(name)
        try:
            return self._grpc_stub.database_contains(request).contains
        except RpcError as e:
            raise GraknClientException(e)

    def create(self, name: str) -> None:
        request = database_proto.Database.Create.Req()
        request.name = _not_blank(name)
        try:
            self._grpc_stub.database_create(request)
        except RpcError as e:
            raise GraknClientException(e)

    def delete(self, name: str) -> None:
        request = database_proto.Database.Delete.Req()
        request.name = _not_blank(name)
        try:
            self._grpc_stub.database_delete(request)
        except RpcError as e:
            raise GraknClientException(e)

    def all(self) -> List[str]:
        try:
            return list(self._grpc_stub.database_all(database_proto.Database.All.Req()).names)
        except RpcError as e:
            raise GraknClientException(e)
