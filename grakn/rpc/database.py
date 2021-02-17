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
from typing import Set, Optional

import grakn_protocol.protobuf.database_pb2 as database_proto

from grakn.rpc.cluster.server_address import ServerAddress
from grakn.rpc.utils import rpc_call


class Database(ABC):

    @abstractmethod
    def name(self) -> str:
        pass

    @abstractmethod
    def delete(self) -> None:
        pass

    class Replica(ABC):

        @abstractmethod
        def database(self) -> "DatabaseCluster":
            pass

        @abstractmethod
        def term(self) -> int:
            pass

        @abstractmethod
        def is_primary(self) -> bool:
            pass

        @abstractmethod
        def is_preferred_secondary(self) -> bool:
            pass

        @abstractmethod
        def address(self) -> ServerAddress:
            pass


class DatabaseCluster(Database, ABC):

    @abstractmethod
    def replicas(self) -> Set[Database.Replica]:
        pass

    @abstractmethod
    def primary_replica(self) -> Optional[Database.Replica]:
        pass

    @abstractmethod
    def preferred_secondary_replica(self) -> Database.Replica:
        pass


class _DatabaseRPC(Database):

    def __init__(self, database_manager, name: str):
        self._name = name
        self._grpc_stub = database_manager.grpc_stub()

    def name(self) -> str:
        return self._name

    def delete(self) -> None:
        request = database_proto.Database.Delete.Req()
        request.name = self._name
        rpc_call(lambda: self._grpc_stub.database_delete(request))

    def __str__(self):
        return self._name
