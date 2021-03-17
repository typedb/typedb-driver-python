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

# Repackaging these symbols allows them to be imported from "grakn.client"
from grakn.common.exception import GraknClientException  # noqa # pylint: disable=unused-import
from grakn.concept.type.value_type import ValueType  # noqa # pylint: disable=unused-import
from grakn.api.options import GraknOptions
from grakn.rpc.cluster.database import DatabaseCluster, _DatabaseClusterRPC  # noqa # pylint: disable=unused-import
from grakn.rpc.cluster.database_manager import DatabaseManagerCluster
from grakn.rpc.database_manager import DatabaseManager
from grakn.rpc.session import Session, SessionType
from grakn.rpc.transaction import TransactionType  # noqa # pylint: disable=unused-import


class GraknClient(ABC):

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @abstractmethod
    def databases(self) -> DatabaseManager:
        pass

    @abstractmethod
    def session(self, database: str, session_type: SessionType, options: GraknOptions = None) -> Session:
        pass

    @abstractmethod
    def is_cluster(self) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class GraknClientCluster(GraknClient):

    @abstractmethod
    def databases(self) -> DatabaseManagerCluster:
        pass
