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

from typing import Dict, List

from grakn.rpc.cluster.address import Address
from grakn.rpc.database_manager import DatabaseManager, _RPCDatabaseManager


class _RPCDatabaseManagerCluster(DatabaseManager):

    def __init__(self, database_managers: Dict[Address.Server, "_RPCDatabaseManager"]):
        assert database_managers
        self._database_managers = database_managers

    def contains(self, name: str) -> bool:
        return next(iter(self._database_managers.values())).contains(name)

    def create(self, name: str) -> None:
        for database_manager in self._database_managers.values():
            database_manager.create(name)

    def delete(self, name: str) -> None:
        for database_manager in self._database_managers.values():
            database_manager.delete(name)

    def all(self) -> List[str]:
        return next(iter(self._database_managers.values())).all()
