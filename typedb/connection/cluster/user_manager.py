#
#   Copyright (C) 2021 Vaticle
#
#   Licensed to the Apache Software Foundation (ASF) under one
#   or more contributor license agreements.  See the NOTICE file
#   distributed with this work for additional information
#   regarding copyright ownership.  The ASF licenses this file
#   to you under the Apache License, Version 2.0 (the
#   "License"); you may not use this file except in compliance
#   with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing,
#   software distributed under the License is distributed on an
#   "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
#   KIND, either express or implied.  See the License for the
#   specific language governing permissions and limitations
#   under the License.
#
from typing import List,TYPE_CHECKING

from typedb.api.connection.user import UserManager, User
from typedb.common.rpc.request_builder import cluster_user_manager_create_req, cluster_user_manager_all_req, \
    cluster_user_manager_contains_req
from typedb.connection.cluster.database import _FailsafeTask, _ClusterDatabase
from typedb.connection.cluster.user import _ClusterUser
from typedb.common.exception import TypeDBClientException, CLUSTER_USER_DOES_NOT_EXIST

_SYSTEM_DB = "_system"

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient

class _ClusterUserManager(UserManager):

    def __init__(self, client: "_ClusterClient"):
        self._client = client

    def create(self, name: str, password: str) -> None:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_create(
                cluster_user_manager_create_req(name, password))
        )
        failsafe_task.run_primary_replica()

    def all(self) -> List[User]:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            self._get_user_list
        )
        val = failsafe_task.run_primary_replica()
        return val

    def _get_user_list(self, replica: _ClusterDatabase.Replica):
        users_proto = self._client._stub(replica.address()).users_all(cluster_user_manager_all_req())
        return [_ClusterUser(self._client, username) for username in users_proto.names]

    def contains(self, name: str) -> bool:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_contains(cluster_user_manager_contains_req(name))
        )
        return failsafe_task.run_primary_replica()

    def get(self, name: str) -> User:
        if (self.contains(name)):
            return _ClusterUser(self._client, name)
        else:
            raise TypeDBClientException.of(CLUSTER_USER_DOES_NOT_EXIST, name)


class _UserManagerFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", task):
        super(_UserManagerFailsafeTask, self).__init__(client, _SYSTEM_DB)
        self._task = task

    def run(self, replica: _ClusterDatabase.Replica):
        return self._task(replica)

