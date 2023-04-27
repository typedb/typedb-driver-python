#
#   Copyright (C) 2022 Vaticle
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
from typedb.common.rpc.request_builder import cluster_user_manager_contains_req, cluster_user_manager_create_req, \
    cluster_user_manager_delete_req, cluster_user_manager_all_req, cluster_user_manager_password_set_req, \
    cluster_user_manager_get_req
from typedb.connection.cluster.database import _FailsafeTask, _ClusterDatabase
from typedb.connection.cluster.user import _ClusterUser

_SYSTEM_DB = "_system"

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient

class _ClusterUserManager(UserManager):

    def __init__(self, client: "_ClusterClient"):
        self._client = client

    def create(self, username: str, password: str) -> None:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_create(
                cluster_user_manager_create_req(username, password))
        )
        failsafe_task.run_primary_replica()

    def delete(self, username: str) -> None:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_delete(
                cluster_user_manager_delete_req(username))
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
        return [_ClusterUser.of(user, self._client) for user in users_proto.users]

    def contains(self, username: str) -> bool:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_contains(cluster_user_manager_contains_req(username))
        )
        return failsafe_task.run_primary_replica()

    def get(self, username: str) -> User:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: _ClusterUser.of(self._client._stub(replica.address()).users_get(cluster_user_manager_get_req(username)).user, self._client)
        )
        return failsafe_task.run_primary_replica()

    def password_set(self, username: str, password: str) -> None:
        failsafe_task = _UserManagerFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).users_password_set(
                cluster_user_manager_password_set_req(username, password))
        )
        failsafe_task.run_primary_replica()


class _UserManagerFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", task):
        super(_UserManagerFailsafeTask, self).__init__(client, _SYSTEM_DB)
        self._task = task

    def run(self, replica: _ClusterDatabase.Replica):
        return self._task(replica)
