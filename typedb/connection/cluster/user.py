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
from typing import TYPE_CHECKING

from typedb.api.connection.user import User
from typedb.common.rpc.request_builder import cluster_user_password_req, cluster_user_delete_req
from typedb.connection.cluster.database import _FailsafeTask, _ClusterDatabase

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient

class _ClusterUser(User):

    def __init__(self, client: "_ClusterClient", name: str):
        self._client = client
        self._name = name

    def name(self) -> str:
        return self._name

    def password(self, password: str) -> None:
        failsafe_task = _UserFailsafeTask(self._client, lambda replica: self._client._stub(replica.address()).user_password(cluster_user_password_req(self.name(), password)))
        failsafe_task.run_primary_replica()

    def delete(self) -> None:
        failsafe_task = _UserFailsafeTask(self._client, lambda replica: self._client._stub(replica.address()).user_delete(cluster_user_delete_req(self.name())))
        failsafe_task.run_primary_replica()


class _UserFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", task):
        from typedb.connection.cluster.user_manager import _SYSTEM_DB

        super(_UserFailsafeTask, self).__init__(client, _SYSTEM_DB)
        self._task = task

    def run(self, replica: _ClusterDatabase.Replica):
        return self._task(replica)
