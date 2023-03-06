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
from typing import Optional, TYPE_CHECKING

import typedb_protocol.cluster.cluster_user_pb2 as cluster_user_proto

from typedb.api.connection.user import User
from typedb.common.rpc.request_builder import cluster_user_password_update_req
from typedb.connection.cluster.database import _FailsafeTask, _ClusterDatabase

if TYPE_CHECKING:
    from typedb.connection.cluster.client import _ClusterClient

class _ClusterUser(User):

    def __init__(self, client: "_ClusterClient", username: str, password_expiry_days: Optional[int]):
        self._client = client
        self._username = username
        self._password_expiry_days = password_expiry_days

    @staticmethod
    def of(user: cluster_user_proto.ClusterUser, client: "_ClusterClient"):
        if user.get_password_expiry_case() == cluster_user_proto.ClusterUser.PasswordExpiryCase.PASSWORDEXPIRY_NOT_SET:
            return _ClusterUser(client, user.get_username(), None)
        else:
            return _ClusterUser(client, user.get_username(), user.get_password_expiry_days())

    def username(self) -> str:
        return self._username

    def password_expiry_days(self) -> Optional[int]:
        return self._password_expiry_days

    def password_update(self, password_old: str, password_new: str) -> None:
        failsafe_task = _UserFailsafeTask(
            self._client,
            lambda replica: self._client._stub(replica.address()).user_password_update(cluster_user_password_update_req(self.username(), password_old, password_new))
        )
        failsafe_task.run_primary_replica()


class _UserFailsafeTask(_FailsafeTask):

    def __init__(self, client: "_ClusterClient", task):
        from typedb.connection.cluster.user_manager import _SYSTEM_DB

        super(_UserFailsafeTask, self).__init__(client, _SYSTEM_DB)
        self._task = task

    def run(self, replica: _ClusterDatabase.Replica):
        return self._task(replica)
