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

from __future__ import annotations
from typing import Optional, TYPE_CHECKING

from typedb.api.connection.user import User
from typedb.typedb_client_python import user_get_username, \
    user_get_password_expiry_seconds, user_password_update

if TYPE_CHECKING:
    from typedb.typedb_client_python import User as NativeUser, Connection as NativeConnection


class _User(User):

    def __init__(self, user: NativeUser, connection: NativeConnection):
        self._native_user = user
        self._native_connection = connection

    def username(self) -> str:
        return user_get_username(self._native_user)

    def password_expiry_seconds(self) -> Optional[int]:
        if res := user_get_password_expiry_seconds(self._native_user) >= 0:
            return res
        return None

    def password_update(self, password_old: str, password_new: str) -> None:
        user_password_update(self._native_user, self._native_connection, password_old, password_new)
