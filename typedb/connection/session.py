#
# Copyright (C) 2022 Vaticle
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

from __future__ import annotations
from typing import TYPE_CHECKING, Optional

from typedb.api.connection.options import Options
from typedb.api.connection.session import Session
from typedb.connection.transaction import _Transaction

from typedb.typedb_client_python import session_new, session_on_close, session_force_close, session_is_open, \
    session_get_database_name, SessionCallbackDirector


if TYPE_CHECKING:
    from typedb.api.connection.session import SessionType
    from typedb.api.connection.transaction import Transaction, TransactionType
    from typedb.connection.database import _Database


class _Session(Session):

    def __init__(self, database: _Database, session_type: SessionType, options: Optional[Options] = None):
        if not options:
            options = Options()
        self._type = session_type
        self._options = options
        db = database.native_object
        db.thisown = 0
        self._native_object = session_new(db, session_type.value, options.native_object)

    @property
    def native_object(self):
        return self._native_object

    def is_open(self) -> bool:
        return session_is_open(self.native_object)

    @property
    def type(self) -> SessionType:
        return self._type

    def database_name(self) -> str:
        return session_get_database_name(self.native_object)

    @property
    def options(self) -> Options:
        return self._options

    def transaction(self, transaction_type: TransactionType, options: Options = None) -> Transaction:
        return _Transaction(self, transaction_type, options)

    def close(self) -> None:
        session_force_close(self.native_object)

    def on_close(self, function: callable):
        session_on_close(self.native_object, _Session.Callback(function))

    class Callback(SessionCallbackDirector):

        def __init__(self, function: callable):
            super().__init__()
            self._function = function

        def callback(self) -> None:
            self._function()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False
