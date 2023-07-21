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
import time
from typing import TYPE_CHECKING

import typedb_protocol.common.session_pb2 as session_proto

from typedb.api.connection.options import TypeDBOptions
from typedb.api.connection.session import Session, SessionType
from typedb.api.connection.transaction import TypeDBTransaction, TransactionType
from typedb.common.concurrent.atomic import AtomicBoolean
from typedb.common.concurrent.lock import ReadWriteLock
from typedb.common.rpc.request_builder import session_open_req
from typedb.common.rpc.stub import TypeDBStub
from typedb.connection.database import _DatabaseImpl
from typedb.connection.transaction import _TypeDBTransactionImpl
from typedb.stream.request_transmitter import RequestTransmitter

from typedb.common.exception import TypeDBClientException

from typedb.typedb_client_python import session_new, session_on_close, session_force_close, session_is_open, session_get_database_name, SessionCallbackDirector

if TYPE_CHECKING:
    from typedb.connection.client import _TypeDBClientImpl


class _SessionImpl(Session):
    def __init__(self, database: str, session_type: SessionType, options: TypeDBOptions = None):
        if not options:
            options = TypeDBOptions.core()
        self._session_type = session_type
        self._options = options
        self._session = session_new(database, session_type, options)

    def is_open(self) -> bool:
        return session_is_open(self._session)

    def session_type(self) -> SessionType:
        return self._session_type

    def database_name(self) -> str:
        return session_get_database_name(self._session)

    def options(self) -> TypeDBOptions:
        return self._options

    def transaction(self, transaction_type: TransactionType, options: TypeDBOptions = None) -> TypeDBTransaction:
        return _TypeDBTransactionImpl(self, transaction_type, options)

    def close(self) -> None:
        session_force_close(self._session)

    def on_close(self, function: callable):
        session_on_close(self._session, _SessionImpl.Callback(function))

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
