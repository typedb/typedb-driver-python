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
from threading import Lock
from typing import Dict

from grpc import Channel

from typedb.api.connection.client import TypeDBClient
from typedb.api.connection.options import TypeDBOptions
from typedb.api.connection.session import SessionType
from typedb.common.concurrent.scheduled_executor import ScheduledExecutor
from typedb.common.rpc.stub import TypeDBStub
from typedb.connection.database_manager import _TypeDBDatabaseManagerImpl
from typedb.connection.session import _TypeDBSessionImpl
from typedb.stream.request_transmitter import RequestTransmitter


class _TypeDBClientImpl(TypeDBClient):
    _PULSE_INTERVAL_SECONDS = 5

    # TODO: Detect number of available CPUs
    def __init__(self, address: str, parallelisation: int = 2):
        self._address = address
        self._transmitter = RequestTransmitter(parallelisation)
        self._sessions: Dict[bytes, _TypeDBSessionImpl] = {}
        self._sessions_lock = Lock()
        self._is_open = True
        self._pulse_executor = ScheduledExecutor()
        self._pulse_executor.schedule_at_fixed_rate(interval=self._PULSE_INTERVAL_SECONDS, action=self._transmit_pulses)

    def session(self, database: str, session_type: SessionType, options=None) -> _TypeDBSessionImpl:
        if not options:
            options = TypeDBOptions.core()
        session = _TypeDBSessionImpl(self, database, session_type, options)
        with self._sessions_lock:
            self._sessions[session.session_id()] = session
        return session

    def remove_session(self, session: _TypeDBSessionImpl) -> None:
        with self._sessions_lock:
            del self._sessions[session.session_id()]

    def databases(self) -> _TypeDBDatabaseManagerImpl:
        pass

    def is_open(self) -> bool:
        return self._is_open

    def address(self) -> str:
        return self._address

    def channel(self) -> Channel:
        pass

    def stub(self) -> TypeDBStub:
        pass

    def new_channel_and_stub(self) -> (Channel, TypeDBStub):
        pass

    def transmitter(self) -> RequestTransmitter:
        return self._transmitter

    def is_cluster(self) -> bool:
        return False

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False

    def close(self) -> None:
        self._is_open = False
        with self._sessions_lock:
            sessions = self._sessions.copy()
        for session in sessions.values():
            session.close()
        self._pulse_executor.shutdown()

    def _transmit_pulses(self) -> None:
        if not self.is_open():
            return
        with self._sessions_lock:
            sessions = self._sessions.copy()
        for session in sessions.values():
            session.transmit_pulse()
