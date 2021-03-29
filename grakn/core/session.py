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
import sched
import time
from threading import Thread
from typing import TYPE_CHECKING

import grakn_protocol.common.session_pb2 as session_proto
from grpc import RpcError

from grakn.api.options import GraknOptions
from grakn.api.session import GraknSession, SessionType
from grakn.api.transaction import GraknTransaction, TransactionType
from grakn.common.concurrent.atomic import AtomicBoolean
from grakn.common.concurrent.lock import ReadWriteLock
from grakn.common.rpc.request_builder import session_open_req
from grakn.core.database import _CoreDatabase
from grakn.core.transaction import _CoreTransaction
from grakn.stream.request_transmitter import RequestTransmitter

if TYPE_CHECKING:
    from grakn.core.client import _CoreClient


class _CoreSession(GraknSession):
    _PULSE_INTERVAL_SECONDS = 5

    def __init__(self, client: "_CoreClient", database: str, session_type: SessionType, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        self._client = client
        self._address = client.address()
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._session_type = session_type
        self._options = options
        self._rw_lock = ReadWriteLock()
        self._stub = client.stub()
        self._database = _CoreDatabase(stub=self._stub, name=database)

        start_time = time.time() * 1000.0
        res = self._stub.session_open(session_open_req(database, session_type.proto(), options.proto()))
        end_time = time.time() * 1000.0
        self._network_latency_millis = int(end_time - start_time - res.server_duration_millis)
        self._session_id = res.session_id
        self._is_open = AtomicBoolean(True)

        self._pulse = self._scheduler.enter(delay=self._PULSE_INTERVAL_SECONDS, priority=1, action=self._transmit_pulse, argument=())
        Thread(target=self._scheduler.run, name="session_pulse_{}".format(self._session_id.hex()), daemon=True).start()

    def is_open(self) -> bool:
        return self._is_open.get()

    def session_type(self) -> SessionType:
        return self._session_type

    def database(self) -> _CoreDatabase:
        return self._database

    def options(self) -> GraknOptions:
        return self._options

    def transaction(self, transaction_type: TransactionType, options: GraknOptions = None) -> GraknTransaction:
        if not options:
            options = GraknOptions.core()
        try:
            self._rw_lock.acquire_read()
            return _CoreTransaction(self, transaction_type, options)
        finally:
            self._rw_lock.release_read()

    def session_id(self) -> bytes:
        return self._session_id

    def address(self) -> str:
        return self._client.address()

    def transmitter(self) -> RequestTransmitter:
        return self._client.transmitter()

    def network_latency_millis(self) -> int:
        return self._network_latency_millis

    def close(self) -> None:
        try:
            self._rw_lock.acquire_write()
            if self._is_open.compare_and_set(True, False):
                self._client.remove_session(self)
                try:
                    self._scheduler.cancel(self._pulse)
                except ValueError:  # This may occur if a pulse is in transit right now.
                    pass
                req = session_proto.Session.Close.Req()
                req.session_id = self._session_id
                try:
                    self._stub.session_close(req)
                except RpcError:  # This generally means the session is already closed.
                    pass
        finally:
            self._rw_lock.release_write()

    def _transmit_pulse(self) -> None:
        if not self.is_open():
            return
        pulse_req = session_proto.Session.Pulse.Req()
        pulse_req.session_id = self._session_id
        try:
            alive = self._stub.session_pulse(pulse_req).alive
        except RpcError:
            alive = False

        if alive:
            self._pulse = self._scheduler.enter(delay=self._PULSE_INTERVAL_SECONDS, priority=1, action=self._transmit_pulse, argument=())
            Thread(target=self._scheduler.run, name="session_pulse_{}".format(self._session_id.hex()), daemon=True).start()
        else:
            self._is_open.set(False)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is not None:
            return False
