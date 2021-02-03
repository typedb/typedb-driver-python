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
import enum
import sched
import time
from abc import ABC, abstractmethod
from threading import Thread

import grakn_protocol.protobuf.session_pb2 as session_proto
import grpc
from grakn_protocol.protobuf.grakn_pb2_grpc import GraknStub
from grpc import RpcError

from grakn import grakn_proto_builder
from grakn.common.exception import GraknClientException
from grakn.options import GraknOptions
from grakn.rpc.transaction import Transaction, TransactionType


class SessionType(enum.Enum):
    DATA = 0
    SCHEMA = 1


def _session_type_proto(session_type: SessionType):
    if session_type == SessionType.DATA:
        return session_proto.Session.Type.Value("DATA")
    if session_type == SessionType.SCHEMA:
        return session_proto.Session.Type.Value("SCHEMA")


class Session(ABC):

    @abstractmethod
    def transaction(self, transaction_type: TransactionType, options=None) -> Transaction:
        pass

    @abstractmethod
    def session_type(self) -> SessionType:
        pass

    @abstractmethod
    def is_open(self) -> bool:
        pass

    @abstractmethod
    def close(self) -> None:
        pass

    @abstractmethod
    def database(self) -> str:
        pass

    @abstractmethod
    def __enter__(self):
        pass

    @abstractmethod
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass


class _RPCSession(Session):
    _PULSE_FREQUENCY_SECONDS = 5

    def __init__(self, client, database: str, session_type: SessionType, options: GraknOptions = None):
        if not options:
            options = GraknOptions.core()
        self._address = client._address
        self._channel = grpc.insecure_channel(client._address)
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._database = database
        self._session_type = session_type
        self._grpc_stub = GraknStub(self._channel)

        open_req = session_proto.Session.Open.Req()
        open_req.database = database
        open_req.type = _session_type_proto(session_type)
        open_req.options.CopyFrom(grakn_proto_builder.options(options))

        self._session_id = self._grpc_stub.session_open(open_req).session_id
        self._is_open = True
        self._pulse = self._scheduler.enter(delay=self._PULSE_FREQUENCY_SECONDS, priority=1, action=self._transmit_pulse, argument=())
        Thread(target=self._scheduler.run, name="session_pulse_{}".format(self._session_id.hex()), daemon=True).start()

    def transaction(self, transaction_type: TransactionType, options=None) -> Transaction:
        if not options:
            options = GraknOptions.core()
        return Transaction(self._address, self._session_id, transaction_type, options)

    def session_type(self) -> SessionType: return self._session_type

    def is_open(self) -> bool: return self._is_open

    def close(self) -> None:
        if self._is_open:
            self._is_open = False
            self._scheduler.cancel(self._pulse)
            self._scheduler.empty()
            req = session_proto.Session.Close.Req()
            req.session_id = self._session_id
            try:
                self._grpc_stub.session_close(req)
            except RpcError as e:
                raise GraknClientException(e)
            finally:
                self._channel.close()

    def database(self) -> str: return self._database

    def _transmit_pulse(self) -> None:
        if not self._is_open:
            return
        pulse_req = session_proto.Session.Pulse.Req()
        pulse_req.session_id = self._session_id
        res = self._grpc_stub.session_pulse(pulse_req)
        if res.alive:
            self._pulse = self._scheduler.enter(delay=self._PULSE_FREQUENCY_SECONDS, priority=1, action=self._transmit_pulse, argument=())
            Thread(target=self._scheduler.run, name="session_pulse_{}".format(self._session_id.hex()), daemon=True).start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False
