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

from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.session_pb2 as session_proto
import enum

from grakn import grakn_proto_builder
from grakn.options import GraknOptions
from grakn.rpc.transaction import Transaction, TransactionType


class SessionType(enum.Enum):
    DATA = 0
    SCHEMA = 1


class Session(object):

    _PULSE_FREQUENCY_SECONDS = 5

    def __init__(self, client, database: str, session_type: SessionType, options=GraknOptions()):
        self._channel = client._channel
        self._scheduler = sched.scheduler(time.time, time.sleep)
        self._database = database
        self._session_type = session_type
        self._grpc_stub = GraknStub(self._channel)

        open_req = session_proto.Session.Open.Req()
        open_req.database = database
        open_req.type = Session._session_type_proto(session_type)
        open_req.options.CopyFrom(grakn_proto_builder.options(options))

        self._session_id = self._grpc_stub.session_open(open_req).session_id
        self._is_open = True
        self._pulse = self._scheduler.enter(delay=self._PULSE_FREQUENCY_SECONDS, priority=1, action=self._transmit_pulse, argument=())
        # TODO: This thread blocks the process from closing. We should try cancelling the scheduled task when the
        #       session closes. If that doesn't work, we need some other way of getting the thread to exit.
        Thread(target=self._scheduler.run, daemon=True).start()

    def transaction(self, transaction_type: TransactionType, options=GraknOptions()):
        return Transaction(self._channel, self._session_id, transaction_type, options)

    def session_type(self): return self._session_type

    def is_open(self): return self._is_open

    def close(self):
        if self._is_open:
            self._is_open = False
            self._scheduler.cancel(self._pulse)
            self._scheduler.empty()
            req = session_proto.Session.Close.Req()
            req.session_id = self._session_id
            self._grpc_stub.session_close(req)

    def database(self): return self._database

    def _transmit_pulse(self):
        if not self._is_open:
            return
        pulse_req = session_proto.Session.Pulse.Req()
        pulse_req.session_id = self._session_id
        res = self._grpc_stub.session_pulse(pulse_req)
        if res.alive:
            self._pulse = self._scheduler.enter(delay=self._PULSE_FREQUENCY_SECONDS, priority=1, action=self._transmit_pulse, argument=())
            Thread(target=self._scheduler.run, daemon=True).start()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False

    @staticmethod
    def _session_type_proto(session_type: SessionType):
        if session_type == SessionType.DATA:
            return session_proto.Session.Type.Value("DATA")
        if session_type == SessionType.SCHEMA:
            return session_proto.Session.Type.Value("SCHEMA")
