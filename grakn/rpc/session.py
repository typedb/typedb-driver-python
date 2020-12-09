from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.session_pb2 as session_proto
import enum

from grakn.options import GraknOptions
from grakn.proto_builder import GraknProtoBuilder
from grakn.rpc.transaction import Transaction


class Session(object):

    def __init__(self, client, database, session_type, options=GraknOptions()):
        self._channel = client._channel
        self._scheduler = client._scheduler
        self._database = database
        self._session_type = session_type
        self._grpc_stub = GraknStub(self._channel)

        open_req = session_proto.Session.Open.Req()
        open_req.database = database
        open_req.type = Session._session_type_proto(session_type)
        open_req.options.CopyFrom(GraknProtoBuilder.options(options))

        self._session_id = self._grpc_stub.session_open(open_req).session_id
        self._is_open = True
        self._pulse = self._scheduler.enter(5, 1, self._transmit_pulse, ())

    def transaction(self, transaction_type, options=GraknOptions()):
        return Transaction(self._channel, self._session_id, transaction_type, options)

    def session_type(self): return self._session_type

    def is_open(self): return self._is_open

    def close(self):
        if self._is_open:
            self._is_open = False
            req = session_proto.Session.Close.Req()
            req.session_id = self._session_id
            self._grpc_stub.session_close(req)

    def database(self): return self._database

    def _transmit_pulse(self):
        if not self._is_open:
            return
        pulse_req = session_proto.Session.Pulse.Req()
        pulse_req.session_id = self._session_id
        is_alive = self._grpc_stub.session_pulse(pulse_req).is_alive
        if is_alive:
            self._pulse = self._scheduler.enter(5, 1, self._transmit_pulse, ())

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        if exc_tb is None:
            pass
        else:
            return False

    @staticmethod
    def _session_type_proto(session_type):
        if session_type == Session.Type.DATA:
            return session_proto.Session.Type.Value("DATA")
        if session_type == Session.Type.SCHEMA:
            return session_proto.Session.Type.Value("SCHEMA")

    class Type(enum.Enum):
        DATA = 0
        SCHEMA = 1
