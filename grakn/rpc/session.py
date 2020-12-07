from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.session_pb2 as session_proto
import enum

from grakn.rpc.Transaction import Transaction


class Session(object):

    def __init__(self, channel, database, session_type, options=None):
        self._channel = channel
        self._database = database
        self._session_type = session_type
        self._grpc_stub = GraknStub(channel)

        openReq = session_proto.Session.Open.Req()
        openReq.database = database
        openReq.type = Session._session_type_proto(session_type)
        # TODO openReq.options = None

        self._session_id = self._grpc_stub.session_open(openReq).session_id
        self._is_open = True
        # TODO transmit pulses at regular intervals

    def transaction(self, transaction_type, options=None):
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
        if session_type == Session.Type.DATA: return session_proto.Session.Type.Value("DATA")
        if session_type == Session.Type.SCHEMA: return session_proto.Session.Type.Value("SCHEMA")

    class Type(enum.Enum):
        DATA = 0
        SCHEMA = 1
