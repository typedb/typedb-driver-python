from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.session_pb2 as session_proto
import enum


class Session(object):

    def __init__(self, channel, database, type, options=None):
        self._channel = channel
        self._database = database
        self._type = type
        self._blockingGrpcStub = GraknStub(channel)

        openReq = session_proto.Session.Open.Req()
        openReq.database = database
        openReq.type = Session._session_type(type)
        # TODO openReq.options = None

        self._session_id = self._blockingGrpcStub.session_open(openReq).session_id
        self._is_open = True

    def transaction(self, type, options=None): pass

    def type(self): return self._type

    def is_open(self): return self._is_open

    def close(self):
        if self._is_open:
            self._is_open = False
            req = session_proto.Session.Close.Req()
            req.session_id = self._session_id
            self._blockingGrpcStub.session_close(req)

    def database(self): return self._database

    @staticmethod
    def _session_type(type):
        if type == Session.Type.DATA: return session_proto.Session.Type.Value("DATA")
        if type == Session.Type.SCHEMA: return session_proto.Session.Type.Value("SCHEMA")

    class Type(enum.Enum):
        DATA = 0
        SCHEMA = 1
