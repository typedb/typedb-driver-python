from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.database_pb2 as database_proto
from grpc import Channel


class DatabaseManager(object):

    def __init__(self, channel: Channel):
        self._grpc_stub = GraknStub(channel)

    def contains(self, name: str):
        request = database_proto.Database.Contains.Req()
        request.name = name
        return self._grpc_stub.database_contains(request).contains

    def create(self, name: str):
        request = database_proto.Database.Create.Req()
        request.name = name
        self._grpc_stub.database_create(request)

    def delete(self, name: str):
        request = database_proto.Database.Delete.Req()
        request.name = name
        self._grpc_stub.database_delete(request)

    def all(self):
        return list(self._grpc_stub.database_all(database_proto.Database.All.Req()).names)
