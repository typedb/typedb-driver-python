from graknprotocol.protobuf.grakn_pb2_grpc import GraknStub
import graknprotocol.protobuf.database_pb2 as database_proto


class DatabaseManager(object):

    def __init__(self, channel):
        self._blockingGrpcStub = GraknStub(channel)

    def contains(self, name):
        return self._blockingGrpcStub.database_contains(database_proto.Database.Contains.Req()).contains

    def create(self, name):
        request = database_proto.Database.Create.Req()
        request.name = name
        self._blockingGrpcStub.database_create(request)

    def delete(self, name):
        request = database_proto.Database.Delete.Req()
        request.name = name
        self._blockingGrpcStub.database_delete(request)

    def all(self):
        return list(self._blockingGrpcStub.database_all(database_proto.Database.All.Req()).names)
