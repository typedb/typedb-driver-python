import graknprotocol.protobuf.concept_pb2 as concept_proto
import graknprotocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.type.type import Type, RemoteType


class RoleType(Type):

    def __init__(self, label: str, scope: str, is_root: bool):
        super(RoleType, self).__init__(label, is_root)
        self._scope = scope

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return RoleType(type_proto.label, type_proto.scope, type_proto.root)

    def as_remote(self, transaction):
        return RemoteRoleType(transaction, self._label, self._scope, self._is_root)


class RemoteRoleType(RemoteType):

    def __init__(self, transaction, label: str, scope: str, is_root: bool):
        super(RemoteRoleType, self).__init__(transaction, label, is_root)
        self._scope = scope
