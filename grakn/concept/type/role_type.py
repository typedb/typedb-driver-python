from typing import Callable, List

import graknprotocol.protobuf.concept_pb2 as concept_proto

from grakn.concept import proto_reader
from grakn.concept.type.type import Type, RemoteType


class RoleType(Type):

    def __init__(self, label: str, scope: str, is_root: bool):
        super(RoleType, self).__init__(label, is_root)
        self._scope = scope

    @staticmethod
    def _of(type_proto: concept_proto.Type):
        return RoleType(type_proto.label, type_proto.scope, type_proto.root)

    def get_scope(self):
        return self._scope

    def get_scoped_label(self):
        return self.get_scope() + ":" + self.get_label()

    def as_remote(self, transaction):
        return RemoteRoleType(transaction, self.get_label(), self.get_scope(), self.is_root())

    def is_role_type(self):
        return True

    def __str__(self):
        return type(self).__name__ + "[label:" + self.get_scoped_label() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_scoped_label() == other.get_scoped_label()


class RemoteRoleType(RemoteType):

    def __init__(self, transaction, label: str, scope: str, is_root: bool):
        super(RemoteRoleType, self).__init__(transaction, label, is_root)
        self._scope = scope

    def get_scope(self):
        return self._scope

    def get_scoped_label(self):
        return self.get_scope() + ":" + self.get_label()

    def as_remote(self, transaction):
        return RemoteRoleType(transaction, self.get_label(), self.get_scope(), self.is_root())

    def get_relation_type(self):
        method = concept_proto.Type.Req()
        method.role_type_get_relation_type_req.CopyFrom(concept_proto.RoleType.GetRelationType.Req())
        return proto_reader.type_(self._execute(method).role_type_get_relation_type_res.relation_type)

    def get_relation_types(self):
        method = concept_proto.Type.Req()
        method.role_type_get_relation_types_req.CopyFrom(concept_proto.RoleType.GetRelationTypes.Req())
        return self._type_stream(method, lambda res: res.role_type_get_relation_types_res.relation_types)

    def get_players(self):
        method = concept_proto.Type.Req()
        method.role_type_get_players_req.CopyFrom(concept_proto.RoleType.GetPlayers.Req())
        return self._type_stream(method, lambda res: res.role_type_get_players_res.thing_types)

    def is_role_type(self):
        return True

    def _type_stream(self, method: concept_proto.Type.Req, type_list_getter: Callable[[concept_proto.Type.Res], List[concept_proto.Type]]):
        method.scope = self.get_scope()
        return super(RemoteRoleType, self)._type_stream(method, type_list_getter)

    def _execute(self, method: concept_proto.Type.Req):
        method.scope = self.get_scope()
        return super(RemoteRoleType, self)._execute(method)

    def __str__(self):
        return type(self).__name__ + "[label:" + self.get_scoped_label() + "]"

    def __eq__(self, other):
        if other is self:
            return True
        if not other or type(self) != type(other):
            return False
        return self.get_scoped_label() == other.get_scoped_label()
