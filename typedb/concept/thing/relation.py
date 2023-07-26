#
# Copyright (C) 2022 Vaticle
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

from __future__ import annotations
from typing import Iterator

import typedb_protocol.common.concept_pb2 as concept_proto

from typedb.api.concept.thing.relation import Relation
from typedb.api.concept.thing.thing import Thing
from typedb.api.concept.type.relation_type import RelationType
from typedb.api.concept.type.role_type import RoleType
from typedb.api.connection.transaction import Transaction
from typedb.common.rpc.request_builder import relation_add_player_req, relation_remove_player_req, \
    relation_get_players_req, relation_get_players_by_role_type_req, relation_get_relating_req
from typedb.concept.proto import concept_proto_builder, concept_proto_reader
from typedb.concept.thing.thing import _Thing
from typedb.concept.type.relation_type import _RelationType
from typedb.concept.type.role_type import _RoleType

from typedb.typedb_client_python import relation_get_type, relation_add_role_player, relation_remove_role_player, \
    relation_get_players_by_role_type, Concept, relation_get_role_players, role_player_get_role_type, \
    role_player_get_player, relation_get_relating


class _Relation(Relation, _Thing):

    # def __init__(self, iid: str, is_inferred: bool, relation_type: RelationType):
    #     super(_Relation, self).__init__(iid, is_inferred)
    #     self._type = relation_type

    # @staticmethod
    # def of(thing_proto: concept_proto.Thing):
    #     return _Relation(concept_proto_reader.iid(thing_proto.iid), thing_proto.inferred, concept_proto_reader.type_(thing_proto.type))

    def get_type(self) -> _RelationType:
        return _RelationType(relation_get_type(self._concept))

    # def as_relation(self) -> Relation:
    #     return self

    def add_player(self, transaction: Transaction, role_type: RoleType, player: Thing) -> None:
        relation_add_role_player(self.native_transaction(transaction), self._concept,
                                 role_type.native_object(), player.native_object())

    def remove_player(self, transaction: Transaction, role_type: RoleType, player: Thing) -> None:
        relation_remove_role_player(self.native_transaction(transaction), self._concept,
                                    role_type.native_object(), player.native_object())

    def get_players(self, transaction: Transaction, *role_types: RoleType) -> Iterator[_Thing]:
        native_role_types = [Concept(rt.native_object()) for rt in role_types]
        return (_Thing(item) for item in relation_get_players_by_role_type(self.native_transaction(transaction),
                                                                           self._concept, native_role_types))

    def get_players_by_role_type(self, transaction: Transaction) -> dict[RoleType, list[Thing]]:
        role_players = {}
        for role_player in relation_get_role_players(self.native_transaction(transaction), self._concept):
            role = _RoleType(role_player_get_role_type(role_player))
            player = _Thing(role_player_get_player(role_player))
            role_players.get(role, []).append(player)
        return role_players

    def get_relating(self, transaction: Transaction) -> Iterator[_RoleType]:
        return (_RoleType(item) for item in relation_get_relating(self.native_transaction(transaction), self._concept))
