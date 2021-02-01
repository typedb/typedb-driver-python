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

import grakn_protocol.protobuf.concept_pb2 as concept_proto
import grakn_protocol.protobuf.transaction_pb2 as transaction_proto

from grakn.concept.proto import concept_proto_builder, concept_proto_reader
from grakn.concept.thing.thing import Thing, RemoteThing


class Relation(Thing):

    @staticmethod
    def _of(thing_proto: concept_proto.Thing):
        return Relation(concept_proto_reader.iid(thing_proto.iid))

    def as_remote(self, transaction):
        return RemoteRelation(transaction, self.get_iid())

    def is_relation(self):
        return True


class RemoteRelation(RemoteThing):

    def as_remote(self, transaction):
        return RemoteRelation(transaction, self.get_iid())

    def get_players_by_role_type(self):
        method = concept_proto.Thing.Req()
        method.relation_get_players_by_role_type_req.CopyFrom(concept_proto.Relation.GetPlayersByRoleType.Req())
        method.iid = concept_proto_builder.iid(self.get_iid())

        request = transaction_proto.Transaction.Req()
        request.thing_req.CopyFrom(method)
        stream = self._transaction._stream(request, lambda res: res.thing_res.relation_get_players_by_role_type_res.role_types_with_players)

        role_player_dict = {}
        for role_player in stream:
            role = concept_proto_reader.type_(role_player.role_type)
            player = concept_proto_reader.thing(role_player.player)
            if role not in role_player_dict:
                role_player_dict[role] = []
            role_player_dict[role].append(player)
        return role_player_dict

    def get_players(self, role_types=None):
        if not role_types:
            role_types = []
        method = concept_proto.Thing.Req()
        get_players_req = concept_proto.Relation.GetPlayers.Req()
        get_players_req.role_types.extend(concept_proto_builder.types(role_types))
        method.relation_get_players_req.CopyFrom(get_players_req)
        return self._thing_stream(method, lambda res: res.relation_get_players_res.things)

    def add_player(self, role_type, player):
        method = concept_proto.Thing.Req()
        add_player_req = concept_proto.Relation.AddPlayer.Req()
        add_player_req.role_type.CopyFrom(concept_proto_builder.type_(role_type))
        add_player_req.player.CopyFrom(concept_proto_builder.thing(player))
        method.relation_add_player_req.CopyFrom(add_player_req)
        self._execute(method)

    def remove_player(self, role_type, player):
        method = concept_proto.Thing.Req()
        remove_player_req = concept_proto.Relation.RemovePlayer.Req()
        remove_player_req.role_type.CopyFrom(concept_proto_builder.type_(role_type))
        remove_player_req.player.CopyFrom(concept_proto_builder.thing(player))
        method.relation_remove_player_req.CopyFrom(remove_player_req)
        self._execute(method)

    def is_relation(self):
        return True
