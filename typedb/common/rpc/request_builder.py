#
# Copyright (C) 2021 Vaticle
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
from datetime import datetime
from typing import List
from uuid import UUID

import typedb_protocol.cluster.cluster_database_pb2 as cluster_database_proto
import typedb_protocol.cluster.cluster_server_pb2 as cluster_server_proto
import typedb_protocol.cluster.cluster_user_pb2 as cluster_user_proto
import typedb_protocol.common.concept_pb2 as concept_proto
import typedb_protocol.common.logic_pb2 as logic_proto
import typedb_protocol.common.options_pb2 as options_proto
import typedb_protocol.common.query_pb2 as query_proto
import typedb_protocol.common.session_pb2 as session_proto
import typedb_protocol.common.transaction_pb2 as transaction_proto
import typedb_protocol.core.core_database_pb2 as core_database_proto

from typedb.common.exception import TypeDBClientException, GET_HAS_WITH_MULTIPLE_FILTERS
from typedb.common.label import Label


# CoreDatabaseManager

def core_database_manager_create_req(name: str):
    req = core_database_proto.CoreDatabaseManager.Create.Req()
    req.name = name
    return req


def core_database_manager_contains_req(name: str):
    req = core_database_proto.CoreDatabaseManager.Contains.Req()
    req.name = name
    return req


def core_database_manager_all_req():
    return core_database_proto.CoreDatabaseManager.All.Req()


# CoreDatabase

def core_database_schema_req(name: str):
    req = core_database_proto.CoreDatabase.Schema.Req()
    req.name = name
    return req


def core_database_delete_req(name: str):
    req = core_database_proto.CoreDatabase.Delete.Req()
    req.name = name
    return req


# ClusterServer

def cluster_server_manager_all_req():
    return cluster_server_proto.ServerManager.All.Req()


# ClusterDatabaseManager

def cluster_database_manager_get_req(name: str):
    req = cluster_database_proto.ClusterDatabaseManager.Get.Req()
    req.name = name
    return req

def cluster_database_manager_all_req():
    return cluster_database_proto.ClusterDatabaseManager.All.Req()


# ClusterUserManager

def cluster_user_manager_all_req():
    return cluster_user_proto.ClusterUserManager.All.Req()

def cluster_user_manager_create_req(name: str, password: str):
    req = cluster_user_proto.ClusterUserManager.Create.Req()
    req.username = name
    req.password = password
    return req

def cluster_user_manager_contains_req(name: str):
    req = cluster_user_proto.ClusterUserManager.Contains.Req()
    req.username = name
    return req


# ClusterUser

def cluster_user_password_req(name: str, password: str):
    req = cluster_user_proto.ClusterUser.Password.Req()
    req.username = name
    req.password = password
    return req

def cluster_user_delete_req(name: str):
    req = cluster_user_proto.ClusterUser.Delete.Req()
    req.username = name
    return req


# Session

def session_open_req(database: str, session_type: session_proto.Session.Type, options: options_proto.Options):
    req = session_proto.Session.Open.Req()
    req.database = database
    req.type = session_type
    req.options.CopyFrom(options)
    return req


def session_pulse_req(session_id: bytes):
    req = session_proto.Session.Pulse.Req()
    req.session_id = session_id
    return req


def session_close_req(session_id: bytes):
    req = session_proto.Session.Close.Req()
    req.session_id = session_id
    return req


# Transaction

def transaction_client_msg(reqs: List[transaction_proto.Transaction.Req]):
    req = transaction_proto.Transaction.Client()
    req.reqs.extend(reqs)
    return req


def transaction_stream_req(req_id: UUID):
    req = transaction_proto.Transaction.Req()
    req.req_id = req_id.bytes
    stream_req = transaction_proto.Transaction.Stream.Req()
    req.stream_req.CopyFrom(stream_req)
    return req


def transaction_open_req(session_id: bytes, transaction_type: transaction_proto.Transaction.Type,
                         options: options_proto.Options, network_latency_millis: int):
    open_req = transaction_proto.Transaction.Open.Req()
    open_req.session_id = session_id
    open_req.type = transaction_type
    open_req.options.CopyFrom(options)
    open_req.network_latency_millis = network_latency_millis
    req = transaction_proto.Transaction.Req()
    req.open_req.CopyFrom(open_req)
    return req


def transaction_commit_req():
    req = transaction_proto.Transaction.Req()
    commit_req = transaction_proto.Transaction.Commit.Req()
    req.commit_req.CopyFrom(commit_req)
    return req


def transaction_rollback_req():
    req = transaction_proto.Transaction.Req()
    rollback_req = transaction_proto.Transaction.Rollback.Req()
    req.rollback_req.CopyFrom(rollback_req)
    return req


# QueryManager

def query_manager_req(query_mgr_req: query_proto.QueryManager.Req, options: options_proto.Options):
    req = transaction_proto.Transaction.Req()
    query_mgr_req.options.CopyFrom(options)
    req.query_manager_req.CopyFrom(query_mgr_req)
    return req


def query_manager_define_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    def_req = query_proto.QueryManager.Define.Req()
    def_req.query = query
    query_mgr_req.define_req.CopyFrom(def_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_undefine_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    undef_req = query_proto.QueryManager.Undefine.Req()
    undef_req.query = query
    query_mgr_req.undefine_req.CopyFrom(undef_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_match_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    mat_req = query_proto.QueryManager.Match.Req()
    mat_req.query = query
    query_mgr_req.match_req.CopyFrom(mat_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_match_aggregate_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    match_agg_req = query_proto.QueryManager.MatchAggregate.Req()
    match_agg_req.query = query
    query_mgr_req.match_aggregate_req.CopyFrom(match_agg_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_match_group_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    match_grp_req = query_proto.QueryManager.MatchGroup.Req()
    match_grp_req.query = query
    query_mgr_req.match_group_req.CopyFrom(match_grp_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_match_group_aggregate_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    match_agg_grp_req = query_proto.QueryManager.MatchGroupAggregate.Req()
    match_agg_grp_req.query = query
    query_mgr_req.match_group_aggregate_req.CopyFrom(match_agg_grp_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_insert_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    ins_req = query_proto.QueryManager.Insert.Req()
    ins_req.query = query
    query_mgr_req.insert_req.CopyFrom(ins_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_delete_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    del_req = query_proto.QueryManager.Delete.Req()
    del_req.query = query
    query_mgr_req.delete_req.CopyFrom(del_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_update_req(query: str, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    up_req = query_proto.QueryManager.Update.Req()
    up_req.query = query
    query_mgr_req.update_req.CopyFrom(up_req)
    return query_manager_req(query_mgr_req, options)


def query_manager_explain_req(explainable_id: int, options: options_proto.Options):
    query_mgr_req = query_proto.QueryManager.Req()
    explain_req = query_proto.QueryManager.Explain.Req()
    explain_req.explainable_id = explainable_id
    query_mgr_req.explain_req.CopyFrom(explain_req)
    return query_manager_req(query_mgr_req, options)


# ConceptManager

def concept_manager_req(concept_mgr_req: concept_proto.ConceptManager.Req):
    req = transaction_proto.Transaction.Req()
    req.concept_manager_req.CopyFrom(concept_mgr_req)
    return req


def concept_manager_put_entity_type_req(label: str):
    req = concept_proto.ConceptManager.Req()
    put_entity_type_req = concept_proto.ConceptManager.PutEntityType.Req()
    put_entity_type_req.label = label
    req.put_entity_type_req.CopyFrom(put_entity_type_req)
    return concept_manager_req(req)


def concept_manager_put_relation_type_req(label: str):
    req = concept_proto.ConceptManager.Req()
    put_relation_type_req = concept_proto.ConceptManager.PutRelationType.Req()
    put_relation_type_req.label = label
    req.put_relation_type_req.CopyFrom(put_relation_type_req)
    return concept_manager_req(req)


def concept_manager_put_attribute_type_req(label: str, value_type: concept_proto.AttributeType.ValueType):
    req = concept_proto.ConceptManager.Req()
    put_attribute_type_req = concept_proto.ConceptManager.PutAttributeType.Req()
    put_attribute_type_req.label = label
    put_attribute_type_req.value_type = value_type
    req.put_attribute_type_req.CopyFrom(put_attribute_type_req)
    return concept_manager_req(req)


def concept_manager_get_thing_type_req(label: str):
    req = concept_proto.ConceptManager.Req()
    get_thing_type_req = concept_proto.ConceptManager.GetThingType.Req()
    get_thing_type_req.label = label
    req.get_thing_type_req.CopyFrom(get_thing_type_req)
    return concept_manager_req(req)


def concept_manager_get_thing_req(iid: str):
    req = concept_proto.ConceptManager.Req()
    get_thing_req = concept_proto.ConceptManager.GetThing.Req()
    get_thing_req.iid = byte_string(iid)
    req.get_thing_req.CopyFrom(get_thing_req)
    return concept_manager_req(req)


# LogicManager

def logic_manager_req(logic_mgr_req: logic_proto.LogicManager.Req):
    req = transaction_proto.Transaction.Req()
    req.logic_manager_req.CopyFrom(logic_mgr_req)
    return req


def logic_manager_put_rule_req(label: str, when: str, then: str):
    req = logic_proto.LogicManager.Req()
    put_rule_req = logic_proto.LogicManager.PutRule.Req()
    put_rule_req.label = label
    put_rule_req.when = when
    put_rule_req.then = then
    req.put_rule_req.CopyFrom(put_rule_req)
    return logic_manager_req(req)


def logic_manager_get_rule_req(label: str):
    req = logic_proto.LogicManager.Req()
    get_rule_req = logic_proto.LogicManager.GetRule.Req()
    get_rule_req.label = label
    req.get_rule_req.CopyFrom(get_rule_req)
    return logic_manager_req(req)


def logic_manager_get_rules_req():
    req = logic_proto.LogicManager.Req()
    req.get_rules_req.CopyFrom(logic_proto.LogicManager.GetRules.Req())
    return logic_manager_req(req)


# Type

def type_req(req: concept_proto.Type.Req, label: Label):
    req.label = label.name()
    if label.scope():
        req.scope = label.scope()
    tx_req = transaction_proto.Transaction.Req()
    tx_req.type_req.CopyFrom(req)
    return tx_req


def type_is_abstract_req(label: Label):
    req = concept_proto.Type.Req()
    req.type_is_abstract_req.CopyFrom(concept_proto.Type.IsAbstract.Req())
    return type_req(req, label)


def type_set_label_req(label: Label, new_label: str):
    req = concept_proto.Type.Req()
    set_label_req = concept_proto.Type.SetLabel.Req()
    set_label_req.label = new_label
    req.type_set_label_req.CopyFrom(set_label_req)
    return type_req(req, label)


def type_get_supertypes_req(label: Label):
    req = concept_proto.Type.Req()
    req.type_get_supertypes_req.CopyFrom(concept_proto.Type.GetSupertypes.Req())
    return type_req(req, label)


def type_get_subtypes_req(label: Label):
    req = concept_proto.Type.Req()
    req.type_get_subtypes_req.CopyFrom(concept_proto.Type.GetSubtypes.Req())
    return type_req(req, label)


def type_get_supertype_req(label: Label):
    req = concept_proto.Type.Req()
    req.type_get_supertype_req.CopyFrom(concept_proto.Type.GetSupertype.Req())
    return type_req(req, label)


def type_delete_req(label: Label):
    req = concept_proto.Type.Req()
    req.type_delete_req.CopyFrom(concept_proto.Type.Delete.Req())
    return type_req(req, label)


# RoleType

def proto_role_type(label: Label, encoding: concept_proto.Type.Encoding):
    proto_type = concept_proto.Type()
    proto_type.scope = label.scope()
    proto_type.label = label.name()
    proto_type.encoding = encoding
    return proto_type


def role_type_get_relation_types_req(label: Label):
    req = concept_proto.Type.Req()
    req.role_type_get_relation_types_req.CopyFrom(concept_proto.RoleType.GetRelationTypes.Req())
    return type_req(req, label)


def role_type_get_players_req(label: Label):
    req = concept_proto.Type.Req()
    req.role_type_get_players_req.CopyFrom(concept_proto.RoleType.GetPlayers.Req())
    return type_req(req, label)


# ThingType

def proto_thing_type(label: Label, encoding: concept_proto.Type.Encoding):
    proto_type = concept_proto.Type()
    proto_type.label = label.name()
    proto_type.encoding = encoding
    return proto_type


def thing_type_set_abstract_req(label: Label):
    req = concept_proto.Type.Req()
    req.thing_type_set_abstract_req.CopyFrom(concept_proto.ThingType.SetAbstract.Req())
    return type_req(req, label)


def thing_type_unset_abstract_req(label: Label):
    req = concept_proto.Type.Req()
    req.thing_type_unset_abstract_req.CopyFrom(concept_proto.ThingType.UnsetAbstract.Req())
    return type_req(req, label)


def thing_type_set_supertype_req(label: Label, supertype: concept_proto.Type):
    req = concept_proto.Type.Req()
    set_supertype_req = concept_proto.Type.SetSupertype.Req()
    set_supertype_req.type.CopyFrom(supertype)
    req.type_set_supertype_req.CopyFrom(set_supertype_req)
    return type_req(req, label)


def thing_type_get_plays_req(label: Label):
    req = concept_proto.Type.Req()
    req.thing_type_get_plays_req.CopyFrom(concept_proto.ThingType.GetPlays.Req())
    return type_req(req, label)


def thing_type_set_plays_req(label: Label, role_type: concept_proto.Type, overridden_role_type: concept_proto.Type = None):
    req = concept_proto.Type.Req()
    set_plays_req = concept_proto.ThingType.SetPlays.Req()
    set_plays_req.role.CopyFrom(role_type)
    if overridden_role_type:
        set_plays_req.overridden_role.CopyFrom(overridden_role_type)
    req.thing_type_set_plays_req.CopyFrom(set_plays_req)
    return type_req(req, label)


def thing_type_unset_plays_req(label: Label, role_type: concept_proto.Type):
    req = concept_proto.Type.Req()
    unset_plays_req = concept_proto.ThingType.UnsetPlays.Req()
    unset_plays_req.role.CopyFrom(role_type)
    req.thing_type_unset_plays_req.CopyFrom(unset_plays_req)
    return type_req(req, label)


def thing_type_get_owns_req(label: Label, value_type: concept_proto.AttributeType.ValueType = None, keys_only: bool = False):
    req = concept_proto.Type.Req()
    get_owns_req = concept_proto.ThingType.GetOwns.Req()
    get_owns_req.keys_only = keys_only
    if value_type:
        get_owns_req.value_type = value_type
    req.thing_type_get_owns_req.CopyFrom(get_owns_req)
    return type_req(req, label)


def thing_type_set_owns_req(label: Label, attribute_type: concept_proto.Type, overridden_type: concept_proto.Type = None, is_key: bool = False):
    req = concept_proto.Type.Req()
    set_owns_req = concept_proto.ThingType.SetOwns.Req()
    set_owns_req.attribute_type.CopyFrom(attribute_type)
    set_owns_req.is_key = is_key
    if overridden_type:
        set_owns_req.overridden_type.CopyFrom(overridden_type)
    req.thing_type_set_owns_req.CopyFrom(set_owns_req)
    return type_req(req, label)


def thing_type_unset_owns_req(label: Label, attribute_type: concept_proto.Type):
    req = concept_proto.Type.Req()
    unset_owns_req = concept_proto.ThingType.UnsetOwns.Req()
    unset_owns_req.attribute_type.CopyFrom(attribute_type)
    req.thing_type_unset_owns_req.CopyFrom(unset_owns_req)
    return type_req(req, label)


def thing_type_get_instances_req(label: Label):
    req = concept_proto.Type.Req()
    req.thing_type_get_instances_req.CopyFrom(concept_proto.ThingType.GetInstances.Req())
    return type_req(req, label)


# EntityType

def entity_type_create_req(label: Label):
    req = concept_proto.Type.Req()
    create_req = concept_proto.EntityType.Create.Req()
    req.entity_type_create_req.CopyFrom(create_req)
    return type_req(req, label)


# RelationType

def relation_type_create_req(label: Label):
    req = concept_proto.Type.Req()
    create_req = concept_proto.RelationType.Create.Req()
    req.relation_type_create_req.CopyFrom(create_req)
    return type_req(req, label)


def relation_type_get_relates_req(label: Label, role_label: str = None):
    req = concept_proto.Type.Req()
    if role_label:
        get_relates_req = concept_proto.RelationType.GetRelatesForRoleLabel.Req()
        get_relates_req.label = role_label
        req.relation_type_get_relates_for_role_label_req.CopyFrom(get_relates_req)
    else:
        req.relation_type_get_relates_req.CopyFrom(concept_proto.RelationType.GetRelates.Req())
    return type_req(req, label)


def relation_type_set_relates_req(label: Label, role_label: str, overridden_label: str = None):
    req = concept_proto.Type.Req()
    set_relates_req = concept_proto.RelationType.SetRelates.Req()
    set_relates_req.label = role_label
    if overridden_label:
        set_relates_req.overridden_label = overridden_label
    req.relation_type_set_relates_req.CopyFrom(set_relates_req)
    return type_req(req, label)


def relation_type_unset_relates_req(label: Label, role_label: str):
    req = concept_proto.Type.Req()
    unset_relates_req = concept_proto.RelationType.UnsetRelates.Req()
    unset_relates_req.label = role_label
    req.relation_type_unset_relates_req.CopyFrom(unset_relates_req)
    return type_req(req, label)


# AttributeType

def attribute_type_get_owners_req(label: Label, only_key: bool = False):
    req = concept_proto.Type.Req()
    get_owners_req = concept_proto.AttributeType.GetOwners.Req()
    get_owners_req.only_key = only_key
    req.attribute_type_get_owners_req.CopyFrom(get_owners_req)
    return type_req(req, label)


def attribute_type_put_req(label: Label, value: concept_proto.Attribute.Value):
    req = concept_proto.Type.Req()
    put_req = concept_proto.AttributeType.Put.Req()
    put_req.value.CopyFrom(value)
    req.attribute_type_put_req.CopyFrom(put_req)
    return type_req(req, label)


def attribute_type_get_req(label: Label, value: concept_proto.Attribute.Value):
    req = concept_proto.Type.Req()
    get_req = concept_proto.AttributeType.Get.Req()
    get_req.value.CopyFrom(value)
    req.attribute_type_get_req.CopyFrom(get_req)
    return type_req(req, label)


def attribute_type_get_regex_req(label: Label):
    req = concept_proto.Type.Req()
    req.attribute_type_get_regex_req.CopyFrom(concept_proto.AttributeType.GetRegex.Req())
    return type_req(req, label)


def attribute_type_set_regex_req(label: Label, regex: str):
    req = concept_proto.Type.Req()
    set_regex_req = concept_proto.AttributeType.SetRegex.Req()
    set_regex_req.regex = regex
    req.attribute_type_set_regex_req.CopyFrom(set_regex_req)
    return type_req(req, label)


# Thing

def byte_string(iid: str):
    return bytes.fromhex(iid.lstrip("0x"))


def proto_thing(iid: str):
    thing = concept_proto.Thing()
    thing.iid = byte_string(iid)
    return thing


def thing_req(req: concept_proto.Thing.Req, iid: str):
    req.iid = byte_string(iid)
    tx_req = transaction_proto.Transaction.Req()
    tx_req.thing_req.CopyFrom(req)
    return tx_req


def thing_get_has_req(iid: str, attribute_types: List[concept_proto.Type] = None, only_key: bool = False):
    if attribute_types and only_key:
        raise TypeDBClientException.of(GET_HAS_WITH_MULTIPLE_FILTERS)
    req = concept_proto.Thing.Req()
    get_has_req = concept_proto.Thing.GetHas.Req()
    if only_key:
        get_has_req.keys_only = only_key
    elif attribute_types:
        get_has_req.attribute_types.extend(attribute_types)
    req.thing_get_has_req.CopyFrom(get_has_req)
    return thing_req(req, iid)


def thing_set_has_req(iid: str, attribute: concept_proto.Thing):
    req = concept_proto.Thing.Req()
    set_has_req = concept_proto.Thing.SetHas.Req()
    set_has_req.attribute.CopyFrom(attribute)
    req.thing_set_has_req.CopyFrom(set_has_req)
    return thing_req(req, iid)


def thing_unset_has_req(iid: str, attribute: concept_proto.Thing):
    req = concept_proto.Thing.Req()
    unset_has_req = concept_proto.Thing.UnsetHas.Req()
    unset_has_req.attribute.CopyFrom(attribute)
    req.thing_unset_has_req.CopyFrom(unset_has_req)
    return thing_req(req, iid)


def thing_get_playing_req(iid: str):
    req = concept_proto.Thing.Req()
    req.thing_get_playing_req.CopyFrom(concept_proto.Thing.GetPlaying.Req())
    return thing_req(req, iid)


def thing_get_relations_req(iid: str, role_types: List[concept_proto.Type] = None):
    if not role_types:
        role_types = []
    req = concept_proto.Thing.Req()
    get_relations_req = concept_proto.Thing.GetRelations.Req()
    get_relations_req.role_types.extend(role_types)
    req.thing_get_relations_req.CopyFrom(get_relations_req)
    return thing_req(req, iid)


def thing_delete_req(iid: str):
    req = concept_proto.Thing.Req()
    req.thing_delete_req.CopyFrom(concept_proto.Thing.Delete.Req())
    return thing_req(req, iid)


# Relation

def relation_add_player_req(iid: str, role_type: concept_proto.Type, player: concept_proto.Thing):
    req = concept_proto.Thing.Req()
    add_player_req = concept_proto.Relation.AddPlayer.Req()
    add_player_req.role_type.CopyFrom(role_type)
    add_player_req.player.CopyFrom(player)
    req.relation_add_player_req.CopyFrom(add_player_req)
    return thing_req(req, iid)


def relation_remove_player_req(iid: str, role_type: concept_proto.Type, player: concept_proto.Thing):
    req = concept_proto.Thing.Req()
    remove_player_req = concept_proto.Relation.RemovePlayer.Req()
    remove_player_req.role_type.CopyFrom(role_type)
    remove_player_req.player.CopyFrom(player)
    req.relation_remove_player_req.CopyFrom(remove_player_req)
    return thing_req(req, iid)


def relation_get_players_req(iid: str, role_types: List[concept_proto.Type] = None):
    if not role_types:
        role_types = []
    req = concept_proto.Thing.Req()
    get_players_req = concept_proto.Relation.GetPlayers.Req()
    get_players_req.role_types.extend(role_types)
    req.relation_get_players_req.CopyFrom(get_players_req)
    return thing_req(req, iid)


def relation_get_players_by_role_type_req(iid: str):
    req = concept_proto.Thing.Req()
    req.relation_get_players_by_role_type_req.CopyFrom(concept_proto.Relation.GetPlayersByRoleType.Req())
    return thing_req(req, iid)


def relation_get_relating_req(iid: str):
    req = concept_proto.Thing.Req()
    req.relation_get_relating_req.CopyFrom(concept_proto.Relation.GetRelating.Req())
    return thing_req(req, iid)


# Attribute

def attribute_get_owners_req(iid: str, owner_type: concept_proto.Type = None):
    req = concept_proto.Thing.Req()
    get_owners_req = concept_proto.Attribute.GetOwners.Req()
    if owner_type:
        get_owners_req.thing_type = owner_type
    req.attribute_get_owners_req.CopyFrom(get_owners_req)
    return thing_req(req, iid)


def proto_boolean_attribute_value(value: bool):
    value_proto = concept_proto.Attribute.Value()
    value_proto.boolean = value
    return value_proto


def proto_long_attribute_value(value: int):
    value_proto = concept_proto.Attribute.Value()
    value_proto.long = value
    return value_proto


def proto_double_attribute_value(value: float):
    value_proto = concept_proto.Attribute.Value()
    value_proto.double = value
    return value_proto


def proto_string_attribute_value(value: str):
    value_proto = concept_proto.Attribute.Value()
    value_proto.string = value
    return value_proto


def proto_datetime_attribute_value(value: datetime):
    value_proto = concept_proto.Attribute.Value()
    value_proto.date_time = int((value - datetime(1970, 1, 1)).total_seconds() * 1000)
    return value_proto


# Rule

def rule_req(label: str, req: logic_proto.Rule.Req):
    req.label = label
    tx_req = transaction_proto.Transaction.Req()
    tx_req.rule_req.CopyFrom(req)
    return tx_req


def rule_set_label_req(label: str, new_label: str):
    req = logic_proto.Rule.Req()
    set_label_req = logic_proto.Rule.SetLabel.Req()
    set_label_req.label = new_label
    req.rule_set_label_req.CopyFrom(set_label_req)
    return rule_req(label, req)


def rule_delete_req(label: str):
    req = logic_proto.Rule.Req()
    req.rule_delete_req.CopyFrom(logic_proto.Rule.Delete.Req())
    return rule_req(label, req)
