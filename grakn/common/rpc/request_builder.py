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
import uuid
from typing import List

import grakn_protocol.cluster.cluster_database_pb2 as cluster_database_proto
import grakn_protocol.cluster.cluster_server_pb2 as cluster_server_proto
import grakn_protocol.common.concept_pb2 as concept_proto
import grakn_protocol.common.logic_pb2 as logic_proto
import grakn_protocol.common.options_pb2 as options_proto
import grakn_protocol.common.query_pb2 as query_proto
import grakn_protocol.common.session_pb2 as session_proto
import grakn_protocol.common.transaction_pb2 as transaction_proto
import grakn_protocol.core.core_database_pb2 as core_database_proto


# CoreDatabaseManager
from grakn.common.label import Label


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
    req = transaction_proto.Transaction.Req()
    req.reqs.extend(reqs)
    return req


def transaction_stream_req(req_id: uuid):
    req = transaction_proto.Transaction.Req()
    req.req_id = str(req_id)
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
    return req


def concept_manager_put_relation_type_req(label: str):
    req = concept_proto.ConceptManager.Req()
    put_relation_type_req = concept_proto.ConceptManager.PutRelationType.Req()
    put_relation_type_req.label = label
    req.put_relation_type_req.CopyFrom(put_relation_type_req)
    return req


def concept_manager_put_attribute_type_req(label: str, value_type: concept_proto.AttributeType.ValueType):
    req = concept_proto.ConceptManager.Req()
    put_attribute_type_req = concept_proto.ConceptManager.PutAttributeType.Req()
    put_attribute_type_req.label = label
    put_attribute_type_req.value_type = value_type
    req.put_attribute_type_req.CopyFrom(put_attribute_type_req)
    return req


def concept_manager_get_thing_type_req(label: str):
    req = concept_proto.ConceptManager.Req()
    get_thing_type_req = concept_proto.ConceptManager.GetThingType.Req()
    get_thing_type_req.label = label
    req.get_thing_type_req.CopyFrom(get_thing_type_req)
    return req


def concept_manager_get_thing_req(iid: str):
    req = concept_proto.ConceptManager.Req()
    get_thing_req = concept_proto.ConceptManager.GetThing.Req()
    get_thing_req.iid = byte_string(iid)
    req.get_thing_req.CopyFrom(get_thing_req)
    return req


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
    return req


def logic_manager_get_rule_req(label: str):
    req = logic_proto.LogicManager.Req()
    get_rule_req = logic_proto.LogicManager.GetRule.Req()
    get_rule_req.label = label
    req.get_rule_req.CopyFrom(get_rule_req)
    return req


def logic_manager_get_rules_req():
    req = logic_proto.LogicManager.Req()
    req.get_rules_req.CopyFrom(logic_proto.LogicManager.GetRules.Req())
    return req


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
    req = concept_proto.RoleType.Req()
    req.role_type_get_relation_types_req.CopyFrom(concept_proto.RoleType.GetRelationTypes.Req())
    return type_req(req, label)


def role_type_get_players_req(label: Label):
    req = concept_proto.RoleType.Req()
    req.role_type_get_players_req.CopyFrom(concept_proto.RoleType.GetPlayers.Req())
    return type_req(req, label)


# ThingType

def proto_thing_type(label: Label, encoding: concept_proto.Type.Encoding):
    proto_type = concept_proto.Type()
    proto_type.label = label.name()
    proto_type.encoding = encoding
    return proto_type


# TODO: ThingType methods, RelationType, AttributeType


# Thing

def byte_string(iid: str):
    return bytes.fromhex(iid.lstrip("0x"))

# TODO: Thing methods, Relation, Attribute
