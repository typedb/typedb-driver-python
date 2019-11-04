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

import unittest
from grakn.client import GraknClient
from grakn.service.Session.util.ResponseReader import Value, ConceptList, ConceptSet, ConceptSetMeasure, AnswerGroup, Void
from tests.integration.base import test_Base

client = None

class test_Answers(test_Base):
    @classmethod
    def setUpClass(cls):
        global client
        client = GraknClient("localhost:48555")

    @classmethod
    def tearDownClass(cls):
        client.close()


#     @staticmethod
#     def _build_parentship(tx):
#         """ Helper to set up some state to test answers in a tx/keyspace """
#         parentship_type = tx.put_relation_type("parentship")
#         parentship = parentship_type.create()
#         parent_role = tx.put_role("parent")
#         child_role = tx.put_role("child")
#         parentship_type.relates(parent_role)
#         parentship_type.relates(child_role)
#         person_type = tx.put_entity_type("person")
#         person_type.plays(parent_role)
#         person_type.plays(child_role)
#         parent = person_type.create()
#         child = person_type.create()
#         parentship.assign(child_role, child)
#         parentship.assign(parent_role, parent)
#         tx.commit() # closes the tx
#         return {'child': child.id, 'parent': parent.id, 'parentship': parentship.id}
#
#
#     def test_shortest_path_answer_ConceptList(self):
#         """ Test shortest path which returns a ConceptList """
#         local_session = client.session("shortestpath")
#         tx = local_session.transaction().write()
#         parentship_map = test_Answers._build_parentship(tx) # this closes the tx
#         tx = local_session.transaction().write()
#         result = tx.query('compute path from {0}, to {1};'.format(parentship_map['parent'], parentship_map['child']))
#         answer = next(result)
#         self.assertIsInstance(answer, ConceptList)
#         self.assertEqual(len(answer.list()), 3)
#         self.assertTrue(parentship_map['parent'] in answer.list())
#         self.assertTrue(parentship_map['child'] in answer.list())
#         self.assertTrue(parentship_map['parentship'] in answer.list())
#
#         tx.close()
#         local_session.close()
#         client.keyspaces().delete("shortestpath")
#
#     def test_cluster_anwer_ConceptSet(self):
#         """ Test clustering with connected components response as ConceptSet """
#         local_session = client.session("clusterkeyspace")
#         tx = local_session.transaction().write()
#         parentship_map = test_Answers._build_parentship(tx) # this closes the tx
#         tx = local_session.transaction().write()
#         result = tx.query("compute cluster in [person, parentship], using connected-component;")
#         concept_set_answer = next(result)
#         self.assertIsInstance(concept_set_answer, ConceptSet)
#         self.assertEqual(len(concept_set_answer.set()), 3)
#         self.assertTrue(parentship_map['parent'] in concept_set_answer.set())
#         self.assertTrue(parentship_map['child'] in concept_set_answer.set())
#         self.assertTrue(parentship_map['parentship'] in concept_set_answer.set())
#         tx.close()
#         local_session.close()
#         client.keyspaces().delete("clusterkeyspace")
#
#
#     def test_compute_centrality_answer_ConceptSetMeasure(self):
#         """ Test compute centrality, response type ConceptSetMeasure """
#         local_session = client.session("centralitykeyspace")
#         tx = local_session.transaction().write()
#         parentship_map = test_Answers._build_parentship(tx) # this closes the tx
#         tx = local_session.transaction().write()
#         result = tx.query("compute centrality in [person, parentship], using degree;")
#         concept_set_measure_answer = next(result)
#         self.assertIsInstance(concept_set_measure_answer, ConceptSetMeasure)
#         self.assertEqual(concept_set_measure_answer.measurement(), 1)
#         self.assertTrue(parentship_map['parent'] in concept_set_measure_answer.set())
#         self.assertTrue(parentship_map['child'] in concept_set_measure_answer.set())
#         tx.close()
#         local_session.close()
#         client.keyspaces().delete("centralitykeyspace")
#
#
#     def test_compute_aggregate_group_answer_AnswerGroup(self):
#         """ Test compute aggreate count, response type AnwerGroup """
#         local_session = client.session("aggregategroup")
#         tx = local_session.transaction().write()
#         parentship_map = test_Answers._build_parentship(tx) # this closes the tx
#         tx = local_session.transaction().write()
#         result = tx.query("match $x isa person; $y isa person; (parent: $x, child: $y) isa parentship; get; group $x;")
#         answer_group = next(result)
#         self.assertIsInstance(answer_group, AnswerGroup)
#         self.assertEqual(answer_group.owner().id, parentship_map['parent'])
#         self.assertEqual(answer_group.answers()[0].get('x').id, parentship_map['parent'])
#         self.assertEqual(answer_group.answers()[0].map()['y'].id, parentship_map['child'])
#         tx.close()
#         local_session.close()
#         client.keyspaces().delete("aggregategroup")
#
#
#     def test_delete_returns_Void(self):
#         """ Test `match...delete`, response type should be Void"""
#         local_session = client.session("matchdelete_void")
#         tx = local_session.transaction().write()
#         tx.query("define person sub entity;")
#         result = list(tx.query("insert $x isa person;"))
#         inserted_person = result[0].get("x")
#         person_id = inserted_person.id
#
#         void_result = list(tx.query("match $x id {0}; delete $x;".format(person_id)))[0]
#         self.assertEqual(type(void_result), Void)
#         self.assertTrue("success" in void_result.message())
#
#         self.assertTrue(inserted_person.is_deleted())
#         tx.close()
#         local_session.close()
#         client.keyspaces().delete("matchdelete_void")
#

    def test_conceptmap_explanation(self):
        """ Test explanations when hitting a transitive rule """
        local_session = client.session("transitivity")
        tx = local_session.transaction().write()
        tx.query("""
            define
                object sub entity, plays owned, plays owner;
                ownership sub relation, relates owned, relates owner;
                transitive-ownership sub rule, when {
                    (owned: $x, owner: $y) isa ownership;
                    (owned: $y, owner: $z) isa ownership;
                }, then {
                    (owned: $x, owner: $z) isa ownership;
                };
            """)
        tx.query("""
            insert
                $a isa object; $b isa object; $c isa object; $d isa object; $e isa object;
                (owned: $a, owner: $b) isa ownership;
                (owned: $b, owner: $c) isa ownership;
                (owned: $c, owner: $d) isa ownership;
                (owned: $d, owner: $e) isa ownership;
        """)
        tx.commit()

        tx = local_session.transaction().write()
        answers = tx.query("match (owner: $x, owned: $y) isa ownership; get;")

        has_explanation = 0
        no_explanation = 0
        for concept_map in answers:
            pattern = concept_map.query_pattern()
            if concept_map.has_explanation():
                explanation = concept_map.explanation()
                self.assertIsNotNone(explanation)
                has_explanation += 1
            else:
                no_explanation += 1

        self.assertEqual(no_explanation, 4)
        self.assertEqual(has_explanation, 6)




if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
