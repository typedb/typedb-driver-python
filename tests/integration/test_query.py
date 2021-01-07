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
from grakn.client import GraknClient, SessionType, TransactionType
from tests.integration.base import test_base, GraknServer


class TestQuery(test_base):
    @classmethod
    def setUpClass(cls):
        global client
        client = GraknClient()

    @classmethod
    def tearDownClass(cls):
        client.close()

    def setUp(self):
        if "grakn" not in client.databases().all():
            client.databases().create("grakn")

    def test_define_and_undef_relation_type(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                tx.query().define("define lionfight sub relation, relates victor, relates loser;")
                lionfight_type = tx.concepts().get_relation_type("lionfight")
                print("define: " + lionfight_type._label)
                tx.query().undefine("undefine lionfight sub relation;")
                tx.commit()

    def test_insert_some_entities(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                tx.query().define("define lion sub entity;")
                tx.commit()
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                for answer in tx.query().insert("insert $a isa lion; $b isa lion; $c isa lion;"):
                    print("insert: " + str(answer))
                    tx.commit()

    # def test_match_aggregate(self):
    #     with client.session("grakn", SessionType.DATA) as session:
    #         with session.transaction(TransactionType.READ) as tx:
    #             answer = next(tx.query().match_aggregate("match $p isa lion; get $p; count;"))
    #             print("count: " + str(answer.as_int()))

    def test_match_group(self):
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.READ) as tx:
                for answer in tx.query().match_group("match $p isa lion; get $p; group $p;"):
                    for concept_map in answer.concept_maps:
                        print("group aggregate: " + str(answer.owner) + ": " + str(concept_map))

    # def test_match_group_aggregate(self):
    #     with client.session("grakn", SessionType.DATA) as session:
    #         with session.transaction(TransactionType.READ) as tx:
    #             for answer in tx.query().match_group_aggregate("match $p isa lion; get $p; group $p; count;"):
    #                 print("group aggregate: " + str(answer.owner) + ": " + str(answer.numeric.as_int()))


if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
