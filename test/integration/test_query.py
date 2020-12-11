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
from test.integration.base import test_base, GraknServer


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

    def test_define_relation_type(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                tx.query().define("define lionfight sub relation, relates victor, relates loser;")
                lionfight_type = tx.concepts().get_type("lionfight")
                print(lionfight_type._label)

    def test_insert_some_entities(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                tx.query().define("define lion sub entity;")
                tx.commit()
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                for answer in tx.query().insert("insert $a isa lion; $b isa lion; $c isa lion;"):
                    print(answer)


if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
