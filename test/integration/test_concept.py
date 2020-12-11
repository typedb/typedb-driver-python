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
from grakn.common.exception import GraknClientException
from test.integration.base import test_base, GraknServer


class TestConcept(test_base):
    @classmethod
    def setUpClass(cls):
        super(TestConcept, cls).setUpClass()
        global client
        client = GraknClient()

    @classmethod
    def tearDownClass(cls):
        super(TestConcept, cls).tearDownClass()
        client.close()

    def setUp(self):
        if "grakn" not in client.databases().all():
            client.databases().create("grakn")

    def test_get_supertypes(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                for lion_supertype in lion.as_remote(tx).get_supertypes():
                    print(str(lion_supertype) + " is a supertype of 'lion'")

    def test_streaming_operation_on_closed_tx(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                tx.close()
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

    def test_invalid_streaming_operation(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                lion = tx.concepts().put_entity_type("lion")
                lion._label = "lizard"
                try:
                    for _ in lion.as_remote(tx).get_supertypes():
                        self.fail()
                    self.fail()
                except GraknClientException:
                    pass

    def test_get_many_instances(self):
        with client.session("grakn", SessionType.SCHEMA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                goldfish_type = tx.concepts().put_entity_type("goldfish")
                tx.commit()
        with client.session("grakn", SessionType.DATA) as session:
            with session.transaction(TransactionType.WRITE) as tx:
                for _ in range(100):
                    goldfish_type.as_remote(tx).create()
                goldfish_count = sum(1 for _ in goldfish_type.as_remote(tx).get_instances())
                print("There are " + str(goldfish_count) + " goldfish.")


if __name__ == "__main__":
    with GraknServer():
        unittest.main(verbosity=2)
